// runall.cjs — strict: only run root-level .cjs files; refuse any file inside ./scripts/ or other subdirs
const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');
require('dotenv').config();

// ----------------- CONFIG (edit list of root scripts to run) -----------------
const scripts = [
  "trakingfile.cjs",
  "A1.cjs",
  "labour.cjs",
  "master.cjs",
  "link.cjs",
  "achiv.cjs",
  "works.cjs"
];
// -----------------------------------------------------------------------------


// small helpers
function nowISO(){ return new Date().toISOString(); }
function trimTo(s, n=2000){ if (!s) return ''; return s.length>n ? s.slice(0,n) : s; }

// repo root & forbidden dir
const repoRoot = path.resolve(__dirname) + path.sep;
const forbiddenDir = path.normalize(path.join(repoRoot, 'scripts')) + path.sep;

// read config/targets.json
const cfgPath = path.join(__dirname, 'config', 'targets.json');
if (!fs.existsSync(cfgPath)) {
  console.error("Missing config/targets.json (expected at ./config/targets.json)");
  process.exit(1);
}
let cfg;
try {
  cfg = JSON.parse(fs.readFileSync(cfgPath,'utf8'));
} catch (e) {
  console.error("Could not parse config/targets.json:", e.message || e);
  process.exit(1);
}
const LOG_ID = cfg?.log?.spreadsheetId;
const LOG_TAB = cfg?.log?.tab || 'Runs';
if (!LOG_ID) {
  console.error("log.spreadsheetId missing in config/targets.json");
  process.exit(1);
}

// ---------------- Google Sheets helper (appendRowsTo) ----------------
// uses GOOGLE_CREDENTIALS_BASE64 env var if present, otherwise ./creds.json
const { google } = require('googleapis');

let creds;
if (process.env.GOOGLE_CREDENTIALS_BASE64) {
  try {
    creds = JSON.parse(Buffer.from(process.env.GOOGLE_CREDENTIALS_BASE64, 'base64').toString('utf8'));
  } catch (e) {
    console.error("Invalid GOOGLE_CREDENTIALS_BASE64:", e.message || e);
    process.exit(1);
  }
} else {
  const localCreds = path.join(__dirname, 'creds.json');
  if (!fs.existsSync(localCreds)) {
    console.error("Service account creds not found. Set GOOGLE_CREDENTIALS_BASE64 or place creds.json in repo root.");
    process.exit(1);
  }
  creds = require(localCreds);
}

const authClient = new google.auth.GoogleAuth({
  credentials: creds,
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});

async function getSheetsClient() {
  const client = await authClient.getClient();
  return google.sheets({ version: 'v4', auth: client });
}

/**
 * Append rows (array of arrays) to given spreadsheetId and sheet/tab name.
 * Best-effort: errors are logged but do not throw (so runall can continue).
 */
async function appendRowsTo(spreadsheetId, tabName, rows) {
  try {
    const sheets = await getSheetsClient();
    const range = `${tabName}!A2`;
    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: rows },
    });
    return true;
  } catch (e) {
    console.warn("appendRowsTo failed:", e && e.message ? e.message : e);
    return false;
  }
}
// ----------------------------------------------------------------------


// Helper: resolve and validate a candidate script name
function resolveAndValidate(scriptName) {
  // No path separators allowed (must be root-level file name only)
  if (scriptName.includes('/') || scriptName.includes('\\')) {
    return { ok: false, reason: 'FORBIDDEN_PATH_SEGMENT', detail: 'script name must not contain path separators' };
  }

  const full = path.normalize(path.join(repoRoot, scriptName));

  // Ensure resolved path is inside repoRoot
  if (!full.startsWith(repoRoot)) {
    return { ok: false, reason: 'OUTSIDE_REPO', detail: full };
  }

  // Forbid anything that lies inside the ./scripts directory (safety)
  if (full.startsWith(forbiddenDir)) {
    return { ok: false, reason: 'FORBIDDEN_LOCATION', detail: forbiddenDir };
  }

  // Must be a file and exist
  if (!fs.existsSync(full) || !fs.statSync(full).isFile()) {
    return { ok: false, reason: 'NOT_FOUND', detail: full };
  }

  // Must end with .cjs
  if (!full.endsWith('.cjs')) {
    return { ok: false, reason: 'NOT_CJS', detail: full };
  }

  return { ok: true, full };
}

// Run a single script synchronously (spawnSync) and capture output/time
function runOne(fullPath) {
  const start = Date.now();
  // Use the same node executable used to run runall
  const res = spawnSync(process.execPath, [fullPath], { encoding: 'utf8', maxBuffer: 1024 * 1024 * 5 });
  const end = Date.now();
  return {
    ok: res.status === 0,
    code: res.status,
    durationMs: end - start,
    stdout: trimTo(res.stdout || ''),
    stderr: trimTo(res.stderr || '')
  };
}

// ----------------- Helpers to write run-note column -----------------
function colLetterFromIndex(n) {
  let s = "";
  while (n > 0) {
    const m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

async function ensureRunSheet(sheetsClient) {
  try {
    const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: LOG_ID });
    const names = (meta.data.sheets||[]).map(s=>s.properties.title);
    if (!names.includes('run')) {
      await sheetsClient.spreadsheets.batchUpdate({
        spreadsheetId: LOG_ID,
        requestBody: { requests:[{ addSheet:{ properties:{ title:'run' } } }] }
      });
    }
  } catch (e) {
    // best-effort
  }
}

// read first header row of run sheet and return column letter for filename (create header cell if missing)
async function getRunColumnLetterImmediate(sheetsClient, fileBasename) {
  try {
    const res = await sheetsClient.spreadsheets.values.get({ spreadsheetId: LOG_ID, range: 'run!1:1' });
    const headers = (res.data.values && res.data.values[0]) ? res.data.values[0] : [];
    for (let i=0;i<headers.length;i++){
      if (String(headers[i]).trim() === fileBasename) return colLetterFromIndex(i+1);
    }
    // find empty slot
    let emptyIndex = headers.findIndex(h => h===undefined || h===null || String(h).trim()==='');
    if (emptyIndex===-1) emptyIndex = headers.length;
    const colIdx = emptyIndex + 1;
    const col = colLetterFromIndex(colIdx);
    // write filename into header
    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: LOG_ID,
      range: `run!${col}1`,
      valueInputOption: 'RAW',
      requestBody: { values: [[fileBasename]] }
    });
    return col;
  } catch (e) {
    return 'A';
  }
}

// dedup runsToday using SCRAPE_STATUS run_note column
async function countRunsTodayImmediate(sheetsClient) {
  try {
    const res = await sheetsClient.spreadsheets.values.get({ spreadsheetId: LOG_ID, range: 'SCRAPE_STATUS!A2:E' });
    const rows = res.data.values || [];
    const today = new Date().toLocaleDateString('en-IN');
    const s = new Set();
    for (const r of rows) {
      const rn = (r[4]||'').toString();
      if (rn.includes(today)) s.add(rn);
    }
    return s.size;
  } catch (e) {
    return 0;
  }
}

async function writeRunNoteImmediate(sheetsClient, colLetter, filePath, nowFull, runner, failures, runsToday) {
  const lines = [];
  lines.push(`1) File: ${filePath}`);
  lines.push(`2) Time: ${nowFull}`);
  lines.push(`3) By: ${runner}`);
  lines.push(`4) Failures:`);
  if (!failures || failures.length === 0) {
    lines.push('NONE'); lines.push(''); lines.push(''); lines.push('');
  } else {
    for (let i=0;i<4;i++){
      if (i<failures.length) {
        const f=failures[i];
        lines.push(`${f.panchayat||'UNKNOWN'}\t${f.finYear||'UNKNOWN'}`);
      } else lines.push('');
    }
  }
  lines.push(`Runs today: ${runsToday}`);
  lines.push('5) Run complete');
  while(lines.length<10) lines.push('');
  const values = lines.map(l=>[l]);
  const range = `run!${colLetter}3:${colLetter}12`;
  try {
    await sheetsClient.spreadsheets.values.update({ spreadsheetId: LOG_ID, range, valueInputOption:'RAW', requestBody:{ values } });
  } catch (e) {
    // best-effort
  }
}
// ----------------------------------------------------------------------


// Main
(async () => {
  console.log("🚀 Run start:", nowISO());
  console.log("Repo root:", repoRoot);
  console.log("Forbidden scripts dir:", forbiddenDir);
  console.log("Will attempt to run (order):", scripts.join(', '));
  console.log('---');

  // Dry-check & show resolved paths
  const plan = scripts.map(s => {
    const r = resolveAndValidate(s);
    return { name: s, ...r };
  });

  console.log("Plan:");
  plan.forEach(p => {
    if (p.ok) {
      console.log(`  [OK]   ${p.name} -> ${p.full}`);
    } else {
      console.log(`  [SKIP] ${p.name} -> ${p.reason} ${p.detail ? '- ' + p.detail : ''}`);
    }
  });

  // Append header to central log (best-effort) — write header row if sheet is empty
  const header = [["Timestamp","Script","Action","Status","Duration(ms)","Note"]];
  try { await appendRowsTo(LOG_ID, LOG_TAB, header); } catch (e) {}

  const results = [];

  for (const p of plan) {
    if (!p.ok) {
      const note = `${p.reason}${p.detail ? ': ' + p.detail : ''}`;
      console.log(`❌ Skipping ${p.name}: ${note}`);
      const row = [[ nowISO(), p.name, 'SKIP', 'FAIL', 0, note ]];
      try { await appendRowsTo(LOG_ID, LOG_TAB, row); } catch (e) {}
      results.push({ script: p.name, ok: false, skipped: true, reason: note });
      continue;
    }

    console.log(`▶ Executing ${p.name}  (${p.full})`);
    const r = runOne(p.full);
    const note = r.ok ? (r.stdout || `exit:0`) : (r.stderr || `exit:${r.code}`);
    const row = [[ nowISO(), p.name, 'EXEC', r.ok ? 'OK' : 'FAIL', r.durationMs, note ]];
    try {
      await appendRowsTo(LOG_ID, LOG_TAB, row);
      console.log("  ↳ Logged to Runs");
    } catch (e) {
      console.error("  ↳ Log failed:", e && e.message ? e.message : e);
    }

    // --- NEW: immediate run-note update in central 'run' sheet for this script ---
    try {
      const sheetsClient = await getSheetsClient();
      await ensureRunSheet(sheetsClient);
      const basename = p.name; // use filename as header
      const col = await getRunColumnLetterImmediate(sheetsClient, basename);
      const runsToday = await countRunsTodayImmediate(sheetsClient);
      const nowFull = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
      const runner = process.env.RUNNER_TYPE || (process.env.TERMUX_VERSION ? 'mobile' : (process.platform||'').toLowerCase().includes('android') ? 'mobile' : 'system');
      const failures = r.ok ? [] : [{ row:'', panchayat:'UNKNOWN', finYear:'UNKNOWN', status: `FAIL:${r.code}` }];
      await writeRunNoteImmediate(sheetsClient, col, path.resolve(p.full), nowFull, runner, failures, runsToday);
      console.log('  ↳ Run-note updated in run sheet');
    } catch (e) {
      console.warn('  ↳ Could not write run-note:', e && e.message ? e.message : e);
    }
    // --- end run-note update ---

    results.push({ script: p.name, ok: r.ok, durationMs: r.durationMs, stdout: r.stdout, stderr: r.stderr });
    console.log(r.ok ? `✅ ${p.name} OK (${r.durationMs}ms)` : `❌ ${p.name} FAIL code=${r.code} (${r.durationMs}ms)`);
  }

  console.log('---');
  console.log("Summary:");
  results.forEach(r => {
    if (r.skipped) console.log(`  SKIPPED: ${r.script}  (${r.reason})`);
    else console.log(`  ${r.ok ? 'RUN' : 'FAILED'}: ${r.script}  (${r.durationMs ?? 0} ms)`);
  });

  const allOk = results.every(r => r.ok === true || r.skipped);
  console.log("🏁 Done:", nowISO(), "Status:", allOk ? "ALL_OK_OR_SKIPPED":"HAS_FAILS");
  process.exit(allOk ? 0 : 1);
})().catch(err => {
  console.error("Fatal:", err && err.stack ? err.stack : err);
  process.exit(1);
});
