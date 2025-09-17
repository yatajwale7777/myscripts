// hyperlinks.cjs (final) - reads URL from Sheet2!B2, writes links to Sheet2!B6
// also appends failures to central SCRAPE_STATUS and writes run-note to central run sheet

const { google } = require("googleapis");
const axios = require("axios");
const cheerio = require("cheerio");
const path = require("path");
require("dotenv").config();

// ---------------- CONFIG ----------------
const SPREADSHEET_ID = process.env.SPREADSHEET_ID || "1bsS9b0FDjzPghhAfMW0YRsTdNnKdN6QMC6TS8vxlsJg";
const SHEET_NAME = process.env.SHEET_NAME || "Sheet2";
const READ_RANGE = `${SHEET_NAME}!B2`;
const WRITE_RANGE = `${SHEET_NAME}!B6`;

// central run sheet (shared by all scripts). By default same as SPREADSHEET_ID, override with env.
const CENTRAL_RUN_SPREADSHEET_ID = process.env.RUN_SPREADSHEET_ID || SPREADSHEET_ID;

const AXIOS_TIMEOUT = 20000;

// --------------- AUTH -------------------
let creds;
if (process.env.GOOGLE_CREDENTIALS_BASE64) {
  creds = JSON.parse(Buffer.from(process.env.GOOGLE_CREDENTIALS_BASE64, "base64").toString("utf8"));
} else {
  creds = require("./creds.json");
}
const auth = new google.auth.GoogleAuth({
  credentials: creds,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

async function getSheetsClient() {
  const client = await auth.getClient();
  return google.sheets({ version: "v4", auth: client });
}

// --------------- HELPERS ----------------
function colLetterFromIndex(n) {
  let s = "";
  while (n > 0) {
    const m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

function extractFromUrl(url) {
  try {
    const u = new URL(url);
    const p = (u.searchParams.get("panchayat_name") || u.searchParams.get("PANCHAYAT_NAME") || "").replace(/\+/g, " ");
    const fy = u.searchParams.get("fin_year") || u.searchParams.get("FIN_YEAR") || "";
    return { panchayat: decodeURIComponent(p).toUpperCase().trim() || "UNKNOWN", finYear: fy || "UNKNOWN" };
  } catch (e) {
    return { panchayat: "UNKNOWN", finYear: "UNKNOWN" };
  }
}

// --------------- CORE: read URL, fetch links, write links ----------------
async function getUrlFromSheet(sheets) {
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: READ_RANGE });
  return res.data.values?.[0]?.[0] || "";
}

async function fetchHyperlinks(url) {
  const axiosHttp = axios.create({ timeout: AXIOS_TIMEOUT, headers: { "User-Agent": "Mozilla/5.0" } });
  const { data } = await axiosHttp.get(url);
  const $ = cheerio.load(data);
  const links = [];
  $('a').each((_, el) => {
    const href = $(el).attr('href');
    if (href) links.push([href]);
  });
  return links;
}

async function writeLinksToSheet(sheets, links) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: WRITE_RANGE,
    valueInputOption: "RAW",
    requestBody: { values: links },
  });
}

// --------------- CENTRAL SCRAPE_STATUS & run sheet helpers ----------------
async function ensureCentralScrapeStatus(sheetsClient) {
  const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID });
  const existing = (meta.data.sheets || []).map(s => s.properties.title);
  const target = 'SCRAPE_STATUS';
  if (!existing.includes(target)) {
    await sheetsClient.spreadsheets.batchUpdate({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: target } } }] }
    });
    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range: `${target}!A1:E1`,
      valueInputOption: "RAW",
      requestBody: { values: [['row','panchayat','finYear','status','run_note']] }
    });
  }
}

async function appendFailuresToCentralScrapeStatus(sheetsClient, failures, runNote) {
  if (!failures || failures.length === 0) return;
  await ensureCentralScrapeStatus(sheetsClient);
  const rows = failures.map(f => [f.row || "", f.panchayat || "UNKNOWN", f.finYear || "UNKNOWN", f.status || "ERROR", runNote]);
  await sheetsClient.spreadsheets.values.append({
    spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
    range: 'SCRAPE_STATUS!A2',
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: rows }
  });
  console.log(`📝 Appended ${rows.length} failure rows to central SCRAPE_STATUS.`);
}

async function ensureCentralRunSheet(sheetsClient) {
  const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID });
  const existing = (meta.data.sheets || []).map(s => s.properties.title);
  if (!existing.includes('run')) {
    await sheetsClient.spreadsheets.batchUpdate({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: 'run' } } }] }
    });
  }
}

async function getRunColumnLetter(sheetsClient, fileBasename) {
  try {
    const res = await sheetsClient.spreadsheets.values.get({ spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID, range: 'run!1:1' });
    const headers = (res.data.values && res.data.values[0]) ? res.data.values[0] : [];
    for (let i = 0; i < headers.length; i++) {
      if (String(headers[i]).trim() === fileBasename) return colLetterFromIndex(i + 1);
    }
    let emptyIndex = headers.findIndex(h => h === undefined || h === null || String(h).trim() === '');
    if (emptyIndex === -1) emptyIndex = headers.length;
    const colIdx = emptyIndex + 1;
    const colLetter = colLetterFromIndex(colIdx);
    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range: `run!${colLetter}1`,
      valueInputOption: "RAW",
      requestBody: { values: [[fileBasename]] }
    });
    return colLetter;
  } catch (e) {
    console.warn("Could not read/write run header, defaulting to column A:", e && e.message ? e.message : e);
    return 'A';
  }
}

async function countRunsToday(sheetsClient) {
  try {
    const res = await sheetsClient.spreadsheets.values.get({ spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID, range: 'SCRAPE_STATUS!A2:E' });
    const rows = res.data.values || [];
    const todayPart = new Date().toLocaleDateString('en-IN');
    const seen = new Set();
    for (const r of rows) {
      const rn = (r[4] || '').toString();
      if (rn.includes(todayPart)) seen.add(rn);
    }
    return seen.size;
  } catch (e) {
    return 0;
  }
}

async function writeRunNoteToColumn(sheetsClient, colLetter, filePath, nowFull, runner, failures, runsToday) {
  const lines = [];
  lines.push(`1) File: ${filePath}`);
  lines.push(`2) Time: ${nowFull}`);
  lines.push(`3) By: ${runner}`);
  lines.push(`4) Failures:`);
  if (!failures || failures.length === 0) {
    lines.push('NONE');
    lines.push(''); lines.push(''); lines.push('');
  } else {
    for (let i = 0; i < 4; i++) {
      if (i < failures.length) {
        const f = failures[i];
        lines.push(`${f.panchayat || 'UNKNOWN'}\t${f.finYear || 'UNKNOWN'}`);
      } else lines.push('');
    }
  }
  lines.push(`Runs today: ${runsToday}`);
  lines.push('5) Run complete');
  while (lines.length < 10) lines.push('');
  const values = lines.map(l => [l]);
  const range = `run!${colLetter}3:${colLetter}12`;
  await sheetsClient.spreadsheets.values.update({
    spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
    range,
    valueInputOption: "RAW",
    requestBody: { values }
  });
  console.log(`Run note written to central run!${colLetter}3:${colLetter}12`);
}

// --------------- MAIN ----------------
(async () => {
  console.log('🔧 hyperlinks.cjs starting...');
  const sheetsClient = await getSheetsClient();
  const failures = [];

  try {
    // Read URL from sheet
    const url = await getUrlFromSheet(sheetsClient);
    if (!url) throw new Error('URL not found in ' + READ_RANGE);

    // Fetch links
    const links = await fetchHyperlinks(url);
    if (!links || links.length === 0) {
      const meta = extractFromUrl(url);
      failures.push({ row: '', panchayat: meta.panchayat || 'UNKNOWN', finYear: meta.finYear || 'UNKNOWN', status: 'NO_LINKS' });
      console.warn('⚠️ No hyperlinks found.');
    } else {
      // Write links back to sheet
      await writeLinksToSheet(sheetsClient, links);
      console.log(`✅ Imported ${links.length} hyperlinks to ${WRITE_RANGE}`);
    }
  } catch (err) {
    const msg = err && err.message ? err.message : String(err);
    console.error('❌ Error:', msg);
    // push generic failure if URL missing or fetch failed
    const meta = extractFromUrl(err && err.url ? err.url : '');
    failures.push({ row: '', panchayat: meta.panchayat || 'UNKNOWN', finYear: meta.finYear || 'UNKNOWN', status: `ERROR:${msg}` });
  }

  // Prepare run-note and append failures to central SCRAPE_STATUS + write run note
  try {
    const runner = process.env.RUNNER_TYPE || (process.env.TERMUX_VERSION ? 'mobile' : (process.platform || '').toLowerCase().includes('android') ? 'mobile' : 'system');
    const filePath = path.resolve(__filename || 'hyperlinks.cjs');
    const nowFull = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    const runNote = `${filePath} | ${nowFull} | ${runner}`;

    await appendFailuresToCentralScrapeStatus(sheetsClient, failures, runNote);
    await ensureCentralRunSheet(sheetsClient);
    const basename = path.basename(__filename || 'hyperlinks.cjs');
    const col = await getRunColumnLetter(sheetsClient, basename);
    const runsToday = await countRunsToday(sheetsClient);
    await writeRunNoteToColumn(sheetsClient, col, filePath, nowFull, runner, failures, runsToday);
  } catch (e) {
    console.warn('Could not update central run/SCRAPE_STATUS:', e && e.message ? e.message : e);
  }

  console.log('🔚 hyperlinks.cjs finished.');
})();
