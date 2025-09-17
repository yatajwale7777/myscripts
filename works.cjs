// works.cjs (auto-select column in 'run' sheet by filename)
// - Reads URLs from Sheet3!B3:B (no writes to Sheet3).
// - Writes scraped rows to Sheet5 (same as before).
// - Appends failures to SCRAPE_STATUS.
// - Writes run-note to run!{col}3:{col}12 where col is chosen by matching filename in run!1:1,
//   or by writing filename into first empty header cell.

const { google } = require("googleapis");
const axios = require("axios");
const cheerio = require("cheerio");
const http = require("http");
const https = require("https");
const path = require("path");
require("dotenv").config();

// CONFIG
const SPREADSHEET_ID = "1bsS9b0FDjzPghhAfMW0YRsTdNnKdN6QMC6TS8vxlsJg";
const SHEET3_RANGE = "Sheet3!B3:B";      // only READ URLs from here
const SHEET3_STATUS_START = 3;
const SHEET5_START_ROW = 3;
const CONCURRENCY = parseInt(process.env.CONCURRENCY || "10", 10);
const RETRIES = 3;
const AXIOS_TIMEOUT = 20000;

// Auth
let creds;
if (process.env.GOOGLE_CREDENTIALS_BASE64) {
  creds = JSON.parse(Buffer.from(process.env.GOOGLE_CREDENTIALS_BASE64, "base64").toString("utf8"));
} else {
  creds = require("./creds.json");
}
const auth = new google.auth.GoogleAuth({ credentials: creds, scopes: ["https://www.googleapis.com/auth/spreadsheets"] });

async function getSheetsClient() {
  const client = await auth.getClient();
  return google.sheets({ version: "v4", auth: client });
}

// Utilities
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function colLetterFromIndex(n) {
  // 1 -> A, 2 -> B, ...
  let s = "";
  while (n > 0) {
    const m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

// Read URLs
async function getUrlsWithRow() {
  const sheets = await getSheetsClient();
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: SHEET3_RANGE });
  const vals = res.data.values || [];
  const urls = [];
  for (let i = 0; i < vals.length; i++) {
    const v = (vals[i] && vals[i][0]) ? vals[i][0].trim() : "";
    if (!v) continue;
    const row = SHEET3_STATUS_START + i;
    urls.push({ url: v, row });
  }
  return urls;
}

// HTTP fetch
const axiosHttp = axios.create({
  timeout: AXIOS_TIMEOUT,
  httpAgent: new http.Agent({ keepAlive: true }),
  httpsAgent: new https.Agent({ keepAlive: true }),
  headers: { "User-Agent": "Mozilla/5.0 (compatible; nrega-scraper/1.0)" }
});

async function safeFetch(url) {
  let attempt = 0, lastErr = null;
  while (attempt < RETRIES) {
    try {
      const r = await axiosHttp.get(url);
      return r.data;
    } catch (err) {
      lastErr = err;
      await sleep(500 + Math.pow(2, attempt) * 500);
      attempt++;
    }
  }
  throw lastErr;
}

// Extract meta from URL
function extractFromUrl(url) {
  try {
    const u = new URL(url);
    const params = u.searchParams;
    const rawP = params.get('panchayat_name') || params.get('PANCHAYAT_NAME') || '';
    const rawF = params.get('fin_year') || params.get('FIN_YEAR') || '';
    const panchayat = decodeURIComponent(rawP.replace(/\+/g, ' ')).toUpperCase().trim();
    const finYear = rawF.trim();
    return { panchayat, finYear };
  } catch (e) {
    return { panchayat: '', finYear: '' };
  }
}

// Parse tables (prefer URL meta)
function parseTablesFromHtml(html, url) {
  const $ = cheerio.load(html);
  const tables = $("table");
  if (tables.length < 4) return { data: [], reason: "LESS_THAN_4_TABLES", ...extractFromUrl(url) };

  const metaTableText = tables.eq(2).text().toUpperCase().replace(/\s+/g, " ");
  const extract = (label, nextLabel) => {
    const rx = new RegExp(`${label}\\s*:?[\\s]+([A-Z0-9\\-\\/\\(\\)\\s]+?)\\s+${nextLabel}`, "i");
    const m = metaTableText.match(rx);
    return m ? m[1].trim() : null;
  };
  const district = extract("DISTRICT", "BLOCK") || extract("DISTRICT", "GRAM") || extract("DISTRICT", "PANCHAYAT") || "";
  const block = extract("BLOCK", "PANCHAYAT") || extract("BLOCK", "GRAM") || "";
  const panchayatMatch = metaTableText.match(/PANCHAYAT\s*:?\s*([A-Z0-9\-\(\)\/\s]+)/i);
  const pagePanchayat = panchayatMatch ? panchayatMatch[1].trim() : "";

  const urlMeta = extractFromUrl(url);
  const panchayat = urlMeta.panchayat || (pagePanchayat ? pagePanchayat.toUpperCase().trim() : '');
  const finYear = urlMeta.finYear || (url.match(/fin_year=([\d\-]+)/i) || [null, "UNKNOWN"])[1];
  const state = "MADHYA PRADESH";

  const dataTable = tables.eq(3);
  const rows = dataTable.find("tr").slice(3, -1);
  const data = [];
  rows.each((_, r) => {
    const rowData = [];
    $(r).find("td, th").each((__, c) => rowData.push($(c).text().trim()));
    if (rowData.length) data.push([state, district, block, panchayat, finYear, ...rowData]);
  });

  if (!data.length) return { data: [], reason: "NO_ROWS_IN_TABLE", panchayat, finYear };
  return { data, panchayat, finYear };
}

// write flattened data to Sheet5 (same as before)
async function writeFlattenedToSheet(allData) {
  if (!allData.length) return 0;
  const sheets = await getSheetsClient();
  const maxCols = Math.max(...allData.map(r => r.length));
  const values = allData.map(row => {
    const copy = row.slice(); while (copy.length < maxCols) copy.push(""); return copy;
  });
  const startColIndex = 3; // C
  const endColIndex = startColIndex + maxCols - 1;
  const endColLetter = colLetterFromIndex(endColIndex);
  const endRow = SHEET5_START_ROW + values.length - 1;
  const range = `Sheet5!C${SHEET5_START_ROW}:${endColLetter}${endRow}`;
  await sheets.spreadsheets.values.update({ spreadsheetId: SPREADSHEET_ID, range, valueInputOption: "RAW", requestBody: { values } });
  return values.length;
}

// ensure SCRAPE_STATUS sheet exists (for failure records)
async function ensureScrapeStatusSheet(sheetsClient) {
  const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  const existing = (meta.data.sheets || []).map(s => s.properties.title);
  const target = 'SCRAPE_STATUS';
  if (!existing.includes(target)) {
    await sheetsClient.spreadsheets.batchUpdate({ spreadsheetId: SPREADSHEET_ID, requestBody: { requests: [{ addSheet: { properties: { title: target } } }] } });
    await sheetsClient.spreadsheets.values.update({ spreadsheetId: SPREADSHEET_ID, range: `${target}!A1:E1`, valueInputOption: 'RAW', requestBody: { values: [["row","panchayat","finYear","status","run_note"]] } });
  }
}

async function appendFailuresToScrapeStatus(sheetsClient, failures, runNote) {
  if (!failures.length) return;
  await ensureScrapeStatusSheet(sheetsClient);
  const rows = failures.map(f => [f.row, f.panchayat, f.finYear, f.status, runNote]);
  await sheetsClient.spreadsheets.values.append({ spreadsheetId: SPREADSHEET_ID, range: 'SCRAPE_STATUS!A2', valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', requestBody: { values: rows } });
  console.log(`📝 Appended ${rows.length} failure rows to SCRAPE_STATUS.`);
}

// ensure 'run' sheet exists
async function ensureRunSheet(sheetsClient) {
  const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  const existing = (meta.data.sheets || []).map(s => s.properties.title);
  if (!existing.includes('run')) {
    await sheetsClient.spreadsheets.batchUpdate({ spreadsheetId: SPREADSHEET_ID, requestBody: { requests: [{ addSheet: { properties: { title: 'run' } } }] } });
  }
}

// find column for this file in run!1:1, or create a header cell for it
async function getRunColumnLetter(sheetsClient, fileBasename) {
  // read first row (A1:1)
  try {
    const res = await sheetsClient.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'run!1:1' });
    const headers = (res.data.values && res.data.values[0]) ? res.data.values[0] : [];
    // find existing
    for (let i = 0; i < headers.length; i++) {
      if (String(headers[i]).trim() === fileBasename) {
        return colLetterFromIndex(i + 1); // 1-based
      }
    }
    // not found: find first empty index
    let emptyIndex = headers.findIndex(h => h === undefined || h === null || String(h).trim() === '');
    if (emptyIndex === -1) emptyIndex = headers.length; // append at end
    const colIdx = emptyIndex + 1;
    const colLetter = colLetterFromIndex(colIdx);
    // write filename to that header cell
    const range = `run!${colLetter}1`;
    await sheetsClient.spreadsheets.values.update({ spreadsheetId: SPREADSHEET_ID, range, valueInputOption: 'RAW', requestBody: { values: [[fileBasename]] } });
    return colLetter;
  } catch (e) {
    // If anything fails, default to column A
    console.warn("Could not read/write run header row, defaulting to column A:", e && e.message ? e.message : e);
    return 'A';
  }
}

// write run-note into selected column rows 3..12
async function writeRunNoteToColumn(sheetsClient, colLetter, filePath, nowFull, runner, failures, runsToday) {
  const lines = [];
  lines.push(`1) File: ${filePath}`);
  lines.push(`2) Time: ${nowFull}`);
  lines.push(`3) By: ${runner}`);
  lines.push(`4) Failures:`);
  if (failures.length === 0) {
    lines.push(`NONE`);
    lines.push("");
    lines.push("");
    lines.push("");
  } else {
    for (let i = 0; i < 4; i++) {
      if (i < failures.length) {
        const f = failures[i];
        lines.push(`${f.panchayat || 'UNKNOWN'}\t${f.finYear || 'UNKNOWN'}`);
      } else {
        lines.push("");
      }
    }
  }
  lines.push(`Runs today: ${runsToday}`);
  lines.push(`5) Run complete`);

  // ensure 10 lines (A3..A12)
  while (lines.length < 10) lines.push("");

  const values = lines.map(l => [l]);
  const range = `run!${colLetter}3:${colLetter}12`;
  await sheetsClient.spreadsheets.values.update({ spreadsheetId: SPREADSHEET_ID, range, valueInputOption: 'RAW', requestBody: { values } });
  console.log(`Run note written to run!${colLetter}3:${colLetter}12`);
}

// count runs today by scanning SCRAPE_STATUS column E (run_note) for today's date substring
async function countRunsToday(sheetsClient) {
  try {
    const res = await sheetsClient.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'SCRAPE_STATUS!A2:E' });
    const rows = res.data.values || [];
    const todayPart = new Date().toLocaleDateString('en-IN'); // e.g. '17/9/2025'
    let count = 0;
    for (const r of rows) {
      const runNote = r[4] || '';
      if (runNote.includes(todayPart)) count++;
    }
    return count;
  } catch (e) {
    return 0;
  }
}

// MAIN
async function run() {
  console.log("Starting import...");
  const sheetsClient = await getSheetsClient();

  // best-effort clear Sheet5 area
  try {
    await sheetsClient.spreadsheets.values.clear({ spreadsheetId: SPREADSHEET_ID, range: "Sheet5!C3:X" });
    console.log("Cleared Sheet5!C3:X");
  } catch (e) {
    console.warn("Could not clear Sheet5!C3:X:", e && e.message ? e.message : e);
  }

  // write header to Sheet5!C2 (best-effort)
  try {
    const headers = ["STATE", "DISTRICT", "BLOCK", "PANCHAYAT", "FIN YEAR"];
    await sheetsClient.spreadsheets.values.update({ spreadsheetId: SPREADSHEET_ID, range: "Sheet5!C2", valueInputOption: "RAW", requestBody: { values: [headers] } });
  } catch (e) {
    console.warn("Could not write headers to Sheet5!C2:", e && e.message ? e.message : e);
  }

  // Read URLs
  const urls = await getUrlsWithRow();
  console.log(`🌐 Found ${urls.length} URLs.`);

  const allData = [];
  const failures = [];

  // process URLs in batches
  for (let i = 0; i < urls.length; i += CONCURRENCY) {
    const batch = urls.slice(i, i + CONCURRENCY);
    await Promise.all(batch.map(async ({ url, row }) => {
      try {
        if (!/^https?:\/\//i.test(url)) {
          const meta = extractFromUrl(url);
          failures.push({ row, panchayat: meta.panchayat || 'UNKNOWN', finYear: meta.finYear || 'UNKNOWN', status: 'INVALID_URL' });
          return;
        }
        const html = await safeFetch(url);
        const parsed = parseTablesFromHtml(html, url);
        const urlMeta = extractFromUrl(url);
        const pName = parsed.panchayat || urlMeta.panchayat || 'UNKNOWN';
        const fYear = parsed.finYear || urlMeta.finYear || 'UNKNOWN';
        if (parsed.data && parsed.data.length) {
          allData.push(...parsed.data);
        } else {
          const reason = parsed.reason || 'NO_DATA';
          failures.push({ row, panchayat: pName, finYear: fYear, status: reason });
          console.log(`⚠️ ${pName} ${fYear} => ${reason}`);
        }
      } catch (err) {
        const meta = extractFromUrl(url);
        const msg = (err && err.code) ? `${err.code}` : (err && err.message) ? err.message : 'ERROR';
        failures.push({ row, panchayat: meta.panchayat || 'UNKNOWN', finYear: meta.finYear || 'UNKNOWN', status: `ERROR:${msg}` });
        console.error(`❌ Error for ${meta.panchayat || url} : ${msg}`);
      }
    }));
    await sleep(300);
  }

  // write flattened data to Sheet5
  if (allData.length > 0) {
    try {
      const rowsWritten = await writeFlattenedToSheet(allData);
      console.log(`✅ Wrote ${rowsWritten} rows to Sheet5.`);
    } catch (e) {
      console.warn("Could not write flattened data to Sheet5:", e && e.message ? e.message : e);
    }
  } else {
    console.log("⚠️ No data found to write to Sheet5.");
  }

  // Append failures to SCRAPE_STATUS
  const runner = process.env.RUNNER_TYPE || (process.env.TERMUX_VERSION ? 'mobile' : (process.platform || '').toLowerCase().includes('android') ? 'mobile' : 'system');
  const filePath = path.resolve(__filename || 'works.cjs');
  const nowFull = new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" });
  const runNote = `${filePath} | ${nowFull} | ${runner}`;
  try {
    await appendFailuresToScrapeStatus(sheetsClient, failures, runNote);
  } catch (e) {
    console.warn("Could not append failures to SCRAPE_STATUS:", e && e.message ? e.message : e);
  }

  // Write run-note to chosen column (auto-select by filename in run!1:1)
  try {
    await ensureRunSheet(sheetsClient);
    const fileBasename = path.basename(__filename || 'works.cjs');
    const colLetter = await getRunColumnLetter(sheetsClient, fileBasename); // e.g. 'A' or 'B'
    const runsToday = await countRunsToday(sheetsClient);
    await writeRunNoteToColumn(sheetsClient, colLetter, filePath, nowFull, runner, failures, runsToday);
  } catch (e) {
    console.warn("Could not write run-note to 'run' sheet:", e && e.message ? e.message : e);
  }

  console.log("Import complete.");
}

run().catch(err => {
  console.error("Fatal error:", err && err.message ? err.message : err);
  process.exit(1);
});
