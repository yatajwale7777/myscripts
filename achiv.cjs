// achiv.cjs
// ----------------- CHANGE ONLY THIS TOP BLOCK -----------------
const CONFIG = {
  SHEET_ID: process.env.SHEET_ID || '1vi-z__fFdVhUZr3PEDjhM83kqhFtbJX0Ejcfu9M8RKo',
  SHEET_RANGE: process.env.SHEET_RANGE || 'achiv!A4',
  KEYFILE_NAME: process.env.KEYFILE_NAME || 'creds.json',

  NREGA_URL: process.env.NREGA_URL || 'https://nreganarep.nic.in/netnrega/demand_emp_demand.aspx?file1=empprov&page1=b&lflag=eng&state_name=MADHYA+PRADESH&state_code=17&district_name=BALAGHAT&district_code=1738&block_code=1738002&block_name=KHAIRLANJI&fin_year=2025-2026&source=national&rbl=0&rblhpb=Both&Digest=oDzFUp3uDTVmeqEgUV5uKA'
};
// -------------------------------------------------------------

const axios = require('axios');
const cheerio = require('cheerio');
const { google } = require('googleapis');
const path = require('path');
require('dotenv').config();

// Use config values
const SHEET_ID = CONFIG.SHEET_ID;
const SHEET_RANGE = CONFIG.SHEET_RANGE;
const NREGA_URL = CONFIG.NREGA_URL;
const KEYFILE = path.join(__dirname, CONFIG.KEYFILE_NAME);

// Central run spreadsheet (shared by all scripts). Can be set via env.
const CENTRAL_RUN_SPREADSHEET_ID = process.env.RUN_SPREADSHEET_ID || '1bsS9b0FDjzPghhAfMW0YRsTdNnKdN6QMC6TS8vxlsJg';

const AXIOS_TIMEOUT = 20000;
const RETRIES = 3;

// --- Helpers ---
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
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

// --- SCRAPE ---
async function scrapeTables() {
  console.log('🔧 Running achiv.cjs scrape...');

  const axiosHttp = axios.create({ timeout: AXIOS_TIMEOUT, headers: { "User-Agent": "Mozilla/5.0" } });
  const { data: html } = await axiosHttp.get(NREGA_URL);
  const $ = cheerio.load(html);

  const tables = $('table');
  let finalData = [];

  // Table 2 (index 1): extract one-line date row
  const table2Text = tables.eq(1).text().replace(/\s+/g, ' ').trim();
  const dateMatch = table2Text.match(/(\d{2}-\w{3}-\d{4} \d{2}:\d{2}:\d{2} [AP]M)/);
  const dateRow = [dateMatch ? `Date: ${dateMatch[1]}` : 'Date not found'];
  finalData.push(dateRow); // Push date as first row

  // Table 6 (index 5): main data
  const table6 = tables.eq(5);
  table6.find('tr').each((_, row) => {
    const rowData = [];
    $(row).find('th, td').each((_, cell) => {
      rowData.push($(cell).text().trim());
    });
    if (rowData.length > 0) finalData.push(rowData);
  });

  console.log(`📋 Extracted ${Math.max(0, finalData.length - 1)} data rows (plus date).`);
  return { data: finalData, tableFound: table6.length > 0, rowCount: Math.max(0, finalData.length - 1) };
}

// --- GOOGLE AUTH (reusable client for both data and central writes) ---
async function getSheetsClientWithKeyfile() {
  const auth = new google.auth.GoogleAuth({
    keyFile: KEYFILE,
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });
  const client = await auth.getClient();
  return google.sheets({ version: 'v4', auth: client });
}

// --- Clear old sheet data ---
async function clearSheet(sheets) {
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SHEET_ID,
    range: 'achiv!A4:Z',
  });
  console.log('🧹 Cleared achiv!A4:Z');
}

// --- Write scraped data to configured sheet ---
async function writeToSheet(data) {
  const sheets = await getSheetsClientWithKeyfile();

  // Clear existing data before writing
  try {
    await clearSheet(sheets);
  } catch (e) {
    console.warn('Could not clear achiv!A4:Z (continuing):', e && e.message ? e.message : e);
  }

  // Write new data to configured range (SHEET_RANGE)
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: SHEET_RANGE,
    valueInputOption: 'RAW',
    requestBody: { values: data }
  });

  console.log('✅ Data successfully written to Google Sheet labour report achiv sheet.');
}

// ---------------- Central SCRAPE_STATUS + run sheet functions ----------------
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
      valueInputOption: 'RAW',
      requestBody: { values: [['row','panchayat','finYear','status','run_note']] }
    });
  }
}

async function appendFailuresToCentralScrapeStatus(sheetsClient, failures, runNote) {
  if (!failures || failures.length === 0) return;
  await ensureCentralScrapeStatus(sheetsClient);
  const rows = failures.map(f => [f.row || '', f.panchayat || 'UNKNOWN', f.finYear || 'UNKNOWN', f.status || 'ERROR', runNote]);
  await sheetsClient.spreadsheets.values.append({
    spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
    range: 'SCRAPE_STATUS!A2',
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
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
    // Write filename into header cell
    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range: `run!${colLetter}1`,
      valueInputOption: 'RAW',
      requestBody: { values: [[fileBasename]] }
    });
    return colLetter;
  } catch (e) {
    console.warn('Could not read/write run header, defaulting to column A:', e && e.message ? e.message : e);
    return 'A';
  }
}

// Deduplicated runs count: count unique run_note values for today
async function countRunsToday(sheetsClient) {
  try {
    const res = await sheetsClient.spreadsheets.values.get({ spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID, range: 'SCRAPE_STATUS!A2:E' });
    const rows = res.data.values || [];
    const todayPart = new Date().toLocaleDateString('en-IN'); // e.g. '17/9/2025'
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
    lines.push('');
    lines.push('');
    lines.push('');
  } else {
    for (let i = 0; i < 4; i++) {
      if (i < failures.length) {
        const f = failures[i];
        lines.push(`${f.panchayat || 'UNKNOWN'}\t${f.finYear || 'UNKNOWN'}`);
      } else {
        lines.push('');
      }
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
    valueInputOption: 'RAW',
    requestBody: { values }
  });
  console.log(`Run note written to central run!${colLetter}3:${colLetter}12`);
}

// --- MAIN runner ---
async function main() {
  const failures = [];
  let scrapedData = [];
  let tableFound = false;

  try {
    const result = await scrapeTables();
    scrapedData = result.data || [];
    tableFound = !!result.tableFound;
    const rowCount = result.rowCount || 0;

    if (!tableFound || rowCount === 0) {
      const meta = extractFromUrl(NREGA_URL);
      failures.push({ row: '', panchayat: meta.panchayat || 'UNKNOWN', finYear: meta.finYear || 'UNKNOWN', status: tableFound ? 'NO_ROWS' : 'NO_TABLE' });
      console.warn('⚠️ Table6 (index 5) missing or empty — recording failure.');
    } else {
      console.log(`✅ Scraped ${rowCount} rows (plus date).`);
    }
  } catch (err) {
    const meta = extractFromUrl(NREGA_URL);
    const msg = (err && err.code) ? `${err.code}` : (err && err.message) ? err.message : String(err);
    failures.push({ row: '', panchayat: meta.panchayat || 'UNKNOWN', finYear: meta.finYear || 'UNKNOWN', status: `ERROR:${msg}` });
    console.error('❌ Error during scrape:', msg);
  }

  // Write scraped data to the configured sheet (best-effort)
  try {
    await writeToSheet(scrapedData);
  } catch (err) {
    console.warn('Could not write scraped data to sheet (continuing):', err && err.message ? err.message : err);
  }

  // Prepare run note and append failures to central SCRAPE_STATUS
  const runner = process.env.RUNNER_TYPE || (process.env.TERMUX_VERSION ? 'mobile' : (process.platform || '').toLowerCase().includes('android') ? 'mobile' : 'system');
  const filePath = path.resolve(__filename || 'achiv.cjs');
  const nowFull = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
  const runNote = `${filePath} | ${nowFull} | ${runner}`;

  try {
    const centralSheetsClient = await getSheetsClientWithKeyfile();
    await appendFailuresToCentralScrapeStatus(centralSheetsClient, failures, runNote);
    await ensureCentralRunSheet(centralSheetsClient);
    const basename = path.basename(__filename || 'achiv.cjs');
    const col = await getRunColumnLetter(centralSheetsClient, basename);
    const runsToday = await countRunsToday(centralSheetsClient);
    await writeRunNoteToColumn(centralSheetsClient, col, filePath, nowFull, runner, failures, runsToday);
  } catch (e) {
    console.warn('Could not update central run/SCRAPE_STATUS:', e && e.message ? e.message : e);
  }

  console.log('🔚 achiv.cjs finished.');
}

// Execute
main().catch(err => console.error('❌ Error:', err));
