// hyperlinks.cjs (final) - reads URL from Sheet2!C2, writes links to Sheet2!B6
// now uses centralized path.cjs for config, creds, axios, cheerio and Sheets client

const builtinPath = require("path");
require("dotenv").config();
const P = require("./path.cjs");

// --------------- MAIN ---------------
(async () => {
  console.log("🔧 hyperlinks.cjs starting...");

  // validate env & load configured libs
  let CONFIG;
  try {
    CONFIG = await P.checkAndReport();
  } catch (e) {
    console.error("❌ Env check failed:", e && e.message ? e.message : e);
    process.exit(1);
  }

  // resolved config + libs
  const axiosHttp = CONFIG.axios;
  const cheerioLib = CONFIG.cheerio;
  const SPREADSHEET_ID = process.env.SPREADSHEET_ID || CONFIG.CENTRAL_RUN_SPREADSHEET_ID;
  const SHEET_NAME = process.env.SHEET_NAME || "Sheet2";
  const READ_RANGE = `${SHEET_NAME}!C2`;
const WRITE_RANGE = `${SHEET_NAME}!A6:C`;
  //const WRITE_RANGE = `${SHEET_NAME}!B6`;
  const CENTRAL_RUN_SPREADSHEET_ID = process.env.RUN_SPREADSHEET_ID || CONFIG.CENTRAL_RUN_SPREADSHEET_ID;
  const AXIOS_TIMEOUT = CONFIG.AXIOS_TIMEOUT;

  // get sheets client via helper
  let sheetsClient;
  try {
    sheetsClient = await CONFIG.getSheetsClient();
  } catch (e) {
    console.error("❌ Could not create Google Sheets client:", e && e.message ? e.message : e);
    process.exit(1);
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

  const res = await axiosHttp.get(url, { timeout: AXIOS_TIMEOUT });

  const $ = cheerioLib.load(res.data);

  const links = [];

  const table = $("table").last();

  table.find("tr").slice(4).each((_, tr) => {

    const tds = $(tr).find("td");

    // Panchayat Name (2nd column)
    const panchayat = $(tds[1]).text().trim();

    $(tds).each((col, td) => {

      const a = $(td).find("a");

      if (!a.length) return;

      let href = a.attr("href");

      if (!href) return;

      if (href.startsWith("javascript:")) return;

      if (!href.startsWith("http")) {
        href = new URL(href, url).href;
      }

      let finYear = "";

      switch (col) {
        case 4:
          finYear = "2023-2024";
          break;

        case 8:
          finYear = "2024-2025";
          break;

        case 12:
          finYear = "2025-2026";
          break;

        case 16:
          finYear = "2026-2027";
          break;

        default:
          return;
      }

      links.push([
        panchayat,
        href,
        finYear
      ]);

    });

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
  async function ensureCentralScrapeStatus(sheetsClientLocal) {
    const meta = await sheetsClientLocal.spreadsheets.get({ spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID });
    const existing = (meta.data.sheets || []).map(s => s.properties.title);
    const target = 'SCRAPE_STATUS';
    if (!existing.includes(target)) {
      await sheetsClientLocal.spreadsheets.batchUpdate({
        spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: target } } }] }
      });
      await sheetsClientLocal.spreadsheets.values.update({
        spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
        range: `${target}!A1:E1`,
        valueInputOption: "RAW",
        requestBody: { values: [['row','panchayat','finYear','status','run_note']] }
      });
    }
  }

  async function appendFailuresToCentralScrapeStatus(sheetsClientLocal, failures, runNote) {
    if (!failures || failures.length === 0) return;
    await ensureCentralScrapeStatus(sheetsClientLocal);
    const rows = failures.map(f => [f.row || "", f.panchayat || "UNKNOWN", f.finYear || "UNKNOWN", f.status || "ERROR", runNote]);
    await sheetsClientLocal.spreadsheets.values.append({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range: 'SCRAPE_STATUS!A2',
      valueInputOption: "RAW",
      insertDataOption: "INSERT_ROWS",
      requestBody: { values: rows }
    });
    console.log(`📝 Appended ${rows.length} failure rows to central SCRAPE_STATUS.`);
  }

  async function ensureCentralRunSheet(sheetsClientLocal) {
    const meta = await sheetsClientLocal.spreadsheets.get({ spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID });
    const existing = (meta.data.sheets || []).map(s => s.properties.title);
    if (!existing.includes('run')) {
      await sheetsClientLocal.spreadsheets.batchUpdate({
        spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: 'run' } } }] }
      });
    }
  }

  async function getRunColumnLetter(sheetsClientLocal, fileBasename) {
    try {
      const res = await sheetsClientLocal.spreadsheets.values.get({ spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID, range: 'run!1:1' });
      const headers = (res.data.values && res.data.values[0]) ? res.data.values[0] : [];
      for (let i = 0; i < headers.length; i++) {
        if (String(headers[i]).trim() === fileBasename) return colLetterFromIndex(i + 1);
      }
      let emptyIndex = headers.findIndex(h => h === undefined || h === null || String(h).trim() === '');
      if (emptyIndex === -1) emptyIndex = headers.length;
      const colIdx = emptyIndex + 1;
      const colLetter = colLetterFromIndex(colIdx);
      await sheetsClientLocal.spreadsheets.values.update({
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

  async function countRunsToday(sheetsClientLocal) {
    try {
      const res = await sheetsClientLocal.spreadsheets.values.get({ spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID, range: 'SCRAPE_STATUS!A2:E' });
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

  async function writeRunNoteToColumn(sheetsClientLocal, colLetter, filePath, nowFull, runner, failures, runsToday) {
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
    await sheetsClientLocal.spreadsheets.values.update({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range,
      valueInputOption: "RAW",
      requestBody: { values }
    });
    console.log(`Run note written to central run!${colLetter}3:${colLetter}12`);
  }

  // --------------- MAIN ----------------
  const failures = [];

  try {
    // Read URL from sheet
    const url = await getUrlFromSheet(sheetsClient);

console.log("🌐 URL from Sheet:", url);
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
    const meta = extractFromUrl(err && err.url ? err.url : '');
    failures.push({ row: '', panchayat: meta.panchayat || 'UNKNOWN', finYear: meta.finYear || 'UNKNOWN', status: `ERROR:${msg}` });
  }

  // Prepare run-note and append failures to central SCRAPE_STATUS + write run note
  try {
    const runner = process.env.RUNNER_TYPE || (process.env.TERMUX_VERSION ? 'mobile' : (process.platform || '').toLowerCase().includes('android') ? 'mobile' : 'system');
    const filePath = builtinPath.resolve(__filename || 'hyperlinks.cjs');
    const nowFull = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    const runNote = `${filePath} | ${nowFull} | ${runner}`;

    await appendFailuresToCentralScrapeStatus(sheetsClient, failures, runNote);
    await ensureCentralRunSheet(sheetsClient);
    const basename = builtinPath.basename(__filename || 'hyperlinks.cjs');
    const col = await getRunColumnLetter(sheetsClient, basename);
    const runsToday = await countRunsToday(sheetsClient);
    await writeRunNoteToColumn(sheetsClient, col, filePath, nowFull, runner, failures, runsToday);
  } catch (e) {
    console.warn('Could not update central run/SCRAPE_STATUS:', e && e.message ? e.message : e);
  }

  console.log('🔚 hyperlinks.cjs finished.');
})();
