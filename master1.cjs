// master.cjs (final) - writes scraped data to Sheet5!A19/A20; run notes & failures to central run sheet
// Now uses centralized path.cjs for config, creds, axios, cheerio and Sheets client

const builtinPath = require("path");
require("dotenv").config();
const P = require("./path.cjs");

// --------------- MAIN ---------------
(async () => {
  console.log("🔧 Running master.cjs scrape...");

  // load/validate env + libs
  let CONFIG;
  try {
    CONFIG = await P.checkAndReport();
  } catch (e) {
    console.error("❌ Env check failed:", e && e.message ? e.message : e);
    process.exit(1);
  }

  // resolved config + libs
  const AXIOS_TIMEOUT = CONFIG.AXIOS_TIMEOUT;
  const axiosHttp = CONFIG.axios;
  const cheerioLib = CONFIG.cheerio;
  const DATA_SPREADSHEET_ID = process.env.SPREADSHEET_ID || CONFIG.DATA_SPREADSHEET_ID;
  const INFO_RANGE = process.env.INFO_RANGE || "Sheet5!A18";
  const DATA_RANGE = process.env.DATA_RANGE || "Sheet5!A20:G1000";
  const CENTRAL_RUN_SPREADSHEET_ID = process.env.RUN_SPREADSHEET_ID || CONFIG.CENTRAL_RUN_SPREADSHEET_ID;

  // get sheets client via helper
  let sheetsClient;
  try {
    sheetsClient = await CONFIG.getSheetsClient();
  // ===== Show target Google Sheet =====
    const meta = await sheetsClient.spreadsheets.get({
        spreadsheetId: DATA_SPREADSHEET_ID,
    });

    console.log("📄 Target Spreadsheet :", meta.data.properties.title);
    console.log("🆔 Spreadsheet ID    :", DATA_SPREADSHEET_ID);
    console.log("📑 Info Range        :", INFO_RANGE);
    console.log("📑 Data Range        :", DATA_RANGE);
    // ================================
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

  // --------------- Write to Sheet ----------------
  async function writeToSheet(infoRow, dataRows) {


  // 🧹 Purana data hatao
  await sheetsClient.spreadsheets.values.clear({
    spreadsheetId: DATA_SPREADSHEET_ID,
    range: "Sheet5!A18:G1000",
  });



    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: DATA_SPREADSHEET_ID,
      range: INFO_RANGE,
      valueInputOption: "RAW",
      requestBody: { values: infoRow },
    });

    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: DATA_SPREADSHEET_ID,
      range: DATA_RANGE,
      valueInputOption: "RAW",
      requestBody: { values: dataRows },
    });
  }

  // --------------- Scrape logic ----------------
  async function fetchAndScrape(url) {
    const res = await axiosHttp.get(url, { timeout: AXIOS_TIMEOUT, headers: { "User-Agent": "Mozilla/5.0" } });
    const $ = cheerioLib.load(res.data);
//-----------------
console.log("Total tables =", $("table").length);

$("table").each((i, t) => {
  console.log(
    "Table", i,
    "| Rows:", $(t).find("tr").length,
    "| Text:",
    $(t).text().replace(/\s+/g, " ").substring(0, 100)
  );
});
//----
require("fs").writeFileSync("master_debug.html", res.data);
const reportDate = new Date().toLocaleString("en-IN", {
  timeZone: "Asia/Kolkata",
});

const infoRow = [
  ["VBGRAMG Labour Report"],
  [`Updated On : ${reportDate}`]
];

    //const table2Text = $("table").eq(1).text().replace(/\s+/g, " ").trim();
    //const infoRow = [[table2Text]];

    const rows = [];
    $("table").eq(0).find("tr").each((_, row) => {
      const rowData = [];
      $(row).find("th, td").each((_, cell) => rowData.push($(cell).text().trim()));
      rows.push(rowData);
    });

    return { infoRow, dataRows: rows };
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
        valueInputOption: 'RAW',
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
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
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
      lines.push("NONE");
      lines.push(""); lines.push(""); lines.push("");
    } else {
      for (let i = 0; i < 4; i++) {
        if (i < failures.length) {
          const f = failures[i];
          lines.push(`${f.panchayat || "UNKNOWN"}\t${f.finYear || "UNKNOWN"}`);
        } else {
          lines.push("");
        }
      }
    }
    lines.push(`Runs today: ${runsToday}`);
    lines.push("5) Run complete");
    while (lines.length < 10) lines.push("");
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
  const url = "https://vbgramgrep.dord.gov.in/VBGRAMG/dpc_sms_new_dtl.aspx?payload=nF24fD1Oexee9f2YmvsKEmA9PMUeMZYJRCqlO1EycgsqRFxS3kJmbwl3Su2iO1fpDLQcKz4X4E66a4OpN4pMOyC6-mi9T18yN6ejm115Duf37j8xsivpWRge-YyyATRY5MYrYrpWSOVJ0EJWYTvztj0HaZ1U8PwPS1_WKAZI_Z3Otx3Eq3zB7JSV6mQ4rjY_IxyMwkplvBTMyieoKaxlRa8X-bVsmUEtyXj3MMx1HRbeoeoBw-GEzrnJ_vVjb_6zj0MYhFw25Y9KxDt7VB11UJG63osBU6VjlQdZWlsoCwIURg26jnKauDMKn95so9PquoVAh49lpIyTixvWuDu6Vw";

  const failures = [];
  let infoRow = [[""]];
  let dataRows = [];

  try {
    const scraped = await fetchAndScrape(url);
    infoRow = scraped.infoRow || [[""]];
    dataRows = scraped.dataRows || [];

    if (!dataRows || dataRows.length === 0) {
      const meta = extractFromUrl(url);
      failures.push({ row: "", panchayat: meta.panchayat || "UNKNOWN", finYear: meta.finYear || "UNKNOWN", status: "NO_DATA" });
      console.warn("⚠️ No table rows extracted — recording failure.");
    } else {
      console.log(`📋 Writing 1 info line and ${dataRows.length} table rows...`);
    }

    await writeToSheet(infoRow, dataRows);
    console.log("✅ Data successfully written to Google Sheet Labour report Sheet5!");
  } catch (error) {
    const meta = extractFromUrl(url);
    const msg = (error && error.code) ? `${error.code}` : (error && error.message) ? error.message : String(error);
    failures.push({ row: "", panchayat: meta.panchayat || "UNKNOWN", finYear: meta.finYear || "UNKNOWN", status: `ERROR:${msg}` });
    console.error("❌ Error:", msg);
  }

  // Run-note + failures to central sheet
  const runner = process.env.RUNNER_TYPE || (process.env.TERMUX_VERSION ? "mobile" : (process.platform || "").toLowerCase().includes("android") ? "mobile" : "system");
  const filePath = builtinPath.resolve(__filename || "master.cjs");
  const nowFull = new Date().toLocaleDateString("en-IN") + ", " + new Date().toLocaleTimeString("en-IN");
  const runNote = `${filePath} | ${nowFull} | ${runner}`;

  try {
    await appendFailuresToCentralScrapeStatus(sheetsClient, failures, runNote);
  } catch (e) {
    console.warn("Could not append failures to central SCRAPE_STATUS:", e && e.message ? e.message : e);
  }

  try {
    await ensureCentralRunSheet(sheetsClient);
    const basename = builtinPath.basename(__filename || "master.cjs");
    const col = await getRunColumnLetter(sheetsClient, basename);
    const runsToday = await countRunsToday(sheetsClient);
    await writeRunNoteToColumn(sheetsClient, col, filePath, nowFull, runner, failures, runsToday);
  } catch (e) {
    console.warn("Could not write run-note to central run sheet:", e && e.message ? e.message : e);
  }

  console.log("🔚 master.cjs finished.");
})();
