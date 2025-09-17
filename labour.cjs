// labour.cjs (final) - writes scraped tables to DATA sheet; run-notes & failures to central run sheet

const axios = require("axios");
const cheerio = require("cheerio");
const { google } = require("googleapis");
const path = require("path");
require("dotenv").config();

// ---------------- CONFIG ----------------
const SHEET_ID = process.env.SHEET_ID || "1vi-z__fFdVhUZr3PEDjhM83kqhFtbJX0Ejcfu9M8RKo";
const SHEET_RANGE = process.env.SHEET_RANGE || "R6.09!A3";
const NREGA_URL =
  process.env.NREGA_URL ||
  "https://nreganarep.nic.in/netnrega/dpc_sms_new.aspx?lflag=eng&page=b&Short_Name=MP&state_name=MADHYA+PRADESH&state_code=17&district_name=BALAGHAT&district_code=1738&block_name=KHAIRLANJI&block_code=1738002&fin_year=2025-2026&dt=&EDepartment=ALL&wrkcat=ALL&worktype=ALL&Digest=0Rg9WmyQmiHlGt6U8z1w4A";

// Central run sheet / SCRAPE_STATUS spreadsheet (shared)
const CENTRAL_RUN_SPREADSHEET_ID =
  process.env.RUN_SPREADSHEET_ID || "1bsS9b0FDjzPghhAfMW0YRsTdNnKdN6QMC6TS8vxlsJg";

const AXIOS_TIMEOUT = 20000;
const RETRIES = 3;

// --------------- AUTH -------------------
let creds;
if (process.env.GOOGLE_CREDENTIALS_BASE64) {
  creds = JSON.parse(
    Buffer.from(process.env.GOOGLE_CREDENTIALS_BASE64, "base64").toString("utf8")
  );
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
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

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
    const p =
      (u.searchParams.get("panchayat_name") ||
        u.searchParams.get("PANCHAYAT_NAME") ||
        "").replace(/\+/g, " ");
    const fy = u.searchParams.get("fin_year") || u.searchParams.get("FIN_YEAR") || "";
    return {
      panchayat: decodeURIComponent(p).toUpperCase().trim() || "UNKNOWN",
      finYear: fy || "UNKNOWN",
    };
  } catch (e) {
    return { panchayat: "UNKNOWN", finYear: "UNKNOWN" };
  }
}

// --------------- SCRAPE -----------------
async function scrapeTables() {
  console.log("🔧 Running labour.cjs scrape...");

  const axiosHttp = axios.create({
    timeout: AXIOS_TIMEOUT,
    headers: { "User-Agent": "Mozilla/5.0 (compatible; labour-scraper/1.0)" },
  });

  const res = await axiosHttp.get(NREGA_URL);
  const $ = cheerio.load(res.data);
  const tables = $("table");

  // selected table indexes: 1 (2nd) and 4 (5th)
  const selectedIndexes = [1, 4];
  const finalData = [];

  selectedIndexes.forEach((index) => {
    const t = tables.eq(index);
    if (!t || t.length === 0) return;
    t.find("tr").each((_, row) => {
      const rowData = [];
      $(row)
        .find("th, td")
        .each((_, cell) => rowData.push($(cell).text().trim()));
      if (rowData.length > 0) finalData.push(rowData);
    });
  });

  console.log(`📋 Extracted ${finalData.length} rows (including headers).`);
  return { data: finalData, tableCount: finalData.length };
}

// --------------- WRITE DATA ----------------
async function writeToSheet(sheetsClient, data) {
  await sheetsClient.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: SHEET_RANGE,
    valueInputOption: "RAW",
    requestBody: { values: data },
  });
  console.log("✅ Data successfully written to labour report (data sheet).");
}

// --------------- CENTRAL SCRAPE_STATUS ----------------
async function ensureCentralScrapeStatus(sheetsClient) {
  const meta = await sheetsClient.spreadsheets.get({
    spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
  });
  const existing = (meta.data.sheets || []).map((s) => s.properties.title);
  const target = "SCRAPE_STATUS";
  if (!existing.includes(target)) {
    await sheetsClient.spreadsheets.batchUpdate({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: target } } }] },
    });
    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range: `${target}!A1:E1`,
      valueInputOption: "RAW",
      requestBody: { values: [["row", "panchayat", "finYear", "status", "run_note"]] },
    });
  }
}

async function appendFailuresToCentralScrapeStatus(sheetsClient, failures, runNote) {
  if (!failures || failures.length === 0) return;
  await ensureCentralScrapeStatus(sheetsClient);
  const rows = failures.map((f) => [
    f.row || "",
    f.panchayat || "UNKNOWN",
    f.finYear || "UNKNOWN",
    f.status || "ERROR",
    runNote,
  ]);
  await sheetsClient.spreadsheets.values.append({
    spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
    range: "SCRAPE_STATUS!A2",
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: rows },
  });
  console.log(`📝 Appended ${rows.length} failure rows to central SCRAPE_STATUS.`);
}

// --------------- CENTRAL run sheet helpers ----------------
async function ensureCentralRunSheet(sheetsClient) {
  const meta = await sheetsClient.spreadsheets.get({
    spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
  });
  const existing = (meta.data.sheets || []).map((s) => s.properties.title);
  if (!existing.includes("run")) {
    await sheetsClient.spreadsheets.batchUpdate({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: "run" } } }] },
    });
  }
}

async function getRunColumnLetter(sheetsClient, fileBasename) {
  try {
    const res = await sheetsClient.spreadsheets.values.get({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range: "run!1:1",
    });
    const headers = (res.data.values && res.data.values[0]) ? res.data.values[0] : [];
    for (let i = 0; i < headers.length; i++) {
      if (String(headers[i]).trim() === fileBasename) return colLetterFromIndex(i + 1);
    }
    let emptyIndex = headers.findIndex((h) => h === undefined || h === null || String(h).trim() === "");
    if (emptyIndex === -1) emptyIndex = headers.length;
    const colIdx = emptyIndex + 1;
    const colLetter = colLetterFromIndex(colIdx);
    // write filename header
    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range: `run!${colLetter}1`,
      valueInputOption: "RAW",
      requestBody: { values: [[fileBasename]] },
    });
    return colLetter;
  } catch (e) {
    console.warn("Could not read/write run header, defaulting to column A:", e && e.message ? e.message : e);
    return "A";
  }
}

// Deduplicated count: unique runNote values for today
async function countRunsToday(sheetsClient) {
  try {
    const res = await sheetsClient.spreadsheets.values.get({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range: "SCRAPE_STATUS!A2:E",
    });
    const rows = res.data.values || [];
    const todayPart = new Date().toLocaleDateString("en-IN");
    const seen = new Set();
    for (const r of rows) {
      const rn = (r[4] || "").toString();
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
    lines.push("NONE");
    lines.push("");
    lines.push("");
    lines.push("");
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
  const values = lines.map((l) => [l]);
  const range = `run!${colLetter}3:${colLetter}12`;
  await sheetsClient.spreadsheets.values.update({
    spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
    range,
    valueInputOption: "RAW",
    requestBody: { values },
  });
  console.log(`Run note written to central run!${colLetter}3:${colLetter}12`);
}

// --------------- MAIN ----------------
(async () => {
  console.log("🔧 labour.cjs starting...");
  const sheetsClient = await getSheetsClient();
  const failures = [];

  let scrapedData = [];

  try {
    const res = await scrapeTables();
    scrapedData = res.data || [];
    if (!scrapedData || scrapedData.length === 0) {
      const meta = extractFromUrl(NREGA_URL);
      failures.push({
        row: "",
        panchayat: meta.panchayat || "UNKNOWN",
        finYear: meta.finYear || "UNKNOWN",
        status: "NO_DATA",
      });
      console.warn("⚠️ No data extracted - recording failure.");
    } else {
      console.log(`✅ Scraped ${scrapedData.length} rows.`);
    }
  } catch (err) {
    const meta = extractFromUrl(NREGA_URL);
    const msg = (err && err.code) ? `${err.code}` : (err && err.message) ? err.message : String(err);
    failures.push({
      row: "",
      panchayat: meta.panchayat || "UNKNOWN",
      finYear: meta.finYear || "UNKNOWN",
      status: `ERROR:${msg}`,
    });
    console.error("❌ Error during scrape:", msg);
  }

  // write scraped data to configured sheet
  try {
    await writeToSheet(sheetsClient, scrapedData);
  } catch (e) {
    console.warn("Could not write scraped data to sheet (continuing):", e && e.message ? e.message : e);
  }

  // prepare runNote and append failures to central SCRAPE_STATUS + write run note
  const runner = process.env.RUNNER_TYPE || (process.env.TERMUX_VERSION ? "mobile" : (process.platform || "").toLowerCase().includes("android") ? "mobile" : "system");
  const filePath = path.resolve(__filename || "labour.cjs");
  const nowFull = new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" });
  const runNote = `${filePath} | ${nowFull} | ${runner}`;

  try {
    await appendFailuresToCentralScrapeStatus(sheetsClient, failures, runNote);
  } catch (e) {
    console.warn("Could not append failures to central SCRAPE_STATUS:", e && e.message ? e.message : e);
  }

  try {
    await ensureCentralRunSheet(sheetsClient);
    const basename = path.basename(__filename || "labour.cjs");
    const col = await getRunColumnLetter(sheetsClient, basename);
    const runsToday = await countRunsToday(sheetsClient);
    await writeRunNoteToColumn(sheetsClient, col, filePath, nowFull, runner, failures, runsToday);
  } catch (e) {
    console.warn("Could not write run-note to central run sheet:", e && e.message ? e.message : e);
  }

  console.log("🔚 labour.cjs finished.");
})();
