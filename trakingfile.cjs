// puppeteer-scrape.cjs (final)
// original: Puppeteer scrape -> write to Google Sheet
// added: central run-note + SCRAPE_STATUS logging

const puppeteer = require("puppeteer");
const { setTimeout } = require("timers/promises");
const { google } = require("googleapis");
const fs = require("fs");
const path = require("path");
require("dotenv").config();

// CONFIG
const SPREADSHEET_ID = process.env.SPREADSHEET_ID || "1D1rgIY_KhL_F86WnCE6ey-0p07Fd8jMvhX3iGOFHpO0";
const SHEET_NAME = process.env.SHEET_NAME || "data";
const SILENT_MODE = false; // true to suppress logs

// central run sheet (shared by all scripts). Override with env RUN_SPREADSHEET_ID if needed.
const CENTRAL_RUN_SPREADSHEET_ID = process.env.RUN_SPREADSHEET_ID || "1bsS9b0FDjzPghhAfMW0YRsTdNnKdN6QMC6TS8vxlsJg";

function log(...args) {
  if (!SILENT_MODE) console.log(...args);
}

function isTermux() {
  return fs.existsSync("/data/data/com.termux/files/usr");
}

// --- Google Auth setup ---
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

// Google Sheets helpers
async function clearSheetRange() {
  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A4:Z`,
  });
  log("✅ Sheet range cleared: A4:Z");
}

async function writeToSheet(values) {
  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A4`,
    valueInputOption: "RAW",
    requestBody: { values },
  });
  log("✅ Data written to Google Sheet.");
}

// ----------------- Central SCRAPE_STATUS & run helpers -----------------
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

async function ensureCentralScrapeStatus(sheetsClient) {
  const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID });
  const existing = (meta.data.sheets || []).map(s => s.properties.title);
  const target = "SCRAPE_STATUS";
  if (!existing.includes(target)) {
    await sheetsClient.spreadsheets.batchUpdate({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: target } } }] }
    });
    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range: `${target}!A1:E1`,
      valueInputOption: "RAW",
      requestBody: { values: [["row","panchayat","finYear","status","run_note"]] }
    });
  }
}

async function appendFailuresToCentralScrapeStatus(sheetsClient, failures, runNote) {
  if (!failures || failures.length === 0) return;
  await ensureCentralScrapeStatus(sheetsClient);
  const rows = failures.map(f => [f.row || "", f.panchayat || "UNKNOWN", f.finYear || "UNKNOWN", f.status || "ERROR", runNote]);
  await sheetsClient.spreadsheets.values.append({
    spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
    range: "SCRAPE_STATUS!A2",
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: rows }
  });
  log(`📝 Appended ${rows.length} failure rows to central SCRAPE_STATUS.`);
}

async function ensureCentralRunSheet(sheetsClient) {
  const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID });
  const existing = (meta.data.sheets || []).map(s => s.properties.title);
  if (!existing.includes("run")) {
    await sheetsClient.spreadsheets.batchUpdate({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: "run" } } }] }
    });
  }
}

async function getRunColumnLetter(sheetsClient, fileBasename) {
  try {
    const res = await sheetsClient.spreadsheets.values.get({ spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID, range: "run!1:1" });
    const headers = (res.data.values && res.data.values[0]) ? res.data.values[0] : [];
    for (let i = 0; i < headers.length; i++) {
      if (String(headers[i]).trim() === fileBasename) return colLetterFromIndex(i + 1);
    }
    let emptyIndex = headers.findIndex(h => h === undefined || h === null || String(h).trim() === "");
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
    return "A";
  }
}

async function countRunsToday(sheetsClient) {
  try {
    const res = await sheetsClient.spreadsheets.values.get({ spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID, range: "SCRAPE_STATUS!A2:E" });
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
    lines.push(""); lines.push(""); lines.push("");
  } else {
    for (let i = 0; i < 4; i++) {
      if (i < failures.length) {
        const f = failures[i];
        lines.push(`${f.panchayat || "UNKNOWN"}\t${f.finYear || "UNKNOWN"}`);
      } else lines.push("");
    }
  }
  lines.push(`Runs today: ${runsToday}`);
  lines.push("5) Run complete");
  while (lines.length < 10) lines.push("");
  const values = lines.map(l => [l]);
  const range = `run!${colLetter}3:${colLetter}12`;
  await sheetsClient.spreadsheets.values.update({
    spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
    range,
    valueInputOption: "RAW",
    requestBody: { values }
  });
  log(`Run note written to central run!${colLetter}3:${colLetter}12`);
}

// ----------------- Main scraping + logging runner -----------------
(async () => {
  const failures = [];
  let tableData = [];

  try {
    // Clear old data
    await clearSheetRange();

    // Launch Puppeteer
    const browser = await puppeteer.launch({
      headless: true,
      executablePath: isTermux()
        ? "/data/data/com.termux/files/usr/bin/chromium"
        : undefined,
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
    });
    const page = await browser.newPage();

    // Open NREGA tracker page
    await page.goto(
      "https://nregastrep.nic.in/netnrega/dynamic_muster_track.aspx?lflag=eng&state_code=17&fin_year=2025-2026&state_name=%u092e%u0927%u094d%u092f+%u092a%u094d%u0930%u0926%u0947%u0936+&Digest=%2f0dclwkJQM2w4GAt8GjFPw",
      { waitUntil: "domcontentloaded" }
    );

    // Select filters
    await page.select("#ctl00_ContentPlaceHolder1_ddl_state", "17");
    await setTimeout(1500);
    await page.waitForSelector("#ctl00_ContentPlaceHolder1_ddl_dist", { timeout: 10000 });
    await page.select("#ctl00_ContentPlaceHolder1_ddl_dist", "1738");
    await setTimeout(1500);
    await page.waitForSelector("#ctl00_ContentPlaceHolder1_ddl_blk", { timeout: 15000 });
    await page.select("#ctl00_ContentPlaceHolder1_ddl_blk", "1738002");
    await setTimeout(1500);

    await page.waitForFunction(() => {
      const pan = document.querySelector("#ctl00_ContentPlaceHolder1_ddl_pan");
      return pan && Array.from(pan.options).some((opt) => opt.value === "ALL");
    }, { timeout: 5000 });

    await page.select("#ctl00_ContentPlaceHolder1_ddl_pan", "ALL");
    await setTimeout(1500);
    await page.click("#ctl00_ContentPlaceHolder1_Rbtn_pay_1");
    await setTimeout(500);

    await page.waitForSelector("#ctl00_ContentPlaceHolder1_Button1", {
      visible: true,
      timeout: 10000,
    });
    await Promise.all([
      page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }),
      page.click("#ctl00_ContentPlaceHolder1_Button1"),
    ]);

    // Wait for table rows
    await page.waitForSelector('tbody tr[bgcolor="#82b4ff"]', { timeout: 10000 });

    // Extract table
    tableData = await page.evaluate(() => {
      const rows = Array.from(document.querySelectorAll("tbody tr"));
      const headerIndex = rows.findIndex((row) => {
        const firstCell = row.querySelector("td");
        return firstCell && firstCell.innerText.trim() === "SNo.";
      });
      if (headerIndex === -1) return [];

      const headerRow = Array.from(rows[headerIndex].querySelectorAll("td")).map(
        (td) => td.innerText.trim()
      );
      const dataRows = [];
      for (let i = headerIndex + 1; i < rows.length; i++) {
        const cols = Array.from(rows[i].querySelectorAll("td"));
        if (cols.length !== headerRow.length) break;
        const rowData = cols.map((td) => td.innerText.trim());
        if (rowData.every((cell) => cell === "")) break;
        dataRows.push(rowData);
      }
      return [headerRow, ...dataRows];
    });

    // close browser
    await setTimeout(500);
    await browser.close();

    if (!tableData || tableData.length === 0) {
      // add failure entry using known meta (site lacks panchayat in URL here)
      failures.push({ row: "", panchayat: "ALL", finYear: "2025-2026", status: "NO_DATA" });
      log("⚠️ No table data extracted — recording failure.");
    } else {
      // Write data to Google Sheet
      await writeToSheet(tableData);
      log(`✅ Wrote ${tableData.length - 1} data rows to sheet.`);
    }
  } catch (err) {
    const msg = err && err.message ? err.message : String(err);
    failures.push({ row: "", panchayat: "ALL", finYear: "2025-2026", status: `ERROR:${msg}` });
    console.error("❌ Fatal error during scrape:", msg);
  }

  // Prepare run-note and append failures / write run-note to central
  try {
    const sheetsClient = await getSheetsClient();
    const runner = process.env.RUNNER_TYPE || (isTermux() ? "mobile" : (process.platform || "").toLowerCase().includes("android") ? "mobile" : "system");
    const filePath = path.resolve(__filename || "puppeteer-scrape.cjs");
    const nowFull = new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" });
    const runNote = `${filePath} | ${nowFull} | ${runner}`;

    // append failures (if any)
    try {
      await appendFailuresToCentralScrapeStatus(sheetsClient, failures, runNote);
    } catch (e) {
      console.warn("Could not append failures to central SCRAPE_STATUS:", e && e.message ? e.message : e);
    }

    // write run note column
    try {
      await ensureCentralRunSheet(sheetsClient);
      const basename = path.basename(__filename || "puppeteer-scrape.cjs");
      const col = await getRunColumnLetter(sheetsClient, basename);
      const runsToday = await countRunsToday(sheetsClient);
      await writeRunNoteToColumn(sheetsClient, col, filePath, nowFull, runner, failures, runsToday);
    } catch (e) {
      console.warn("Could not write run-note to central run sheet:", e && e.message ? e.message : e);
    }
  } catch (e) {
    console.warn("Could not update central run/SCRAPE_STATUS:", e && e.message ? e.message : e);
  }

  log("🔚 Puppeteer scrape finished.");
})();
