// A1.cjs (final) - writes scraped data to DATA_SPREADSHEET_ID (R1.1)
// run notes and SCRAPE_STATUS go to CENTRAL_RUN_SPREADSHEET_ID (shared by all scripts)

const axios = require("axios");
const cheerio = require("cheerio");
const { google } = require("googleapis");
const path = require("path");

// ---------------- CONFIG ----------------
const DATA_SPREADSHEET_ID =
  process.env.DATA_SPREADSHEET_ID ||
  "1vi-z__fFdVhUZr3PEDjhM83kqhFtbJX0Ejcfu9M8RKo";

const CENTRAL_RUN_SPREADSHEET_ID =
  process.env.RUN_SPREADSHEET_ID ||
  "1bsS9b0FDjzPghhAfMW0YRsTdNnKdN6QMC6TS8vxlsJg";

const RETRIES = 3;
const AXIOS_TIMEOUT = 20000;

// --------------- AUTH -------------------
let creds;
if (process.env.GOOGLE_CREDENTIALS_BASE64) {
  creds = JSON.parse(
    Buffer.from(process.env.GOOGLE_CREDENTIALS_BASE64, "base64").toString(
      "utf8"
    )
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
    const fy =
      u.searchParams.get("fin_year") || u.searchParams.get("FIN_YEAR") || "";
    return {
      panchayat: decodeURIComponent(p).toUpperCase().trim() || "UNKNOWN",
      finYear: fy || "UNKNOWN",
    };
  } catch (e) {
    return { panchayat: "UNKNOWN", finYear: "UNKNOWN" };
  }
}

// --------------- WRITE TO R1.1 ----------------
async function writeToSheet_R1_1(sheetsClient, infoRow, dataRows) {
  await sheetsClient.spreadsheets.values.update({
    spreadsheetId: DATA_SPREADSHEET_ID,
    range: "R1.1!A3",
    valueInputOption: "RAW",
    requestBody: { values: infoRow },
  });

  await sheetsClient.spreadsheets.values.update({
    spreadsheetId: DATA_SPREADSHEET_ID,
    range: "R1.1!A4",
    valueInputOption: "RAW",
    requestBody: { values: dataRows },
  });
}

// --------------- SCRAPE -----------------
async function fetchAndScrape(url) {
  const axiosHttp = axios.create({
    timeout: AXIOS_TIMEOUT,
    headers: { "User-Agent": "Mozilla/5.0" },
  });
  const res = await axiosHttp.get(url);
  const $ = cheerio.load(res.data);

  const table2Text = $("table").eq(1).text().replace(/\s+/g, " ").trim();
  const infoRow = [[table2Text]];

  const table7 = $("table").eq(6);
  const rows = [];
  table7.find("tr").each((i, row) => {
    const rowData = [];
    $(row)
      .find("th, td")
      .each((j, cell) => {
        const text = $(cell).text().replace(/\s+/g, " ").trim();
        rowData.push(text);
      });
    rows.push(rowData);
  });

  return { infoRow, dataRows: rows, tableFound: table7.length > 0 };
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
      requestBody: {
        values: [["row", "panchayat", "finYear", "status", "run_note"]],
      },
    });
  }
}

async function appendFailuresToCentralScrapeStatus(
  sheetsClient,
  failures,
  runNote
) {
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

// --------------- CENTRAL RUN sheet helpers ----------------
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
    const headers = (res.data.values && res.data.values[0])
      ? res.data.values[0]
      : [];
    for (let i = 0; i < headers.length; i++) {
      if (String(headers[i]).trim() === fileBasename)
        return colLetterFromIndex(i + 1);
    }
    let emptyIndex = headers.findIndex(
      (h) => h === undefined || h === null || String(h).trim() === ""
    );
    if (emptyIndex === -1) emptyIndex = headers.length;
    const colIdx = emptyIndex + 1;
    const colLetter = colLetterFromIndex(colIdx);
    await sheetsClient.spreadsheets.values.update({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range: `run!${colLetter}1`,
      valueInputOption: "RAW",
      requestBody: { values: [[fileBasename]] },
    });
    return colLetter;
  } catch (e) {
    console.warn(
      "run header read/write failed, defaulting to column A:",
      e && e.message ? e.message : e
    );
    return "A";
  }
}

// ✅ Deduplication fix here
async function countRunsToday(sheetsClient) {
  try {
    const res = await sheetsClient.spreadsheets.values.get({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range: "SCRAPE_STATUS!A2:E",
    });
    const rows = res.data.values || [];
    const todayPart = new Date().toLocaleDateString("en-IN"); // '17/9/2025'
    const seen = new Set();
    for (const r of rows) {
      const runNote = (r[4] || "").toString();
      if (runNote.includes(todayPart)) seen.add(runNote);
    }
    return seen.size;
  } catch (e) {
    return 0;
  }
}

async function writeRunNoteToColumn(
  sheetsClient,
  colLetter,
  filePath,
  nowFull,
  runner,
  failures,
  runsToday
) {
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
  lines.push(`5) Run complete`);
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
  console.log("🔧 Running A1.cjs scrape...");
  const sheetsClient = await getSheetsClient();

  const url =
    "https://nreganarep.nic.in/netnrega/app_issue.aspx?page=b&lflag=&state_name=MADHYA+PRADESH&state_code=17&district_name=BALAGHAT&district_code=1738&block_code=1738002&block_name=KHAIRLANJI&fin_year=2025-2026&source=national&Digest=AS/EzXOjY5nZjEFgC7kuSQ";

  const failures = [];
  let infoRow = [[""]];
  let dataRows = [];

  try {
    const scraped = await fetchAndScrape(url);
    infoRow = scraped.infoRow || [[""]];
    dataRows = scraped.dataRows || [];

    if (!scraped.tableFound || !dataRows.length) {
      const meta = extractFromUrl(url);
      failures.push({
        row: "",
        panchayat: meta.panchayat || "UNKNOWN",
        finYear: meta.finYear || "UNKNOWN",
        status: scraped.tableFound ? "NO_ROWS" : "NO_TABLE",
      });
      console.warn("⚠️ Table7 missing or empty — recording failure.");
    }

    console.log(
      `📋 Writing 1 info line and ${dataRows.length} table rows to R1.1 (data sheet)...`
    );
    await writeToSheet_R1_1(sheetsClient, infoRow, dataRows);
    console.log("✅ Data successfully written to R1.1 sheet.");
  } catch (err) {
    const meta = extractFromUrl(url);
    const msg =
      err && err.code
        ? `${err.code}`
        : err && err.message
        ? err.message
        : String(err);
    failures.push({
      row: "",
      panchayat: meta.panchayat || "UNKNOWN",
      finYear: meta.finYear || "UNKNOWN",
      status: `ERROR:${msg}`,
    });
    console.error("❌ Error during scrape/write:", msg);
  }

  // Prepare run-note and append failures
  const runner =
    process.env.RUNNER_TYPE ||
    (process.env.TERMUX_VERSION
      ? "mobile"
      : (process.platform || "").toLowerCase().includes("android")
      ? "mobile"
      : "system");
  const filePath = path.resolve(__filename || "A1.cjs");
  const nowFull = new Date().toLocaleString("en-IN", {
    timeZone: "Asia/Kolkata",
  });
  const runNote = `${filePath} | ${nowFull} | ${runner}`;

  try {
    await appendFailuresToCentralScrapeStatus(sheetsClient, failures, runNote);
  } catch (e) {
    console.warn(
      "Could not append failures to central SCRAPE_STATUS:",
      e && e.message ? e.message : e
    );
  }

  try {
    await ensureCentralRunSheet(sheetsClient);
    const basename = path.basename(__filename || "A1.cjs");
    const col = await getRunColumnLetter(sheetsClient, basename);
    const runsToday = await countRunsToday(sheetsClient);
    await writeRunNoteToColumn(
      sheetsClient,
      col,
      filePath,
      nowFull,
      runner,
      failures,
      runsToday
    );
  } catch (e) {
    console.warn(
      "Could not write run-note to central run sheet:",
      e && e.message ? e.message : e
    );
  }

  console.log("🔚 A1.cjs finished.");
})();
