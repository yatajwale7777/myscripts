// labour.cjs (final) - writes scraped tables to DATA sheet; run-notes & failures to central run sheet
// Now uses centralized path.cjs for config, creds, axios, cheerio and Sheets client

const builtinPath = require("path");
require("dotenv").config();
const P = require("./path.cjs");

// ---------------- MAIN ----------------
(async () => {
  console.log("🔧 labour.cjs starting...");

  // load/validate environment and get configured libs/creds
  let CONFIG = null;
  try {
    CONFIG = await P.checkAndReport();
  } catch (e) {
    console.error("❌ Env check failed:", e && e.message ? e.message : e);
    process.exit(1);
  }

  // config + libs
  const SHEET_ID = process.env.SHEET_ID || CONFIG.DATA_SPREADSHEET_ID;
  const SHEET_RANGE = process.env.SHEET_RANGE || "'R6.09'!A3";
  const NREGA_URL =
    process.env.NREGA_URL ||
    "https://vbgramgrep.dord.gov.in/VBGRAMG/dpc_sms_new.aspx?payload=S8iM6wrVRObU8VT7bmT42oo_lwjwhUaV83eojX_DF-YTER1CypdBlfq8bBC3-_2mTRKHu-BUdBawZJJ1IVsZ7k6pf0g5mqudt0Uxk0mHQkdqORpi2yF5g0qsJuj935YkXFKZAoT4NvYhA-4Wg0OzortE7sTsg0gNulo94ZLIA-7ZN_TK1mGxIuFiC8cQV2vdFTF0gbMqgeQtXnNBwME8FOhzc8Ma_bR6X0-e0YC3p7BeDQBarDXA_27w4VfVpiQF3ZIGmN7bFt5d4PFZ6ARfl1Z090Zo277-8miC_M7Oq4s5qUH45ZlECqPXXAFEshHq9FHhUOf9kTXW3pn7BRM_3DTiwiuC9MfM03cYmiALUnM";

  const CENTRAL_RUN_SPREADSHEET_ID = process.env.RUN_SPREADSHEET_ID || CONFIG.CENTRAL_RUN_SPREADSHEET_ID;

  const AXIOS_TIMEOUT = CONFIG.AXIOS_TIMEOUT;
  const RETRIES = CONFIG.RETRIES;

  const axiosHttp = CONFIG.axios;
  const cheerioLib = CONFIG.cheerio;
const res = await axiosHttp.get(NREGA_URL);

console.log(res.data.includes("R6.9 Daily Status"));





const $ = cheerioLib.load(res.data);

// Report Date from page text
const bodyText = $("body").text();

const match = bodyText.match(/as on\s+(\d{2}\/\d{2}\/\d{4})/i);
// या: const match = bodyText.match(/\d{2}\/\d{2}\/\d{4}/);

const reportDate = match ? match[1] : "";

console.log("Report Date:", reportDate);

require("fs").writeFileSync("debug.html", res.data);

  // get sheets client
  let sheetsClient;
  try {
    sheetsClient = await CONFIG.getSheetsClient();
  } catch (e) {
    console.error("❌ Could not create Google Sheets client:", e && e.message ? e.message : e);
    process.exit(1);
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

   	 // use centralized axios instance (already configured with UA/timeout/retries)
   	 const res = await axiosHttp.get(NREGA_URL);
   	 const $ = cheerioLib.load(res.data);
const bodyText = $("body").text();

const match = bodyText.match(/\d{2}\/\d{2}\/\d{4}/);
const reportDate = match ? match[0] : "";

console.log("📅 Report Date:", reportDate);

const tables = $("table");
	

    const finalData = [];

tables.each((i, table) => {
    $(table).find("tr").each((_, row) => {
        const rowData = [];

        $(row).find("th, td").each((_, cell) => {
            rowData.push($(cell).text().trim());
        });

        if (rowData.length) finalData.push(rowData);
    });
});

    

    return {
    data: finalData,
    tableCount: finalData.length,
    reportDate
};
  }

 // --------------- WRITE DATA ----------------


async function writeToSheet(sheetsClientLocal, data, reportDate) {

  // A1 में Report Date लिखें
  await sheetsClientLocal.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: "R6.09!A1",      // अगर sheet का नाम अलग है तो बदलें
    valueInputOption: "RAW",
    requestBody: {
      values: [[`Report Date : ${reportDate}`]]
    },
  });

  // Table लिखें
  await sheetsClientLocal.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: SHEET_RANGE,     // जैसे R6.09!A3
    valueInputOption: "RAW",
    requestBody: {
      values: data
    },
  });

  console.log("✅ Data successfully written to labour report (data sheet).");
}

  // --------------- CENTRAL SCRAPE_STATUS ----------------
  async function ensureCentralScrapeStatus(sheetsClientLocal) {
    const meta = await sheetsClientLocal.spreadsheets.get({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
    });
    const existing = (meta.data.sheets || []).map((s) => s.properties.title);
    const target = "SCRAPE_STATUS";
    if (!existing.includes(target)) {
      await sheetsClientLocal.spreadsheets.batchUpdate({
        spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: target } } }] },
      });
      await sheetsClientLocal.spreadsheets.values.update({
        spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
        range: `${target}!A1:E1`,
        valueInputOption: "RAW",
        requestBody: { values: [["row", "panchayat", "finYear", "status", "run_note"]] },
      });
    }
  }

  async function appendFailuresToCentralScrapeStatus(sheetsClientLocal, failures, runNote) {
    if (!failures || failures.length === 0) return;
    await ensureCentralScrapeStatus(sheetsClientLocal);
    const rows = failures.map((f) => [
      f.row || "",
      f.panchayat || "UNKNOWN",
      f.finYear || "UNKNOWN",
      f.status || "ERROR",
      runNote,
    ]);
    await sheetsClientLocal.spreadsheets.values.append({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range: "SCRAPE_STATUS!A2",
      valueInputOption: "RAW",
      insertDataOption: "INSERT_ROWS",
      requestBody: { values: rows },
    });
    console.log(`📝 Appended ${rows.length} failure rows to central SCRAPE_STATUS.`);
  }

  // --------------- CENTRAL run sheet helpers ----------------
  async function ensureCentralRunSheet(sheetsClientLocal) {
    const meta = await sheetsClientLocal.spreadsheets.get({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
    });
    const existing = (meta.data.sheets || []).map((s) => s.properties.title);
    if (!existing.includes("run")) {
      await sheetsClientLocal.spreadsheets.batchUpdate({
        spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: "run" } } }] },
      });
    }
  }

  async function getRunColumnLetter(sheetsClientLocal, fileBasename) {
    try {
      const res = await sheetsClientLocal.spreadsheets.values.get({
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
     await sheetsClient.spreadsheets.values.update({
    spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
    range: `run!${colLetter}1`,
    valueInputOption: "RAW",
    requestBody: {
        values: [[fileBasename]]
    },
});


      return colLetter;
    } catch (e) {
      console.warn("Could not read/write run header, defaulting to column A:", e && e.message ? e.message : e);
      return "A";
    }
  }

  // Deduplicated count: unique runNote values for today
  async function countRunsToday(sheetsClientLocal) {
    try {
      const res = await sheetsClientLocal.spreadsheets.values.get({
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

  async function writeRunNoteToColumn(sheetsClientLocal, colLetter, filePath, nowFull, runner, failures, runsToday) {
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
    await sheetsClientLocal.spreadsheets.values.update({
      spreadsheetId: CENTRAL_RUN_SPREADSHEET_ID,
      range,
      valueInputOption: "RAW",
      requestBody: { values },
    });
    console.log(`Run note written to central run!${colLetter}3:${colLetter}12`);
  }

 // --------------- MAIN FLOW ----------------
const failures = [];
let scrapedData = [];
//let reportDate = "";

try {
    const res = await scrapeTables();
console.log("reportDate from scrapeTables =", res.reportDate);

    scrapedData = res.data || [];
    reportDate = res.reportDate || "";   // यहाँ const नहीं लगेगा

    if (!scrapedData || scrapedData.length === 0) {
   //     ...
    } else {
        console.log(`✅ Scraped ${scrapedData.length} rows.`);
    }
} catch (err) {
   // ...
}

// write scraped data
try {
    await writeToSheet(
        sheetsClient,
        scrapedData,
        reportDate
    );
} catch (e) {
    console.warn(
        "Could not write scraped data to sheet (continuing):",
        e && e.message ? e.message : e
    );
}
  // prepare runNote and append failures to central SCRAPE_STATUS + write run note
  const runner = process.env.RUNNER_TYPE || (process.env.TERMUX_VERSION ? "mobile" : (process.platform || "").toLowerCase().includes("android") ? "mobile" : "system");
  const filePath = builtinPath.resolve(__filename || "labour.cjs");
  const nowFull = new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" });
  const runNote = `${filePath} | ${nowFull} | ${runner}`;

  try {
    await appendFailuresToCentralScrapeStatus(sheetsClient, failures, runNote);
  } catch (e) {
    console.warn("Could not append failures to central SCRAPE_STATUS:", e && e.message ? e.message : e);
  }

  try {
    await ensureCentralRunSheet(sheetsClient);
    const basename = builtinPath.basename(__filename || "labour.cjs");
    const col = await getRunColumnLetter(sheetsClient, basename);
    const runsToday = await countRunsToday(sheetsClient);
    await writeRunNoteToColumn(sheetsClient, col, filePath, nowFull, runner, failures, runsToday);
  } catch (e) {
    console.warn("Could not write run-note to central run sheet:", e && e.message ? e.message : e);
  }

  console.log("🔚 labour.cjs finished.");
})();
