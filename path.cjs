/* path.cjs
   Centralized config + environment + library checks for nrega-scraper
   Save this file as C:\Users\q_mdi\documents\nrega-scraper\path.cjs
*/
const fs = require("fs");
const os = require("os");
const path = require("path");
const axiosLib = require("axios");
const cheerioLib = require("cheerio");
const { google } = require("googleapis");

const DEFAULTS = {
  DATA_SPREADSHEET_ID: "1vi-z__fFdVhUZr3PEDjhM83kqhFtbJX0Ejcfu9M8RKo",
  CENTRAL_RUN_SPREADSHEET_ID: "1bsS9b0FDjzPghhAfMW0YRsTdNnKdN6QMC6TS8vxlsJg",
  AXIOS_TIMEOUT: 20000,
  RETRIES: 3,
  USER_AGENT: "Mozilla/5.0 (compatible; nrega-scraper/1.0)",
};

function getEnvOrDefault(name, def) {
  return process.env[name] && process.env[name].trim() !== ""
    ? process.env[name]
    : def;
}

function safeJSONParse(label, s) {
  try {
    return JSON.parse(s);
  } catch (e) {
    const snippet = String(s || "").slice(0, 200).replace(/\s+/g, " ");
    throw new Error(`Invalid ${label} JSON: ${e.message} :: snippet="${snippet}"`);
  }
}

function readLocalCredsSync(filename = "./creds.json") {
  if (!fs.existsSync(filename)) return null;
  const raw = fs.readFileSync(filename, "utf8").replace(/^\uFEFF/, "");
  return safeJSONParse(filename, raw);
}

function looksLikeBase64(s) {
  // Rough check: only base64 chars and reasonably long
  return /^[A-Za-z0-9+/=\r\n]+$/.test(s) && s.length >= 100;
}

function parseCredsFromEnv() {
  const b64 = process.env.GOOGLE_CREDENTIALS_BASE64;
  const raw = process.env.GOOGLE_CREDENTIALS_JSON;

  if (raw && raw.trim()) {
    const json = raw.replace(/^\uFEFF/, "");
    return safeJSONParse("GOOGLE_CREDENTIALS_JSON", json);
  }

  if (b64 && b64.trim()) {
    const val = b64.trim();

    // If someone pasted raw JSON into the *_BASE64 var, detect and parse JSON directly.
    if (val.includes("{") && val.includes("}")) {
      return safeJSONParse("GOOGLE_CREDENTIALS_BASE64(raw-json)", val);
    }

    if (!looksLikeBase64(val)) {
      throw new Error("GOOGLE_CREDENTIALS_BASE64 does not look like valid Base64 (invalid characters/too short).");
    }

    let decoded;
    try {
      decoded = Buffer.from(val, "base64").toString("utf8").replace(/^\uFEFF/, "");
    } catch (e) {
      throw new Error(`Failed to base64-decode GOOGLE_CREDENTIALS_BASE64: ${e.message}`);
    }
    return safeJSONParse("GOOGLE_CREDENTIALS_BASE64(decoded)", decoded);
  }

  return null;
}

function normalizePrivateKey(creds) {
  if (creds && typeof creds.private_key === "string") {
    // If the key has literal '\n' sequences, convert to actual newlines.
    if (creds.private_key.includes("\\n") && !creds.private_key.includes("\n")) {
      creds.private_key = creds.private_key.replace(/\\n/g, "\n");
    }
    // Trim accidental spaces
    creds.private_key = creds.private_key.trim();
  }
  return creds;
}

function getCreds() {
  const envCreds = parseCredsFromEnv();
  if (envCreds) return normalizePrivateKey(envCreds);

  const local = readLocalCredsSync("./creds.json");
  if (local) return normalizePrivateKey(local);

  throw new Error("❌ No credentials found: set GOOGLE_CREDENTIALS_BASE64 or GOOGLE_CREDENTIALS_JSON or provide ./creds.json");
}

function makeAxiosInstance(timeoutMs, extraHeaders) {
  const instance = axiosLib.create({
    timeout: Number(timeoutMs) || DEFAULTS.AXIOS_TIMEOUT,
    headers: Object.assign(
      { "User-Agent": getEnvOrDefault("USER_AGENT", DEFAULTS.USER_AGENT) },
      extraHeaders || {}
    ),
  });

  const RETRIES = Number(getEnvOrDefault("RETRIES", DEFAULTS.RETRIES));
  instance.interceptors.response.use(
    (r) => r,
    async (err) => {
      const config = err.config || {};
      config.__retryCount = config.__retryCount || 0;
      if (config.__retryCount >= RETRIES) return Promise.reject(err);
      config.__retryCount += 1;
      await new Promise((res) => setTimeout(res, 500 * config.__retryCount));
      return instance(config);
    }
  );

  return instance;
}

async function getSheetsClient(creds) {
  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  const client = await auth.getClient();
  return google.sheets({ version: "v4", auth: client });
}

async function checkAndReport() {
  const problems = [];
  const warnings = [];

  const DATA_SPREADSHEET_ID = getEnvOrDefault("DATA_SPREADSHEET_ID", DEFAULTS.DATA_SPREADSHEET_ID);
  const CENTRAL_RUN_SPREADSHEET_ID = getEnvOrDefault("RUN_SPREADSHEET_ID", DEFAULTS.CENTRAL_RUN_SPREADSHEET_ID);

  if (!DATA_SPREADSHEET_ID || DATA_SPREADSHEET_ID.length < 10)
    problems.push("DATA_SPREADSHEET_ID missing/too short");

  if (!CENTRAL_RUN_SPREADSHEET_ID || CENTRAL_RUN_SPREADSHEET_ID.length < 10)
    problems.push("RUN_SPREADSHEET_ID missing/too short");

  let creds = null;
  try {
    creds = getCreds();
  } catch (e) {
    problems.push(e.message);
  }

  if (creds) {
    if (!creds.client_email) warnings.push("⚠ creds.client_email missing");
    if (!creds.private_key) {
      problems.push("❌ creds.private_key missing or empty");
    } else if (!creds.private_key.includes("BEGIN PRIVATE KEY")) {
      warnings.push("creds.private_key does not appear to contain a private key header");
    }
  }

  console.log("---- nrega-scraper environment check ----");
  console.log(`Node: ${process.version}`);
  console.log(`Platform: ${process.platform} ${os.type()} ${os.arch()}`);
  console.log(`DATA_SPREADSHEET_ID: ${!!DATA_SPREADSHEET_ID}`);
  console.log(`RUN_SPREADSHEET_ID: ${!!CENTRAL_RUN_SPREADSHEET_ID}`);
  console.log(`GOOGLE_CREDENTIALS_BASE64: ${!!process.env.GOOGLE_CREDENTIALS_BASE64}`);
  console.log(`GOOGLE_CREDENTIALS_JSON: ${!!process.env.GOOGLE_CREDENTIALS_JSON}`);
  console.log(`./creds.json: ${fs.existsSync("./creds.json")}`);

  if (warnings.length) {
    console.log("\nWarnings:");
    warnings.forEach((w) => console.log("  • " + w));
  }

  if (problems.length) {
    console.log("\nErrors:");
    problems.forEach((p) => console.log("  ✖ " + p));
    throw new Error("Environment validation failed");
  }

  console.log("\n✅ Environment check OK.\n");

  return {
    DATA_SPREADSHEET_ID,
    CENTRAL_RUN_SPREADSHEET_ID,
    AXIOS_TIMEOUT: Number(getEnvOrDefault("AXIOS_TIMEOUT", DEFAULTS.AXIOS_TIMEOUT)),
    RETRIES: Number(getEnvOrDefault("RETRIES", DEFAULTS.RETRIES)),
    creds,
    axios: makeAxiosInstance(Number(getEnvOrDefault("AXIOS_TIMEOUT", DEFAULTS.AXIOS_TIMEOUT)), {}),
    cheerio: cheerioLib,
    getSheetsClient: () => getSheetsClient(creds),
  };
}

if (require.main === module) {
  (async () => {
    try {
      const cfg = await checkAndReport();
      console.log("🔍 Testing Google Sheets auth...");
      const sheets = await cfg.getSheetsClient();
      console.log("✅ Google Sheets client created successfully.");
    } catch (e) {
      console.error("❌ path.cjs self-test failed:", e.message);
      process.exit(1);
    }
  })();
}

module.exports = {
  getEnvOrDefault,
  getCreds,
  checkAndReport,
  makeAxiosInstance,
};
