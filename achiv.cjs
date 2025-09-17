// scripts/achiv.cjs
const axios = require('../lib/http');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');
const { updateRange, clearRange } = require('../lib/sheets'); // use service account via env

// --- load target from config/targets.json ---
function loadTarget(scriptFile) {
  const cfgPath = path.join(__dirname, '..', 'config', 'targets.json');
  if (!fs.existsSync(cfgPath)) throw new Error("Missing config/targets.json");
  const cfg = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
  const tgt = cfg.targets?.[scriptFile];
  if (!tgt) throw new Error(`No target mapping for ${scriptFile} in config/targets.json`);
  // expected: { spreadsheetId: "...", tab: "achiv" }
  return tgt;
}

function nowISO() { return new Date().toISOString(); }

// NREGA URL (same as your original)
const NREGA_URL = 'https://nreganarep.nic.in/netnrega/demand_emp_demand.aspx?file1=empprov&page1=b&lflag=eng&state_name=MADHYA+PRADESH&state_code=17&district_name=BALAGHAT&district_code=1738&block_code=1738002&block_name=KHAIRLANJI&fin_year=2025-2026&source=national&rbl=0&rblhpb=Both&Digest=oDzFUp3uDTVmeqEgUV5uKA';

// ---- scrape helpers ----
async function scrapeTables() {
  console.log('🔧 Running achiv.cjs scrape...');

  const { data: html } = await axios.get(NREGA_URL, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
    timeout: 60000
  });
  const $ = cheerio.load(html);

  const tables = $('table');
  const finalData = [];

  // Table 2 (index 1) → extract one-line date row
  const table2Text = tables.eq(1).text().replace(/\s+/g, ' ').trim();
  const dateMatch = table2Text.match(/(\d{2}-\w{3}-\d{4} \d{2}:\d{2}:\d{2} [AP]M)/);
  const dateRow = [dateMatch ? `Date: ${dateMatch[1]}` : 'Date not found'];
  finalData.push(dateRow); // push as first row (will go at A4)

  // Table 6 (index 5) → main data
  const table6 = tables.eq(5);
  table6.find('tr').each((_, row) => {
    const rowData = [];
    $(row).find('th, td').each((_, cell) => {
      const txt = $(cell).text().replace(/\s+/g, ' ').trim();
      rowData.push(txt);
    });
    if (rowData.length > 0) finalData.push(rowData);
  });

  console.log(`📋 Extracted ${finalData.length - 1} data rows (plus date).`);
  return finalData;
}

// ---- main ----
(async () => {
  try {
    const { spreadsheetId, tab } = loadTarget('achiv.cjs'); // tab should be "achiv"

    const data = await scrapeTables();

    // 1) Clear previous data only from A4:Z (keep headers above if any)
    await clearRange(spreadsheetId, `${tab}!A4:Z`);
    console.log('🧹 Cleared', `${tab}!A4:Z`);

    // 2) Write new data starting at A4
    if (data.length > 0) {
      await updateRange(spreadsheetId, `${tab}!A4`, data, 'RAW');
    }
    console.log('✅ Data written to', `${tab}!A4`);

    process.exit(0);
  } catch (err) {
    console.error('❌ achiv.cjs Error:', err?.message || err);
    process.exit(1);
  }
})();
