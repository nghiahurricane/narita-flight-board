const express = require('express');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const app = express();
app.use(cors());
app.use(express.json());

// ===== DIRECTORY SETUP =====
const publicDir = path.join(__dirname, 'public');
if (!fs.existsSync(publicDir)) fs.mkdirSync(publicDir, { recursive: true });
app.use(express.static('public'));

const historyDir = path.join(__dirname, 'history');
if (!fs.existsSync(historyDir)) fs.mkdirSync(historyDir, { recursive: true });

// ===== DATA STATE =====
const DATA_FILE = path.join(__dirname, 'flights_data.json');
let cachedData = {
    yesterday: { departures: [], arrivals: [] },
    today:     { departures: [], arrivals: [] },
    tomorrow:  { departures: [], arrivals: [] }
};
let lastUpdate = null;
let isScraping = false;

function getJSTDateStr(offsetDays) {
    const d = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo' }));
    d.setDate(d.getDate() + offsetDays);
    return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

let currentJSTDate = getJSTDateStr(0);
let lastScrapedTime = { yesterday: 0, today: 0, tomorrow: 0 };
const SCHEDULE = { today: 300000, tomorrow: 3600000 };
let startupScanDone = false;

// ===================================================
//  GOOGLE SHEETS INTEGRATION (optional)
// ===================================================
// SETUP INSTRUCTIONS:
//  1. Go to console.cloud.google.com → Create / select a project
//  2. Enable "Google Sheets API"
//  3. IAM & Admin → Service Accounts → Create → Download JSON key
//  4. Save the key file as  service_account.json  in this directory
//  5. Create a blank Google Sheet, copy its ID from the URL
//  6. Save  sheets_config.json  → { "spreadsheet_id": "YOUR_SHEET_ID" }
//  7. Open the Google Sheet → Share with the service account email (Editor)
//  8. npm install googleapis
// ===================================================
// ===================================================
//  GOOGLE SHEETS INTEGRATION — 2 tabs per day (DEP / ARR)
//  Each tab: title banner, stats row, styled headers,
//  colour-coded status rows, AutoFilter, frozen panes
// ===================================================
let sheetsApi = null;
let SPREADSHEET_ID = null;

// ── Helpers ──────────────────────────────────────────────────────────────
const rgb = (hex) => ({
    red:   parseInt(hex.slice(0,2),16)/255,
    green: parseInt(hex.slice(2,4),16)/255,
    blue:  parseInt(hex.slice(4,6),16)/255
});

// Named colours used throughout
const C = {
    darkBg:    rgb('0A0F1A'),
    headerBg:  rgb('033B7D'),
    white:     rgb('FFFFFF'),
    cyan:      rgb('00FFFF'),
    yellow:    rgb('FFCC00'),
    green:     rgb('00FF00'),
    red:       rgb('FF3333'),
    gray:      rgb('888888'),
    rowEven:   rgb('0A0F1A'),
    rowOdd:    rgb('111827'),
    onTimeBg:  rgb('003300'),
    delayBg:   rgb('332200'),
    cancelBg:  rgb('330000'),
    doneBg:    rgb('1A1A1A'),
    boardBg:   rgb('00223A'),
    // text on coloured bg
    onTimeTxt: rgb('00FF99'),
    delayTxt:  rgb('FFCC00'),
    cancelTxt: rgb('FF6666'),
    doneTxt:   rgb('888888'),
    boardTxt:  rgb('00AAFF'),
    titleBg:   rgb('011428'),
    statsBg:   rgb('050D1A'),
    statsVal:  rgb('00FFFF'),
};

function cellFmt(bgHex, fgHex, bold=false, size=10, halign='LEFT') {
    return {
        backgroundColor: bgHex,
        textFormat: { foregroundColor: fgHex, bold, fontSize: size,
                      fontFamily: 'Roboto Mono' },
        horizontalAlignment: halign,
        verticalAlignment: 'MIDDLE',
        padding: { top:4, bottom:4, left:6, right:6 }
    };
}

function repeatCell(sheetId, r1, c1, r2, c2, fmt) {
    return { repeatCell: {
        range: { sheetId, startRowIndex:r1, endRowIndex:r2,
                 startColumnIndex:c1, endColumnIndex:c2 },
        cell: { userEnteredFormat: { ...fmt, wrapStrategy: 'CLIP' } },
        fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,padding,wrapStrategy)'
    }};
}

function mergeCells(sheetId, r1, c1, r2, c2) {
    return { mergeCells: {
        range: { sheetId, startRowIndex:r1, endRowIndex:r2,
                 startColumnIndex:c1, endColumnIndex:c2 },
        mergeType: 'MERGE_ALL'
    }};
}

function statusFmt(status) {
    const s = (status||'').toUpperCase();
    if (s.includes('CANCEL')) return { bg: C.cancelBg, fg: C.cancelTxt };
    if (s.includes('DELAY'))  return { bg: C.delayBg,  fg: C.delayTxt  };
    if (s.includes('DEPART') || s.includes('ARRIVED') || s.includes('LANDED'))
        return { bg: C.doneBg, fg: C.doneTxt };
    if (s.includes('BOARD') || s.includes('GATE') || s.includes('FINAL') || s.includes('CHECK'))
        return { bg: C.boardBg, fg: C.boardTxt };
    if (s.includes('ON TIME') || s.includes('GO TO'))
        return { bg: C.onTimeBg, fg: C.onTimeTxt };
    return { bg: C.rowOdd, fg: C.white };
}

// ── Init ─────────────────────────────────────────────────────────────────
async function initGoogleSheets() {
    const saPath  = path.join(__dirname, 'service_account.json');
    const cfgPath = path.join(__dirname, 'sheets_config.json');
    if (!fs.existsSync(saPath) || !fs.existsSync(cfgPath)) {
        console.log('[Sheets] Disabled — service_account.json or sheets_config.json not found.');
        return;
    }
    try {
        const { google } = require('googleapis');
        const credentials = JSON.parse(fs.readFileSync(saPath, 'utf8'));
        const config      = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
        SPREADSHEET_ID    = config.spreadsheet_id;
        const auth = new google.auth.GoogleAuth({
            credentials,
            scopes: ['https://www.googleapis.com/auth/spreadsheets']
        });
        sheetsApi = google.sheets({ version: 'v4', auth });
        console.log('[Sheets] ✅ Enabled. Spreadsheet ID:', SPREADSHEET_ID);
    } catch (e) { console.log('[Sheets] Init error:', e.message); }
}

// ── Ensure DEP / ARR tabs exist, return their sheetIds ───────────────────
async function ensureDayTabs(dateStr) {
    if (!sheetsApi || !SPREADSHEET_ID) return null;
    try {
        const meta = await sheetsApi.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
        const existing = {};
        meta.data.sheets.forEach(s => { existing[s.properties.title] = s.properties.sheetId; });

        const depTitle = `DEP ${dateStr}`;
        const arrTitle = `ARR ${dateStr}`;
        const toCreate = [];
        if (!existing[depTitle]) toCreate.push({ title: depTitle, tabColor: rgb('033B7D') });
        if (!existing[arrTitle]) toCreate.push({ title: arrTitle, tabColor: rgb('7D1F03') });

        if (toCreate.length) {
            await sheetsApi.spreadsheets.batchUpdate({
                spreadsheetId: SPREADSHEET_ID,
                requestBody: {
                    requests: toCreate.map(t => ({
                        addSheet: { properties: { title: t.title, tabColorStyle: { rgbColor: t.tabColor } } }
                    }))
                }
            });
            // Re-fetch to get new sheetIds
            const meta2 = await sheetsApi.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
            meta2.data.sheets.forEach(s => { existing[s.properties.title] = s.properties.sheetId; });
        }
        return { depId: existing[depTitle], arrId: existing[arrTitle], depTitle, arrTitle };
    } catch (e) { console.log('[Sheets] ensureDayTabs error:', e.message); return null; }
}

// ── Build header + data for one tab ──────────────────────────────────────
function buildTabData(flights, type, dateStr) {
    const isArr = type === 'ARR';
    const label = isArr ? '✈  ARRIVALS' : '✈  DEPARTURES';
    const headers = isArr
        ? ['STA', 'ETA / ATA', 'FLIGHT', 'AIRLINE', 'ORIGIN',       'TERMINAL', 'GATE', 'BELT',   'STATUS',  'REMARK']
        : ['STD', 'ETD / ATD', 'FLIGHT', 'AIRLINE', 'DESTINATION',  'TERMINAL', 'GATE', 'CHECK-IN','STATUS',  'REMARK'];

    // Row 0: title (merged A1:J1)
    const titleRow = [`NARITA INT'L AIRPORT (NRT/RJAA)  —  ${label}  —  ${dateStr}`,
                      '','','','','','','','',''];

    // Row 1: stats — label (plain text) | count (formula, semicolon separator for JP/VN locale)
    const statsRow = [
        'TOTAL FLIGHTS',       `=COUNTA(C4:C5000)`,
        'ON TIME',             `=COUNTIF(I4:I5000;"ON TIME")`,
        'DELAYED',             `=COUNTIF(I4:I5000;"*DELAYED*")`,
        isArr ? 'ARRIVED' : 'DEPARTED',
                               isArr ? `=COUNTIF(I4:I5000;"*ARRIVED*")` : `=COUNTIF(I4:I5000;"*DEPARTED*")`,
        'CANCELLED',           `=COUNTIF(I4:I5000;"CANCELED")`,
    ];

    // Row 2: column headers
    const headerRow = headers;

    // Data rows (row 4 onwards, index 3+)
    const dataRows = (flights || []).map(f => [
        f.std        || '',
        f.etd        || '',
        f.flightNo   || '',
        f.airline    || '',
        f.location   || '',
        f.terminal   || '',
        f.gate       || '',
        f.checkin    || '',
        f.status     || '',
        f.remark     || '',
    ]);

    return { titleRow, statsRow, headerRow, dataRows, headers };
}

// ── Apply all formatting to one tab via batchUpdate ───────────────────────
async function formatTab(sheetId, flights, type, dateStr) {
    const { titleRow, statsRow, headerRow, dataRows } = buildTabData(flights, type, dateStr);
    const totalRows = 3 + dataRows.length;
    const COLS = 10; // A–J

    const requests = [];

    // 1. Freeze row 3 (header), freeze no columns
    requests.push({ updateSheetProperties: {
        properties: { sheetId, gridProperties: { frozenRowCount: 3 } },
        fields: 'gridProperties.frozenRowCount'
    }});

    // 2. Set column widths (pixels)
    // STATUS col (index 8) = 150px — wide enough for longest status like "BOARDING (DELAYED)"
    const colWidths = [65, 75, 80, 200, 240, 90, 60, 75, 150, 200];
    colWidths.forEach((px, i) => {
        requests.push({ updateDimensionProperties: {
            range: { sheetId, dimension:'COLUMNS', startIndex:i, endIndex:i+1 },
            properties: { pixelSize: px },
            fields: 'pixelSize'
        }});
    });

    // 3. Row heights
    // Title row (0) = 36px, stats row (1) = 28px, header row (2) = 24px, data rows = 22px
    requests.push({ updateDimensionProperties: {
        range: { sheetId, dimension:'ROWS', startIndex:0, endIndex:1 },
        properties: { pixelSize: 36 }, fields:'pixelSize'
    }});
    requests.push({ updateDimensionProperties: {
        range: { sheetId, dimension:'ROWS', startIndex:1, endIndex:2 },
        properties: { pixelSize: 28 }, fields:'pixelSize'
    }});
    requests.push({ updateDimensionProperties: {
        range: { sheetId, dimension:'ROWS', startIndex:2, endIndex:3 },
        properties: { pixelSize: 26 }, fields:'pixelSize'
    }});
    if (dataRows.length) {
        requests.push({ updateDimensionProperties: {
            range: { sheetId, dimension:'ROWS', startIndex:3, endIndex:3+dataRows.length },
            properties: { pixelSize: 22 }, fields:'pixelSize'
        }});
    }

    // 4. Title row: merge + format
    requests.push(mergeCells(sheetId, 0, 0, 1, COLS));
    requests.push(repeatCell(sheetId, 0, 0, 1, COLS, {
        backgroundColor: C.titleBg,
        textFormat: { foregroundColor: C.cyan, bold:true, fontSize:12, fontFamily:'Roboto Mono' },
        horizontalAlignment: 'CENTER', verticalAlignment: 'MIDDLE',
        padding: { top:4, bottom:4, left:8, right:8 }
    }));

    // 5. Stats row: alternating LABEL (col 0,2,4,6,8 → gray italic) | VALUE (col 1,3,5,7,9 → cyan bold)
    for (let c = 0; c < COLS; c++) {
        const isLabel = c % 2 === 0;
        requests.push(repeatCell(sheetId, 1, c, 2, c+1, {
            backgroundColor: C.statsBg,
            textFormat: {
                foregroundColor: isLabel ? C.gray : C.statsVal,
                italic:  isLabel,
                bold:    !isLabel,
                fontSize: isLabel ? 8 : 11,
                fontFamily: 'Roboto Mono'
            },
            horizontalAlignment: isLabel ? 'RIGHT' : 'CENTER',
            verticalAlignment: 'MIDDLE',
            padding: { top:2, bottom:2, left:4, right:4 }
        }));
    }

    // 6. Header row
    for (let c = 0; c < COLS; c++) {
        requests.push(repeatCell(sheetId, 2, c, 3, c+1, {
            backgroundColor: C.headerBg,
            textFormat: { foregroundColor: C.white, bold:true, fontSize:10, fontFamily:'Roboto Mono' },
            horizontalAlignment: 'CENTER', verticalAlignment: 'MIDDLE',
            padding: { top:4, bottom:4, left:6, right:6 }
        }));
    }

    // 7. Data rows — base alternating colour, then status overrides col I (index 8)
    dataRows.forEach((row, ri) => {
        const rIdx = 3 + ri;
        const rowBg = ri % 2 === 0 ? C.rowEven : C.rowOdd;

        // All cells base style
        for (let c = 0; c < COLS; c++) {
            let fgColor = C.white;
            if (c === 0) fgColor = C.yellow;       // STD/STA time
            else if (c === 1) fgColor = C.green;    // ETD/ETA
            else if (c === 2) fgColor = C.cyan;     // Flight No
            else if (c === 3) fgColor = C.white;    // Airline
            else if (c === 4) fgColor = C.white;    // Destination/Origin bold
            else if (c === 5) fgColor = C.cyan;     // Terminal
            else if (c === 6) fgColor = C.cyan;     // Gate
            else if (c === 7) fgColor = C.yellow;   // Checkin/Belt
            else if (c === 9) fgColor = C.green;    // Remark

            const isBold = c === 4 || c === 2;
            requests.push(repeatCell(sheetId, rIdx, c, rIdx+1, c+1, {
                backgroundColor: rowBg,
                textFormat: { foregroundColor: fgColor, bold: isBold, fontSize:10, fontFamily:'Roboto Mono' },
                horizontalAlignment: c <= 2 || c === 5 || c === 6 || c === 7 ? 'CENTER' : 'LEFT',
                verticalAlignment: 'MIDDLE',
                padding: { top:2, bottom:2, left:6, right:6 }
            }));
        }

        // Status cell (col 8) — override with status colour
        const sf = statusFmt(row[8]);
        requests.push(repeatCell(sheetId, rIdx, 8, rIdx+1, 9, {
            backgroundColor: sf.bg,
            textFormat: { foregroundColor: sf.fg, bold:true, fontSize:10, fontFamily:'Roboto Mono' },
            horizontalAlignment: 'CENTER', verticalAlignment: 'MIDDLE',
            padding: { top:2, bottom:2, left:6, right:6 }
        }));
    });

    // 8. AutoFilter on header row
    requests.push({ setBasicFilter: {
        filter: {
            range: { sheetId, startRowIndex:2, endRowIndex:3+dataRows.length,
                     startColumnIndex:0, endColumnIndex:COLS }
        }
    }});

    // 9. Hide gridlines
    requests.push({ updateSheetProperties: {
        properties: { sheetId, gridProperties: { hideGridlines: true } },
        fields: 'gridProperties.hideGridlines'
    }});

    await sheetsApi.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { requests }
    });
}

// ── Write data values ─────────────────────────────────────────────────────
async function writeTabValues(tabTitle, flights, type, dateStr) {
    const { titleRow, statsRow, headerRow, dataRows } = buildTabData(flights, type, dateStr);
    const updatedRow = ['', '', '', '', '', '', '', '', '',
        `Last updated: ${new Date().toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo' })} JST`];
    const values = [titleRow, statsRow, headerRow, ...dataRows, updatedRow];
    await sheetsApi.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `'${tabTitle}'!A1`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values }
    });
}

// ── Main write function ───────────────────────────────────────────────────
async function writeFlightDataToSheet(dateStr, depFlights, arrFlights) {
    if (!sheetsApi || !SPREADSHEET_ID) return;
    try {
        const tabs = await ensureDayTabs(dateStr);
        if (!tabs) return;

        // Write values
        await writeTabValues(tabs.depTitle, depFlights, 'DEP', dateStr);
        await writeTabValues(tabs.arrTitle, arrFlights, 'ARR', dateStr);

        // Apply formatting
        await formatTab(tabs.depId, depFlights, 'DEP', dateStr);
        await formatTab(tabs.arrId, arrFlights, 'ARR', dateStr);

        console.log(`[Sheets] ✅ Synced ${dateStr}: ${(depFlights||[]).length} dep, ${(arrFlights||[]).length} arr`);
    } catch (e) { console.log('[Sheets] write error:', e.message); }
}

// ── Read back flight data (for history API) ───────────────────────────────
function rowsToFlights(rows) {
    // columns: STD, ETD, FLIGHT, AIRLINE, LOCATION, TERMINAL, GATE, CHECKIN, STATUS, REMARK
    return (rows || []).filter(r => r && r[2]).map(r => ({
        std:      r[0]||'', etd:      r[1]||'', flightNo: r[2]||'',
        airline:  r[3]||'', location: r[4]||'', terminal: r[5]||'',
        gate:     r[6]||'', checkin:  r[7]||'', status:   r[8]||'', remark: r[9]||''
    }));
}

async function readFlightDataFromSheet(dateStr) {
    if (!sheetsApi || !SPREADSHEET_ID) return null;
    try {
        const depTitle = `DEP ${dateStr}`;
        const arrTitle = `ARR ${dateStr}`;
        const [depRes, arrRes] = await Promise.all([
            sheetsApi.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: `'${depTitle}'!A4:J5000`
            }).catch(() => ({ data: { values: [] } })),
            sheetsApi.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: `'${arrTitle}'!A4:J5000`
            }).catch(() => ({ data: { values: [] } }))
        ]);
        const deps = rowsToFlights(depRes.data.values || []);
        const arrs = rowsToFlights(arrRes.data.values || []);
        if (!deps.length && !arrs.length) return null;
        return { departures: deps, arrivals: arrs };
    } catch (e) { return null; }
}

async function listSheetDates() {
    if (!sheetsApi || !SPREADSHEET_ID) return [];
    try {
        const meta = await sheetsApi.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
        const dates = new Set();
        meta.data.sheets.forEach(s => {
            const m = s.properties.title.match(/^(?:DEP|ARR) (\d{4}-\d{2}-\d{2})$/);
            if (m) dates.add(m[1]);
        });
        return [...dates].sort().reverse();
    } catch (e) { return []; }
}

// ===== LOCAL HISTORY =====
function saveToHistory(dateStr, data) {
    const filePath = path.join(historyDir, `${dateStr}.json`);
    try {
        fs.writeFileSync(filePath, JSON.stringify(data));
        console.log(`[History] Saved ${dateStr} (${(data.departures||[]).length} dep, ${(data.arrivals||[]).length} arr)`);
    } catch (e) { console.log('[History] Save error:', e.message); }
}

function loadFromHistory(dateStr) {
    const filePath = path.join(historyDir, `${dateStr}.json`);
    if (!fs.existsSync(filePath)) return null;
    try { return JSON.parse(fs.readFileSync(filePath, 'utf8')); } catch (e) { return null; }
}

function listLocalHistoryDates() {
    try {
        return fs.readdirSync(historyDir)
            .filter(f => /^\d{4}-\d{2}-\d{2}\.json$/.test(f))
            .map(f => f.replace('.json', ''))
            .sort().reverse();
    } catch (e) { return []; }
}

// ===== LOAD CACHED DATA FROM DISK =====
if (fs.existsSync(DATA_FILE)) {
    try {
        const parsed = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
        if (parsed.data && parsed.data.today) {
            cachedData = parsed.data;
            lastUpdate = new Date(parsed.lastUpdate);
            if (parsed.date) currentJSTDate = parsed.date;
        }
    } catch (e) {}
}

// ===== API ROUTES =====
app.get('/api/flights', (req, res) => res.json({ lastUpdate, data: cachedData }));

app.get('/api/history/dates', async (req, res) => {
    try {
        const local  = listLocalHistoryDates();
        const sheets = await listSheetDates();
        const merged = [...new Set([...local, ...sheets])].sort().reverse();
        res.json({ dates: merged });
    } catch (e) { res.json({ dates: [] }); }
});

app.get('/api/history/:date', async (req, res) => {
    const dateStr = req.params.date;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr))
        return res.status(400).json({ error: 'Invalid date format' });

    // Serve from live cache if within rolling window
    if (dateStr === getJSTDateStr(-1)) return res.json(cachedData.yesterday);
    if (dateStr === getJSTDateStr(0))  return res.json(cachedData.today);
    if (dateStr === getJSTDateStr(1))  return res.json(cachedData.tomorrow);

    // Local history file
    const local = loadFromHistory(dateStr);
    if (local) return res.json(local);

    // Google Sheets fallback
    const sheets = await readFlightDataFromSheet(dateStr);
    if (sheets) return res.json(sheets);

    res.status(404).json({ error: 'No data found for this date' });
});

app.get('/api/weather', async (req, res) => {
    try {
        const icao = (req.query.icao || 'RJAA').toUpperCase();
        const response = await fetch(
            `https://aviationweather.gov/api/data/metar?ids=${icao}&format=json`,
            { headers: { 'User-Agent': 'Mozilla/5.0' } }
        );
        res.json(await response.json());
    } catch (e) { res.status(500).json({ error: 'Weather Error' }); }
});

app.get('/api/aircraft', async (req, res) => {
    try {
        const { flightNo, date, type } = req.query;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);
        const response = await fetch(
            `https://api.flightradar24.com/common/v1/flight/list.json?query=${flightNo}&fetchBy=flight&page=1&limit=10`,
            { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }, signal: controller.signal }
        );
        clearTimeout(timeout);
        if (!response.ok) return res.json({ aircraft: 'Radar Blocked', registration: 'N/A' });

        const data = await response.json();
        let result = {
            aircraft: 'Unknown', registration: 'N/A',
            actualDep: null, actualArr: null, estArr: null, estDep: null,
            schedArr: null, schedDep: null,
            orgnTz: 'Asia/Tokyo', destTz: 'Asia/Tokyo', divertedTo: null,
            destIcao: null, originIcao: null, destIata: null, originIata: null
        };

        if (data?.result?.response?.data) {
            const flights = data.result.response.data;
            const toJSTDate = (unix) =>
                new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Tokyo' }).format(new Date(unix * 1000));

            // ── Multi-segment fix (UA838-style: KHH→NRT→SFO same day) ──────────
            // Always pick the segment where NRT is on the CORRECT side.
            const isNRT = (iata, icao) => iata === 'NRT' || icao === 'RJAA';
            const nrtMatch = (f) => type === 'arr'
                ? isNRT(f.airport?.destination?.code?.iata, f.airport?.destination?.code?.icao)
                : isNRT(f.airport?.origin?.code?.iata,      f.airport?.origin?.code?.icao);

            // ── Match quality levels ──────────────────────────────────────────────
            // 1 = exact JST date + correct NRT side  → full data (actuals + sched)
            // 2 = ±1 JST day   + correct NRT side    → scheduled only (overnight)
            // 3 = exact JST date, wrong NRT side     → aircraft/reg only
            // 4 = ±1 JST day,   wrong NRT side       → aircraft/reg only (last resort)
            let exactFlight = null;
            let matchQuality = 0;

            if (date) {
                const base  = new Date(date + 'T00:00:00Z');
                const prev  = new Date(base); prev.setUTCDate(base.getUTCDate() - 1);
                const next  = new Date(base); next.setUTCDate(base.getUTCDate() + 1);
                const candidateDates = new Set([date, prev.toISOString().slice(0,10), next.toISOString().slice(0,10)]);

                const getUnix = (f) => type === 'arr'
                    ? (f.time.scheduled.arrival   || f.time.scheduled.departure)
                    : (f.time.scheduled.departure  || f.time.scheduled.arrival);

                // Pass 1: exact JST date + NRT on correct side
                exactFlight = flights.find(f => {
                    const u = getUnix(f); return u && nrtMatch(f) && toJSTDate(u) === date;
                });
                if (exactFlight) matchQuality = 1;

                // Pass 2: ±1 day + NRT on correct side (overnight arrivals, e.g. EK DXB→NRT)
                if (!exactFlight) {
                    exactFlight = flights.find(f => {
                        const u = getUnix(f); return u && nrtMatch(f) && candidateDates.has(toJSTDate(u));
                    });
                    if (exactFlight) matchQuality = 2;
                }

                // Pass 3: exact JST date, ignore NRT side (domestic / codeshare fallback)
                if (!exactFlight) {
                    exactFlight = flights.find(f => {
                        const u = getUnix(f); return u && toJSTDate(u) === date;
                    });
                    if (exactFlight) matchQuality = 3;
                }

                // Pass 4: ±1 day, ignore NRT side (last resort — aircraft info only)
                if (!exactFlight) {
                    exactFlight = flights.find(f => {
                        const u = getUnix(f); return u && candidateDates.has(toJSTDate(u));
                    });
                    if (exactFlight) matchQuality = 4;
                }
            }

            if (exactFlight) {
                result.aircraft     = exactFlight.aircraft?.model?.code || exactFlight.aircraft?.model?.text || 'TBD';
                result.registration = exactFlight.aircraft?.registration || 'N/A';

                // Always provide airport/timezone context
                if (exactFlight.airport?.origin?.timezone?.name)      result.orgnTz    = exactFlight.airport.origin.timezone.name;
                if (exactFlight.airport?.destination?.timezone?.name) result.destTz    = exactFlight.airport.destination.timezone.name;
                result.destIcao   = exactFlight.airport?.destination?.code?.icao || null;
                result.destIata   = exactFlight.airport?.destination?.code?.iata || null;
                result.originIcao = exactFlight.airport?.origin?.code?.icao      || null;
                result.originIata = exactFlight.airport?.origin?.code?.iata      || null;

                if (matchQuality === 1) {
                    // ✅ Perfect match — include all actual, estimated, and scheduled times
                    result.actualDep = exactFlight.time?.real?.departure      || null;
                    result.actualArr = exactFlight.time?.real?.arrival        || null;
                    result.estArr    = exactFlight.time?.estimated?.arrival   || null;
                    result.estDep    = exactFlight.time?.estimated?.departure || null;
                    result.schedArr  = exactFlight.time?.scheduled?.arrival   || null;
                    result.schedDep  = exactFlight.time?.scheduled?.departure || null;
                    const statusText = exactFlight.status?.text || '';
                    if (statusText.toLowerCase().includes('divert')) {
                        const m = statusText.match(/to\s+([A-Z]{3})/i);
                        if (m) result.divertedTo = m[1].toUpperCase();
                    }
                } else if (matchQuality === 2) {
                    // ⚠️ Date shifted (overnight long-haul) — scheduled times safe to use.
                    // Only include live/actual data if estimated arrival is on the REQUESTED date
                    // (i.e. flight is genuinely in progress for today's schedule).
                    // NEVER copy raw actualArr from a different-date match — this is the root
                    // cause of "today's flight shows DEST LANDED before it even departs".
                    result.schedArr = exactFlight.time?.scheduled?.arrival   || null;
                    result.schedDep = exactFlight.time?.scheduled?.departure || null;
                    const estArrUnix = exactFlight.time?.estimated?.arrival;
                    const actDepUnix = exactFlight.time?.real?.departure;
                    if (estArrUnix && toJSTDate(estArrUnix) === date) {
                        result.estArr    = estArrUnix;
                        result.estDep    = exactFlight.time?.estimated?.departure || null;
                        result.actualDep = actDepUnix || null;
                    }
                }
                // matchQuality 3 / 4 → aircraft info only.
                // Wrong NRT side (e.g. KHH→NRT used for NRT→SFO lookup): returning actual times
                // would falsely mark the flight as departed/landed at the wrong airport.
            }
        }
        res.json(result);
    } catch (e) { res.status(500).json({ aircraft: 'Error', registration: 'N/A' }); }
});

// ===== SCRAPING ENGINE =====
async function smartScraper() {
    if (isScraping) return;
    const nowJSTDate = getJSTDateStr(0);

    // ── Day rollover ────────────────────────────────────────────────────────────
    if (nowJSTDate !== currentJSTDate) {
        // Archive the day that just ended (was "today", now becomes history)
        if ((cachedData.today?.departures || []).length > 0) {
            saveToHistory(currentJSTDate, cachedData.today);
            writeFlightDataToSheet(
                currentJSTDate,
                cachedData.today.departures || [],
                cachedData.today.arrivals   || []
            );
            console.log(`[Rollover] Archived ${currentJSTDate} → history`);
        }
        cachedData.yesterday = cachedData.today;
        cachedData.today     = cachedData.tomorrow;
        cachedData.tomorrow  = { departures: [], arrivals: [] };
        currentJSTDate = nowJSTDate;
        lastScrapedTime = { yesterday: 0, today: 0, tomorrow: 0 };
    }

    // ── Build scrape list ────────────────────────────────────────────────────────
    const now = Date.now();
    const daysToScrape = [];

    // On first startup: scrape yesterday if data is missing
    if (!startupScanDone) {
        startupScanDone = true;
        const yestStr = getJSTDateStr(-1);
        const localHist = loadFromHistory(yestStr);
        if (!localHist && (cachedData.yesterday?.departures || []).length === 0) {
            console.log('[Scraper] Startup: queuing yesterday scan for', yestStr);
            daysToScrape.push({ key: 'yesterday', offset: -1 });
        } else if (localHist && (cachedData.yesterday?.departures || []).length === 0) {
            // Load from local history file instead of re-scraping
            cachedData.yesterday = localHist;
            console.log('[History] Loaded yesterday from local file:', yestStr);
        }
    }

    if (now - lastScrapedTime.today   >= SCHEDULE.today)   daysToScrape.push({ key: 'today',    offset: 0 });
    if (now - lastScrapedTime.tomorrow >= SCHEDULE.tomorrow) daysToScrape.push({ key: 'tomorrow', offset: 1 });
    if (daysToScrape.length === 0) return;

    isScraping = true;
    try {
        // ── Scrape a single NRT page ──────────────────────────────────────────
        const scrapePage = async (url) => {
            let browser;
            try {
                browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
                const page = await browser.newPage();
                await page.setViewport({ width: 1920, height: 1080 });
                await page.setRequestInterception(true);
                page.on('request', req => {
                    if (['media', 'font', 'image'].includes(req.resourceType())) req.abort();
                    else req.continue();
                });
                await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
                await new Promise(r => setTimeout(r, 4000));
                try { await page.waitForSelector('a[class*="part-module"]', { timeout: 20000 }); }
                catch (e) { return []; }
                await page.evaluate(async () => {
                    await new Promise(resolve => {
                        let lastH = 0, retries = 0;
                        const t = setInterval(() => {
                            window.scrollBy(0, 800);
                            const h = document.body.scrollHeight;
                            if (h === lastH) { if (++retries >= 3) { clearInterval(t); resolve(); } }
                            else { retries = 0; lastH = h; }
                        }, 800);
                    });
                });
                return await page.evaluate(() => {
                    const data = [];
                    const rows = document.querySelectorAll('a[class*="part-module"]');
                    const extractTime = el => { const m = el ? el.innerText.match(/\d{2}:\d{2}/) : null; return m ? m[0] : '--:--'; };
                    rows.forEach(row => {
                        let std = extractTime(row.querySelector('[class*="prev"]'));
                        let etd = extractTime(row.querySelector('[class*="current"]'));
                        if (std === '--:--' && etd === '--:--') std = extractTime(row.querySelector('[class*="schedule"]'));
                        if (std === '--:--') std = extractTime(row);
                        if (std === '--:--') return;

                        let location = row.querySelector('[class*="location"]')?.innerText.replace(/Departure|Destination/g,'').trim() || 'UNKNOWN';
                        const airline = row.querySelector('[class*="airline"]')?.innerText.trim() || '';
                        let flightNumRaw = row.querySelector('[class*="flight__"] [class*="text"]')?.innerText.replace(/\s+/g,'') || '';
                        let cleanFlightNo = flightNumRaw;
                        const mFlight = cleanFlightNo.match(/^([A-Z]{2,3}|[A-Z0-9]{2})0*([1-9]\d*)$/i);
                        if (mFlight) cleanFlightNo = mFlight[1] + mFlight[2];

                        let logoUrl = '';
                        const iataM = cleanFlightNo.match(/^([A-Z0-9]{2})/i);
                        if (iataM) {
                            const iata = iataM[1].toUpperCase();
                            if (iata === 'NQ' || airline.toUpperCase().includes('AIR JAPAN'))
                                logoUrl = 'https://images.kiwi.com/airlines/128/NQ.png';
                            else logoUrl = `https://pics.avs.io/60/60/${iata}.png`;
                        }

                        let terminal = '-', floor = '-';
                        const termEl = row.querySelector('[class*="terminal"]');
                        if (termEl) {
                            const tt = termEl.innerText.trim();
                            if (tt.includes('/')) { const pts = tt.split('/'); terminal = pts[0].replace(/Terminal/i,'').trim(); floor = pts[1].trim(); }
                            else terminal = tt.replace(/Terminal/i,'').trim();
                        }

                        let gate = '-', checkin = '';
                        row.querySelectorAll('[class*="default-module"]').forEach(fac => {
                            const tmp = document.createElement('div'); tmp.innerHTML = fac.innerHTML;
                            tmp.querySelectorAll('s,del,strike,span[class*="line-through"]').forEach(el => el.remove());
                            const ct = tmp.innerText;
                            if (ct.match(/^(Gate|ゲート)/i)) {
                                const pts = ct.split(/→|->|=>|変更|change/i);
                                const lp  = pts[pts.length-1].replace(/gate|ゲート|[:：]/ig,'').trim();
                                const mn  = lp.match(/([0-9]+[A-Za-z]?|[A-Za-z]{1,2})/g);
                                if (mn) gate = mn[mn.length-1].toUpperCase();
                            } else if (ct.match(/^(Check-in|チェックイン)/i)) {
                                const pts = ct.split(/→|->|=>|変更|change/i);
                                const lp  = pts[pts.length-1].replace(/check-in|チェックイン|[:：]/ig,'').trim();
                                const mn  = lp.match(/[A-Za-z0-9]+/g);
                                if (mn) checkin = mn[mn.length-1].toUpperCase();
                            }
                        });

                        let remark = '', status = 'ON TIME';
                        const fullText = row.innerText || '';
                        row.querySelectorAll('[class*="-has-border__"]').forEach(b => {
                            const bt = b.innerText.trim().toUpperCase();
                            if (bt.includes('GATE CHANGE')) remark = 'GATE CHANGED';
                            else if (bt.includes('CHECK-IN')) remark = 'CHECK-IN CHANGED';
                            else status = bt;
                        });
                        if (!remark && fullText.toUpperCase().includes('GATE CHANGE'))    remark = 'GATE CHANGED';
                        if (!remark && fullText.toUpperCase().includes('CHECK-IN CHANGE')) remark = 'CHECK-IN CHANGED';
                        if (fullText.toUpperCase().includes('CANCELED')) status = 'CANCELED';

                        data.push({ std, etd, location, airline, flightNo: cleanFlightNo, gate, remark, status, logoUrl, terminal, floor, checkin });
                    });
                    return data;
                });
            } catch (e) { return []; }
            finally { if (browser) await browser.close(); }
        };

        // ── Delay logic ────────────────────────────────────────────────────────
        const processLogic = (newFlights, oldFlights) => {
            newFlights.forEach(f => {
                if (f.status !== 'CANCELED') {
                    const [sh, sm] = f.std.split(':').map(Number);
                    let   [eh, em] = f.etd.replace(/<[^>]*>?/gm,'').replace(/[^0-9:]/g,'').split(':').map(Number);
                    if (sh !== undefined && eh !== undefined) {
                        let sMins = sh*60+sm, eMins = eh*60+em;
                        if (eMins < sMins && (sMins-eMins) > 720) eMins += 1440;
                        if (eMins - sMins >= 15) {
                            const alreadyDelayed = f.status === 'DELAYED' || f.status.startsWith('DELAYED|');
                            const noFlipNeeded   = f.status === 'ON TIME' || f.status === '' || alreadyDelayed;
                            if (noFlipNeeded) {
                                f.status = 'DELAYED'; // plain DELAYED, no flip
                            } else {
                                f.status = `DELAYED|${f.status}`; // flip: DELAYED → current status
                            }
                        }
                    }
                }
                const old = oldFlights.find(o => o.flightNo === f.flightNo && o.std === f.std);
                if (old) {
                    if (old.remark && !f.remark) f.remark = old.remark;
                    if (old.gate !== '-' && f.gate !== '-' && old.gate !== f.gate) f.remark = 'GATE CHANGED';
                }
            });
            return newFlights;
        };

        // ── Scrape each day ────────────────────────────────────────────────────
        for (const day of daysToScrape) {
            try {
                const dateStr = getJSTDateStr(day.offset);
                const [depInt, depDom, arrInt, arrDom] = await Promise.all([
                    scrapePage(`https://www.narita-airport.jp/en/flight/dep-search/?searchDepArr=dep-search&time=all&domInter=I&date=${dateStr}`),
                    scrapePage(`https://www.narita-airport.jp/en/flight/dep-search/?searchDepArr=dep-search&time=all&domInter=D&date=${dateStr}`),
                    scrapePage(`https://www.narita-airport.jp/en/flight/arr-search/?searchDepArr=arr-search&time=all&domInter=I&date=${dateStr}`),
                    scrapePage(`https://www.narita-airport.jp/en/flight/arr-search/?searchDepArr=arr-search&time=all&domInter=D&date=${dateStr}`)
                ]);
                depInt.forEach(f => f.type = 'I'); depDom.forEach(f => f.type = 'D');
                arrInt.forEach(f => f.type = 'I'); arrDom.forEach(f => f.type = 'D');

                const rawDep = [...depInt, ...depDom].sort((a,b) => a.std.localeCompare(b.std));
                const rawArr = [...arrInt, ...arrDom].sort((a,b) => a.std.localeCompare(b.std));

                cachedData[day.key] = {
                    departures: processLogic(rawDep, cachedData[day.key]?.departures || []),
                    arrivals:   processLogic(rawArr, cachedData[day.key]?.arrivals   || [])
                };
                lastScrapedTime[day.key] = Date.now();
                lastUpdate = new Date();
                fs.writeFileSync(DATA_FILE, JSON.stringify({ lastUpdate, date: currentJSTDate, data: cachedData }));
                console.log(`[Scraper] ✅ ${day.key} (${dateStr}): ${rawDep.length} dep, ${rawArr.length} arr`);

                // ── Always write to Sheets after every successful scrape ──────────
                // yesterday: also save to local history
                if (day.key === 'yesterday' && rawDep.length > 0) {
                    saveToHistory(dateStr, cachedData.yesterday);
                    writeFlightDataToSheet(dateStr, cachedData.yesterday.departures, cachedData.yesterday.arrivals);
                }
                // today: write every scrape cycle (every ~5 min) — live status + remarks
                if (day.key === 'today' && rawDep.length > 0) {
                    writeFlightDataToSheet(dateStr, cachedData.today.departures, cachedData.today.arrivals);
                }
                // tomorrow: write every scrape cycle (every ~1 hr) — advance planning data
                if (day.key === 'tomorrow' && rawDep.length > 0) {
                    writeFlightDataToSheet(dateStr, cachedData.tomorrow.departures, cachedData.tomorrow.arrivals);
                }
            } catch (e) { console.log(`[Scraper] Error on ${day.key}:`, e.message); }
        }
    } catch (e) { console.log('[Scraper] Fatal:', e.message); }
    finally { isScraping = false; }
}

// ===== STARTUP =====
initGoogleSheets().then(() => {
    smartScraper();
    setInterval(smartScraper, 60_000);
});

const PORT = process.env.PORT || 80;
app.listen(PORT, () => console.log(`🛫 Narita Flight Board PRO — PORT ${PORT}`));