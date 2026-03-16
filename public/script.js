// ===== STATE =====
let currentTab = 'dep';
let currentDateTab = 'today';
let flightDataCache = { yesterday: {}, today: {}, tomorrow: {} };
let currentDisplayedFlights = [];
let searchTimeout;
let radarCache = {};
try { radarCache = JSON.parse(localStorage.getItem('radarCache')) || {}; } catch(e) {}

let remarkCache = {};
try { remarkCache = JSON.parse(localStorage.getItem('remarkCache')) || {}; } catch(e) {}

// ── Cache version: bump this whenever the FR24 matching logic changes ──────
// v4: removed remarkCache-based destLanded from renderBoard (live-data only)
const RADAR_CACHE_VERSION = 'v4';
(function initCacheVersion() {
    try {
        if (localStorage.getItem('radarCacheVersion') !== RADAR_CACHE_VERSION) {
            localStorage.removeItem('radarCache');
            localStorage.removeItem('remarkCache');
            localStorage.setItem('radarCacheVersion', RADAR_CACHE_VERSION);
            radarCache  = {};
            remarkCache = {};
            console.log('[Cache] Cleared stale cache — upgraded to', RADAR_CACHE_VERSION);
        }
    } catch(e) {}
})();

// ── Helper: JST date string (matches server-side getJSTDateStr) ────────────
function getJSTDateStr(offsetDays = 0) {
    const d = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo' }));
    d.setDate(d.getDate() + offsetDays);
    return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

// ── Purge radar/remark cache entries older than yesterday or newer than tomorrow ──
function cleanStaleRadarCache() {
    const validDates = new Set([getJSTDateStr(-1), getJSTDateStr(0), getJSTDateStr(1)]);
    let changed = false;
    [radarCache, remarkCache].forEach(cache => {
        Object.keys(cache).forEach(key => {
            const m = key.match(/_(\d{4}-\d{2}-\d{2})$/);
            if (m && !validDates.has(m[1])) { delete cache[key]; changed = true; }
        });
    });
    if (changed) {
        try { localStorage.setItem('radarCache',  JSON.stringify(radarCache));  } catch(e) {}
        try { localStorage.setItem('remarkCache', JSON.stringify(remarkCache)); } catch(e) {}
        console.log('[Cache] Cleaned stale date entries');
    }
}

// History state
let historyDateStr = null;        // currently viewing a history date (YYYY-MM-DD)
let historyDataCache = {};        // { 'YYYY-MM-DD': { departures, arrivals } }
let availableHistoryDates = [];

const translations = {
    en: { destLanded:'DEST LANDED', backBtn:'Back', divertedTo:'DIVERTED TO', h:'h', m:'m', ago:'ago', inTime:'in' },
    vi: { destLanded:'ĐÃ HẠ CÁNH', backBtn:'Quay lại', divertedTo:'CHUYỂN HƯỚNG ĐẾN', h:'h', m:'p', ago:'trước', inTime:'sau' },
    ja: { destLanded:'到着済み', backBtn:'戻る', divertedTo:'目的地変更:', h:'時間', m:'分', ago:'前', inTime:'後' }
};
let currentLang = 'en';

let currentAirlineName = '';
let currentAirlineLogo = '';
let currentAirlineTab  = 'dep';
let currentAirlineFlights = [];
// shouldScrollToNow is set ONLY on page load (DOMContentLoaded) and when "Today" is clicked
let shouldScrollToNow = false;

let batchFetchQueue   = [];
let batchFetchRunning = false;
let batchScheduled    = false;
let batchRenderNeeded = false;

let airlineFilterValues = new Set(); // empty = All Airlines (multi-select)
// Legacy alias kept for any remaining single-value references
Object.defineProperty(window, 'airlineFilterValue', {
    get: () => airlineFilterValues.size === 0 ? 'ALL' : [...airlineFilterValues][0],
    set: (v) => { airlineFilterValues = v === 'ALL' ? new Set() : new Set([v]); }
});

// ===== EXPANDED AIRLINE DATABASE =====
const AIRLINE_DB = {
    // ── Japan ──
    'NH': { name:'All Nippon Airways',       iata:'NH', icao:'ANA', founded:1952, alliance:'Star Alliance', hubs:'NRT, HND, ITM',       fleet:'~214' },
    'JL': { name:'Japan Airlines',           iata:'JL', icao:'JAL', founded:1951, alliance:'oneworld',      hubs:'NRT, HND, ITM',       fleet:'~163' },
    'MM': { name:'Peach Aviation',           iata:'MM', icao:'APJ', founded:2011, alliance:'None',          hubs:'KIX, NRT',            fleet:'~40'  },
    'GK': { name:'Jetstar Japan',            iata:'GK', icao:'JJP', founded:2011, alliance:'None',          hubs:'NRT, KIX',            fleet:'~25'  },
    'NQ': { name:'Air Japan',                iata:'NQ', icao:'NJO', founded:2023, alliance:'None',          hubs:'NRT',                 fleet:'~3'   },
    'IJ': { name:'Spring Japan',             iata:'IJ', icao:'SJO', founded:2012, alliance:'None',          hubs:'NRT',                 fleet:'~8'   },
    // ── Southeast Asia extras ──
    'XJ': { name:'Thai AirAsia X',           iata:'XJ', icao:'TAX', founded:2014, alliance:'None',          hubs:'BKK (Suvarnabhumi)',   fleet:'~5'   },
    'VZ': { name:'Thai VietJet Air',         iata:'VZ', icao:'TVJ', founded:2015, alliance:'None',          hubs:'BKK (Suvarnabhumi)',   fleet:'~15'  },
    'ZG': { name:'ZIPAIR Tokyo',              iata:'ZG', icao:'TZP', founded:2020, alliance:'None',          hubs:'NRT',                 fleet:'~11'  },
    // ── USA ──
    'UA': { name:'United Airlines',          iata:'UA', icao:'UAL', founded:1926, alliance:'Star Alliance', hubs:'EWR, ORD, IAD, SFO',  fleet:'~960' },
    'AA': { name:'American Airlines',        iata:'AA', icao:'AAL', founded:1934, alliance:'oneworld',      hubs:'DFW, JFK, MIA, CLT',  fleet:'~960' },
    'DL': { name:'Delta Air Lines',          iata:'DL', icao:'DAL', founded:1924, alliance:'SkyTeam',       hubs:'ATL, JFK, LAX, DTW',  fleet:'~1000'},
    'AS': { name:'Alaska Airlines',          iata:'AS', icao:'ASA', founded:1932, alliance:'oneworld',      hubs:'SEA, PDX, SFO',       fleet:'~320' },
    'HA': { name:'Hawaiian Airlines',        iata:'HA', icao:'HAL', founded:1929, alliance:'None',          hubs:'HNL',                 fleet:'~60'  },
    'B6': { name:'JetBlue Airways',          iata:'B6', icao:'JBU', founded:2000, alliance:'None',          hubs:'JFK, BOS, FLL',       fleet:'~290' },
    // ── Canada ──
    'AC': { name:'Air Canada',               iata:'AC', icao:'ACA', founded:1937, alliance:'Star Alliance', hubs:'YYZ, YVR, YUL',       fleet:'~190' },
    'WS': { name:'WestJet',                  iata:'WS', icao:'WJA', founded:1996, alliance:'None',          hubs:'YYC, YVR, YYZ',       fleet:'~180' },
    // ── Europe ──
    'LH': { name:'Lufthansa',                iata:'LH', icao:'DLH', founded:1953, alliance:'Star Alliance', hubs:'FRA, MUC',            fleet:'~300' },
    'AF': { name:'Air France',               iata:'AF', icao:'AFR', founded:1933, alliance:'SkyTeam',       hubs:'CDG, ORY',            fleet:'~220' },
    'KL': { name:'KLM Royal Dutch Airlines', iata:'KL', icao:'KLM', founded:1919, alliance:'SkyTeam',       hubs:'AMS',                 fleet:'~115' },
    'BA': { name:'British Airways',          iata:'BA', icao:'BAW', founded:1974, alliance:'oneworld',      hubs:'LHR, LGW',            fleet:'~280' },
    'AY': { name:'Finnair',                  iata:'AY', icao:'FIN', founded:1923, alliance:'oneworld',      hubs:'HEL',                 fleet:'~80'  },
    'IB': { name:'Iberia',                   iata:'IB', icao:'IBE', founded:1927, alliance:'oneworld',      hubs:'MAD',                 fleet:'~130' },
    'LX': { name:'Swiss International',      iata:'LX', icao:'SWR', founded:2002, alliance:'Star Alliance', hubs:'ZRH, GVA',            fleet:'~95'  },
    'OS': { name:'Austrian Airlines',        iata:'OS', icao:'AUA', founded:1957, alliance:'Star Alliance', hubs:'VIE',                 fleet:'~80'  },
    'SK': { name:'Scandinavian Airlines',    iata:'SK', icao:'SAS', founded:1946, alliance:'Star Alliance', hubs:'CPH, ARN, OSL',       fleet:'~140' },
    'AZ': { name:'ITA Airways',              iata:'AZ', icao:'ITY', founded:2021, alliance:'SkyTeam',       hubs:'FCO, MXP',            fleet:'~95'  },
    'TP': { name:'TAP Air Portugal',         iata:'TP', icao:'TAP', founded:1945, alliance:'Star Alliance', hubs:'LIS',                 fleet:'~100' },
    'TK': { name:'Turkish Airlines',         iata:'TK', icao:'THY', founded:1933, alliance:'Star Alliance', hubs:'IST',                 fleet:'~420' },
    'SN': { name:'Brussels Airlines',        iata:'SN', icao:'BEL', founded:2002, alliance:'Star Alliance', hubs:'BRU',                 fleet:'~50'  },
    'LO': { name:'LOT Polish Airlines',      iata:'LO', icao:'LOT', founded:1929, alliance:'Star Alliance', hubs:'WAW',                 fleet:'~80'  },
    // ── Middle East ──
    'EK': { name:'Emirates',                 iata:'EK', icao:'UAE', founded:1985, alliance:'None',          hubs:'DXB',                 fleet:'~280' },
    'QR': { name:'Qatar Airways',            iata:'QR', icao:'QTR', founded:1997, alliance:'oneworld',      hubs:'DOH',                 fleet:'~240' },
    'EY': { name:'Etihad Airways',           iata:'EY', icao:'ETD', founded:2003, alliance:'None',          hubs:'AUH',                 fleet:'~100' },
    'GF': { name:'Gulf Air',                 iata:'GF', icao:'GFA', founded:1950, alliance:'None',          hubs:'BAH',                 fleet:'~35'  },
    'WY': { name:'Oman Air',                 iata:'WY', icao:'OMA', founded:1993, alliance:'None',          hubs:'MCT',                 fleet:'~50'  },
    'SV': { name:'Saudia',                   iata:'SV', icao:'SVA', founded:1945, alliance:'SkyTeam',       hubs:'JED, RUH, DMM',       fleet:'~200' },
    'RJ': { name:'Royal Jordanian',          iata:'RJ', icao:'RJA', founded:1963, alliance:'oneworld',      hubs:'AMM',                 fleet:'~25'  },
    'ME': { name:'Middle East Airlines',     iata:'ME', icao:'MEA', founded:1945, alliance:'SkyTeam',       hubs:'BEY',                 fleet:'~15'  },
    // ── Asia-Pacific ──
    'SQ': { name:'Singapore Airlines',       iata:'SQ', icao:'SIA', founded:1947, alliance:'Star Alliance', hubs:'SIN',                 fleet:'~130' },
    'CX': { name:'Cathay Pacific',           iata:'CX', icao:'CPA', founded:1946, alliance:'oneworld',      hubs:'HKG',                 fleet:'~220' },
    'KE': { name:'Korean Air',               iata:'KE', icao:'KAL', founded:1969, alliance:'SkyTeam',       hubs:'ICN, GMP',            fleet:'~165' },
    'OZ': { name:'Asiana Airlines',          iata:'OZ', icao:'AAR', founded:1988, alliance:'Star Alliance', hubs:'ICN',                 fleet:'~85'  },
    '7C': { name:'Jeju Air',                 iata:'7C', icao:'JJA', founded:2005, alliance:'None',          hubs:'ICN, CJU',            fleet:'~50'  },
    'LJ': { name:'Jin Air',                  iata:'LJ', icao:'JNA', founded:2008, alliance:'None',          hubs:'ICN, GMP',            fleet:'~30'  },
    'BX': { name:'Air Busan',                iata:'BX', icao:'ABL', founded:2007, alliance:'None',          hubs:'PUS, GMP',            fleet:'~25'  },
    'ZE': { name:'Eastar Jet',               iata:'ZE', icao:'ESR', founded:2007, alliance:'None',          hubs:'ICN',                 fleet:'~10'  },
    'RS': { name:'Air Seoul',                iata:'RS', icao:'ASV', founded:2015, alliance:'None',          hubs:'ICN',                 fleet:'~8'   },
    'TW': { name:'t\'way air',               iata:'TW', icao:'TWB', founded:2004, alliance:'None',          hubs:'ICN, GMP',            fleet:'~30'  },
    'RF': { name:'Aero K Airlines',          iata:'RF', icao:'EOK', founded:2017, alliance:'None',          hubs:'CJJ',                 fleet:'~8'   },
    'TG': { name:'Thai Airways',             iata:'TG', icao:'THA', founded:1960, alliance:'Star Alliance', hubs:'BKK (Suvarnabhumi)',  fleet:'~90'  },
    'DD': { name:'Nok Air',                  iata:'DD', icao:'NOK', founded:2004, alliance:'None',          hubs:'DMK',                 fleet:'~15'  },
    'PG': { name:'Bangkok Airways',          iata:'PG', icao:'BKP', founded:1968, alliance:'None',          hubs:'BKK, USM',            fleet:'~40'  },
    'FD': { name:'Thai AirAsia',             iata:'FD', icao:'AIQ', founded:2004, alliance:'None',          hubs:'DMK',                 fleet:'~60'  },
    'SL': { name:'Thai Lion Air',            iata:'SL', icao:'TLM', founded:2013, alliance:'None',          hubs:'DMK',                 fleet:'~40'  },
    'MH': { name:'Malaysia Airlines',        iata:'MH', icao:'MAS', founded:1947, alliance:'oneworld',      hubs:'KUL',                 fleet:'~85'  },
    'AK': { name:'AirAsia',                  iata:'AK', icao:'AXM', founded:1993, alliance:'None',          hubs:'KUL, KIX',            fleet:'~200' },
    'D7': { name:'AirAsia X',               iata:'D7', icao:'XAX', founded:2007, alliance:'None',          hubs:'KUL',                 fleet:'~20'  },
    'PR': { name:'Philippine Airlines',      iata:'PR', icao:'PAL', founded:1941, alliance:'None',          hubs:'MNL',                 fleet:'~80'  },
    'Z2': { name:'Philippines AirAsia',      iata:'Z2', icao:'EZD', founded:2010, alliance:'None',          hubs:'MNL',                 fleet:'~20'  },
    '5J': { name:'Cebu Pacific',             iata:'5J', icao:'CEB', founded:1996, alliance:'None',          hubs:'MNL, CEB',            fleet:'~80'  },
    'GA': { name:'Garuda Indonesia',         iata:'GA', icao:'GIA', founded:1949, alliance:'SkyTeam',       hubs:'CGK',                 fleet:'~80'  },
    'QZ': { name:'Indonesia AirAsia',        iata:'QZ', icao:'AWQ', founded:2004, alliance:'None',          hubs:'CGK',                 fleet:'~25'  },
    'VN': { name:'Vietnam Airlines',         iata:'VN', icao:'HVN', founded:1956, alliance:'SkyTeam',       hubs:'HAN, SGN',            fleet:'~100' },
    'VJ': { name:'VietJet Air',              iata:'VJ', icao:'VJC', founded:2011, alliance:'None',          hubs:'HAN, SGN, DAD',       fleet:'~90'  },
    'BL': { name:'Pacific Airlines',         iata:'BL', icao:'PIC', founded:1991, alliance:'None',          hubs:'SGN, HAN',            fleet:'~12'  },
    'TR': { name:'Scoot',                    iata:'TR', icao:'TGW', founded:2011, alliance:'None',          hubs:'SIN',                 fleet:'~50'  },
    '3K': { name:'Jetstar Asia',             iata:'3K', icao:'JSA', founded:2004, alliance:'None',          hubs:'SIN',                 fleet:'~18'  },
    'MI': { name:'SilkAir',                  iata:'MI', icao:'SLK', founded:1976, alliance:'None',          hubs:'SIN',                 fleet:'~30'  },
    'BI': { name:'Royal Brunei Airlines',    iata:'BI', icao:'RBA', founded:1974, alliance:'None',          hubs:'BWN',                 fleet:'~15'  },
    'AI': { name:'Air India',                iata:'AI', icao:'AIC', founded:1932, alliance:'Star Alliance', hubs:'DEL, BOM, BLR',       fleet:'~200' },
    'UK': { name:'Vistara',                  iata:'UK', icao:'VTI', founded:2015, alliance:'Star Alliance', hubs:'DEL, BOM',            fleet:'~55'  },
    // ── China ──
    'CA': { name:'Air China',                iata:'CA', icao:'CCA', founded:1988, alliance:'Star Alliance', hubs:'PEK, PVG, CTU',       fleet:'~430' },
    'CZ': { name:'China Southern Airlines',  iata:'CZ', icao:'CSN', founded:1988, alliance:'SkyTeam',       hubs:'CAN, PEK',            fleet:'~660' },
    'MU': { name:'China Eastern Airlines',   iata:'MU', icao:'CES', founded:1988, alliance:'SkyTeam',       hubs:'PVG, PEK',            fleet:'~600' },
    'ZH': { name:'Shenzhen Airlines',        iata:'ZH', icao:'CSZ', founded:1992, alliance:'Star Alliance', hubs:'SZX',                 fleet:'~200' },
    'HO': { name:'Juneyao Airlines',         iata:'HO', icao:'DKH', founded:2006, alliance:'None',          hubs:'PVG, SHA',            fleet:'~115' },
    'FM': { name:'Shanghai Airlines',        iata:'FM', icao:'CSH', founded:1985, alliance:'SkyTeam',       hubs:'PVG',                 fleet:'~100' },
    '9C': { name:'Spring Airlines',          iata:'9C', icao:'CQH', founded:2005, alliance:'None',          hubs:'PVG, SHE',            fleet:'~100' },
    '3U': { name:'Sichuan Airlines',         iata:'3U', icao:'CSC', founded:1986, alliance:'None',          hubs:'CTU, CKG, XIY',       fleet:'~160' },
    'GJ': { name:'Loong Air',               iata:'GJ', icao:'CDC', founded:2013, alliance:'None',          hubs:'HGH, WNZ',            fleet:'~60'  },
    'MF': { name:'Xiamen Airlines',          iata:'MF', icao:'CXA', founded:1984, alliance:'SkyTeam',       hubs:'XMN, CAN',            fleet:'~230' },
    // ── Macau / Mongolia ──
    'NX': { name:'Air Macau',                iata:'NX', icao:'AMU', founded:1994, alliance:'None',          hubs:'MFM',                 fleet:'~20'  },
    'OM': { name:'MIAT Mongolian Airlines',  iata:'OM', icao:'MGL', founded:1956, alliance:'None',          hubs:'ULN',                 fleet:'~10'  },
    // ── Malaysia / Batik ──
    'OD': { name:'Batik Air Malaysia',       iata:'OD', icao:'OAL', founded:2012, alliance:'None',          hubs:'KUL',                 fleet:'~40'  },
    // ── Taiwan ──
    'CI': { name:'China Airlines',           iata:'CI', icao:'CAL', founded:1959, alliance:'SkyTeam',       hubs:'TPE',                 fleet:'~90'  },
    'BR': { name:'EVA Air',                  iata:'BR', icao:'EVA', founded:1989, alliance:'Star Alliance', hubs:'TPE',                 fleet:'~90'  },
    'IT': { name:'Tigerair Taiwan',          iata:'IT', icao:'TTW', founded:2013, alliance:'None',          hubs:'TPE',                 fleet:'~20'  },
    'JX': { name:'Starlux Airlines',         iata:'JX', icao:'SJX', founded:2018, alliance:'None',          hubs:'TPE',                 fleet:'~25'  },
    // ── Hong Kong ──
    'HX': { name:'Hong Kong Airlines',       iata:'HX', icao:'CRK', founded:2006, alliance:'None',          hubs:'HKG',                 fleet:'~15'  },
    'UO': { name:'HK Express',               iata:'UO', icao:'HKE', founded:2004, alliance:'None',          hubs:'HKG',                 fleet:'~30'  },
    'HB': { name:'Greater Bay Airlines',     iata:'HB', icao:'GBA', founded:2021, alliance:'None',          hubs:'HKG',                 fleet:'~10'  },
    // ── Pacific / Oceania ──
    'QF': { name:'Qantas',                   iata:'QF', icao:'QFA', founded:1920, alliance:'oneworld',      hubs:'SYD, MEL, BNE, PER',  fleet:'~130' },
    'VA': { name:'Virgin Australia',         iata:'VA', icao:'VOZ', founded:2000, alliance:'None',          hubs:'SYD, MEL, BNE, PER',  fleet:'~90'  },
    'NZ': { name:'Air New Zealand',          iata:'NZ', icao:'ANZ', founded:1940, alliance:'Star Alliance', hubs:'AKL',                 fleet:'~110' },
    'FJ': { name:'Fiji Airways',             iata:'FJ', icao:'FAJ', founded:1951, alliance:'None',          hubs:'NAN',                 fleet:'~12'  },
    'TN': { name:'Air Tahiti Nui',           iata:'TN', icao:'THT', founded:1996, alliance:'None',          hubs:'PPT',                 fleet:'~5'   },
    // ── Thailand extras ──
    'WE': { name:'Thai Smile Airways',       iata:'WE', icao:'THD', founded:2012, alliance:'None',          hubs:'BKK (Suvarnabhumi)',   fleet:'~20'  },
    // ── Latin America ──
    'LA': { name:'LATAM Airlines',           iata:'LA', icao:'LAN', founded:1929, alliance:'oneworld',      hubs:'SCL, GRU, BOG',       fleet:'~330' },
    'AM': { name:'Aeromexico',               iata:'AM', icao:'AMX', founded:1934, alliance:'SkyTeam',       hubs:'MEX, MTY',            fleet:'~100' },
    'CM': { name:'Copa Airlines',            iata:'CM', icao:'CMP', founded:1947, alliance:'Star Alliance', hubs:'PTY',                 fleet:'~100' },
    // ── Africa / Others ──
    'ET': { name:'Ethiopian Airlines',       iata:'ET', icao:'ETH', founded:1945, alliance:'Star Alliance', hubs:'ADD',                 fleet:'~130' },
    'KQ': { name:'Kenya Airways',            iata:'KQ', icao:'KQA', founded:1977, alliance:'SkyTeam',       hubs:'NBO',                 fleet:'~35'  },
    'MS': { name:'EgyptAir',                 iata:'MS', icao:'MSR', founded:1932, alliance:'Star Alliance', hubs:'CAI',                 fleet:'~75'  },
    'AT': { name:'Royal Air Maroc',          iata:'AT', icao:'RAM', founded:1957, alliance:'oneworld',      hubs:'CMN',                 fleet:'~50'  },
    // ── Russia / CIS ──
    'SU': { name:'Aeroflot',                 iata:'SU', icao:'AFL', founded:1923, alliance:'SkyTeam',       hubs:'SVO',                 fleet:'~220' },
    'S7': { name:'S7 Airlines',              iata:'S7', icao:'SBI', founded:1992, alliance:'oneworld',      hubs:'OVB, DME',            fleet:'~100' },
    'KC': { name:'Air Astana',               iata:'KC', icao:'KZR', founded:2002, alliance:'None',          hubs:'ALA, NQZ',            fleet:'~40'  },
    'HY': { name:'Uzbekistan Airways',       iata:'HY', icao:'UZB', founded:1992, alliance:'None',          hubs:'TAS',                 fleet:'~35'  },
};

// Alliance logos — accurate inline SVGs matching the real official marks
const ALLIANCE_LOGOS = {
    // Star Alliance: official gold star on navy, "STAR ALLIANCE" wordmark
    'Star Alliance': `<svg viewBox="0 0 110 28" width="110" height="28" style="flex-shrink:0;vertical-align:middle;margin-right:0;">
        <!-- Star icon -->
        <polygon points="10,2 12.4,8.2 19,8.2 13.8,12 15.8,18.2 10,14.5 4.2,18.2 6.2,12 1,8.2 7.6,8.2"
            fill="#FFD700"/>
        <!-- Wordmark -->
        <text x="22" y="11" font-family="Arial,sans-serif" font-weight="700" font-size="7.5"
            fill="#FFD700" letter-spacing="0.8">STAR</text>
        <text x="22" y="21" font-family="Arial,sans-serif" font-weight="700" font-size="7.5"
            fill="#FFD700" letter-spacing="0.8">ALLIANCE</text>
    </svg>`,

    // oneworld: exact official logo — red circle arc + "oneworld" in bold dark text
    'oneworld': `<svg viewBox="0 0 100 28" width="100" height="28" style="flex-shrink:0;vertical-align:middle;margin-right:0;">
        <!-- Red arc left -->
        <path d="M9,14 A8,8 0 1,1 9.01,14" stroke="#CC0000" stroke-width="4" fill="none" stroke-dasharray="30 20"/>
        <!-- "one" in dark -->
        <text x="21" y="19" font-family="Arial Black,Arial,sans-serif" font-weight="900" font-size="13"
            fill="#fff" letter-spacing="-0.5">one</text>
        <!-- "world" in red -->
        <text x="52" y="19" font-family="Arial Black,Arial,sans-serif" font-weight="900" font-size="13"
            fill="#CC0000" letter-spacing="-0.5">world</text>
    </svg>`,

    // SkyTeam: official blue swoosh + "SkyTeam" text
    'SkyTeam': `<svg viewBox="0 0 95 28" width="95" height="28" style="flex-shrink:0;vertical-align:middle;margin-right:0;">
        <!-- Swoosh icon -->
        <path d="M6,20 Q4,8 12,6 Q18,4 16,14 Q14,20 8,22 Q5,23 6,20Z" fill="#0057A8"/>
        <path d="M12,6 Q22,2 22,12 Q22,20 14,22" fill="none" stroke="#0057A8" stroke-width="2.5"/>
        <!-- Wordmark -->
        <text x="26" y="19" font-family="Arial,sans-serif" font-weight="700" font-size="13"
            fill="#fff" letter-spacing="0.2">SkyTeam</text>
    </svg>`
};

function getAllianceBadge(alliance) {
    const styles = {
        'Star Alliance': 'background:#001F5B;border:1px solid #FFD700;',
        'oneworld':      'background:#1a0000;border:1px solid #CC0000;',
        'SkyTeam':       'background:#001F5B;border:1px solid #0057A8;',
        'None':          'background:#1a1a1a;border:1px solid #333;'
    };
    const style = styles[alliance] || 'background:#1a1a1a;border:1px solid #333;';
    const logo  = ALLIANCE_LOGOS[alliance];
    if (!logo) {
        return `<span class="am-alliance" style="${style}padding:4px 12px;border-radius:5px;font-size:0.9rem;color:#aaa;">${alliance}</span>`;
    }
    return `<span class="am-alliance" style="${style}display:inline-flex;align-items:center;padding:4px 10px 4px 8px;border-radius:5px;">${logo}</span>`;
}

// ───────────────────────────────────────────────────
function getAirlineIata(flightNo) {
    // Match 2-char IATA codes including alphanumeric (e.g. 7C, 3K, B6, 5J)
    const m = (flightNo || '').match(/^([A-Z0-9]{2})/i);
    return m ? m[1].toUpperCase() : null;
}

// ===== INIT =====
document.addEventListener('DOMContentLoaded', () => {
    // Clean up stale cache entries from dates outside the rolling 3-day window
    cleanStaleRadarCache();
    // Set scroll flag BEFORE first fetchFlights so we scroll on initial load only
    shouldScrollToNow = true;
    fetchFlights();
    fetchNaritaWeather();
    setInterval(fetchFlights, 30000);
    // Continuous live radar refresh every 4 minutes for active flights
    setInterval(runLiveRadarRefresh, 4 * 60 * 1000);
    setInterval(updateClock, 1000);

    document.getElementById('search-box').addEventListener('input', () => {
        clearTimeout(searchTimeout); searchTimeout = setTimeout(renderBoard, 300);
    });
    document.getElementById('type-filter').addEventListener('change', renderBoard);
    document.getElementById('terminal-filter').addEventListener('change', renderBoard);
    document.getElementById('status-filter').addEventListener('change', renderBoard);

    const clearBtn  = document.getElementById('clear-search');
    const searchBox = document.getElementById('search-box');
    searchBox.addEventListener('input', () => { clearBtn.style.display = searchBox.value ? 'block' : 'none'; });
    clearBtn.addEventListener('click', () => { searchBox.value = ''; clearBtn.style.display = 'none'; renderBoard(); });

    document.getElementById('flight-list').addEventListener('click', (e) => {
        const ac = e.target.closest('.airline-click');
        if (ac) { e.stopPropagation(); openAirlineModal(ac.dataset.airline, ac.dataset.logo); return; }
        const row = e.target.closest('.flight-row');
        if (row) openFlightModal(currentDisplayedFlights[row.getAttribute('data-index')]);
    });

    document.getElementById('airline-modal').addEventListener('click', function(e) { if (e.target === this) closeModals(); });
    document.getElementById('flight-modal').addEventListener('click',  function(e) { if (e.target === this) closeModals(); });
    document.getElementById('history-modal').addEventListener('click', function(e) { if (e.target === this) closeHistoryModal(); });

    document.addEventListener('click', (e) => {
        if (!e.target.closest('#airline-filter-wrapper')) closeAirlineDropdown();
    });

    // Load available history dates
    loadHistoryDates();
});

function changeLanguage(lang) { currentLang = lang; renderBoard(); }
function updateClock() { try { document.getElementById('clock').innerText = new Date().toLocaleTimeString('vi-VN'); } catch(e) {} }

// ===== DATE / TAB SWITCHING =====
function switchDate(d) {
    // Exit history mode when switching to standard date tabs
    if (historyDateStr) {
        historyDateStr = null;
        document.getElementById('history-mode-banner')?.remove();
    }
    currentDateTab = d;
    document.querySelectorAll('.date-btn').forEach(b => b.classList.remove('active'));
    document.getElementById(`btn-date-${d}`).classList.add('active');
    // Auto-scroll ONLY when clicking Today
    shouldScrollToNow = (d === 'today');
    renderBoard();
    scheduleBatchFetch();
}

function switchTab(t) {
    currentTab = t;
    document.getElementById('btn-dep').classList.remove('active');
    document.getElementById('btn-arr').classList.remove('active');
    document.getElementById(`btn-${t}`).classList.add('active');
    renderBoard();
    scheduleBatchFetch();
}

// ===== FETCH =====
async function fetchNaritaWeather() {
    try {
        const res  = await fetch('/api/weather');
        const data = await res.json();
        if (data && data.length > 0) {
            const m = data[0];
            const windStr = m.wspd === 0 ? 'Calm' : `${m.wdir}° at ${m.wspd} knots`;
            document.getElementById('nrt-metar').innerText =
                `[Raw] ${m.rawOb} | [Decoded] Wind: ${windStr} | Vis: ${m.visib}+ SM | Temp: ${m.temp}°C | QNH: ${m.altim} hPa`;
        }
    } catch(e) {}
}

async function fetchFlights() {
    try {
        const response = await fetch('/api/flights');
        const resData  = await response.json();
        flightDataCache = resData.data;
        if (resData.lastUpdate) {
            const d = new Date(resData.lastUpdate);
            document.getElementById('lbl-last-update').innerText =
                `Last Updated: ${d.toLocaleTimeString('en-US', { timeZone: 'Asia/Tokyo', hour12: false, hour: '2-digit', minute: '2-digit' })}`;
        }
        // DO NOT set shouldScrollToNow here — only set on page load or "Today" click
        renderBoard();
        scheduleBatchFetch();
    } catch(e) {}
}

// ===== HISTORY NAVIGATION =====
async function loadHistoryDates() {
    try {
        const res  = await fetch('/api/history/dates');
        const data = await res.json();
        availableHistoryDates = data.dates || [];
        const btn = document.getElementById('btn-history');
        if (btn && availableHistoryDates.length > 0) {
            btn.style.display = '';
            btn.title = `${availableHistoryDates.length} past date(s) available`;
        }
    } catch(e) {}
}

function openHistoryModal() {
    const modal = document.getElementById('history-modal');
    const list  = document.getElementById('history-date-list');
    if (!list) return;

    if (availableHistoryDates.length === 0) {
        list.innerHTML = '<p style="color:#666;padding:20px;text-align:center;">No historical data available yet.<br>Data is saved each day automatically.</p>';
    } else {
        list.innerHTML = availableHistoryDates.map(d => {
            const date   = new Date(d + 'T00:00:00+09:00');
            const label  = date.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric', timeZone: 'Asia/Tokyo' });
            return `<div class="history-date-item" onclick="loadHistoryDate('${d}')">${label}<span style="color:#555;font-size:0.85rem;margin-left:8px;">${d}</span></div>`;
        }).join('');
    }
    modal.classList.add('show');
}

function closeHistoryModal() {
    document.getElementById('history-modal').classList.remove('show');
}

async function loadHistoryDate(dateStr) {
    closeHistoryModal();
    try {
        const res  = await fetch(`/api/history/${dateStr}`);
        if (!res.ok) { alert(`No data found for ${dateStr}`); return; }
        const data = await res.json();
        historyDataCache[dateStr] = data;
        historyDateStr = dateStr;

        // Clear active state on date tabs
        document.querySelectorAll('.date-btn').forEach(b => b.classList.remove('active'));

        // Show banner
        let banner = document.getElementById('history-mode-banner');
        if (!banner) {
            banner = document.createElement('div');
            banner.id = 'history-mode-banner';
            banner.style.cssText = 'background:#1a0d00;border:1px solid #ff8800;border-radius:6px;padding:6px 14px;font-size:1.1rem;color:#ff8800;display:flex;align-items:center;gap:10px;margin-bottom:2px;';
            document.querySelector('.master-tabs-wrapper').insertAdjacentElement('afterend', banner);
        }
        const date  = new Date(dateStr + 'T00:00:00+09:00');
        const label = date.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'short', day: 'numeric', timeZone: 'Asia/Tokyo' });
        banner.innerHTML = `📅 <strong>History Mode:</strong> ${label} &nbsp; <button onclick="switchDate('today')" style="background:#333;border:1px solid #555;color:#fff;padding:2px 10px;border-radius:3px;cursor:pointer;font-family:Oswald,sans-serif;">← Back to Live</button>`;

        shouldScrollToNow = false;
        renderBoard();
    } catch(e) {
        alert(`Error loading data for ${dateStr}`);
    }
}

function getTargetDateStr() {
    if (historyDateStr) return historyDateStr;
    let offset = 0;
    if (currentDateTab === 'yesterday') offset = -1;
    if (currentDateTab === 'tomorrow')  offset =  1;
    const d = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo' }));
    d.setDate(d.getDate() + offset);
    return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

function getCurrentFlightData() {
    if (historyDateStr) return historyDataCache[historyDateStr] || {};
    return flightDataCache[currentDateTab] || {};
}

function formatLocalTimeOnly(unixTimestamp, timezone) {
    if (!unixTimestamp) return '--:--';
    try {
        return new Date(unixTimestamp * 1000).toLocaleTimeString('en-US',
            { timeZone: timezone, hour12: false, hour: '2-digit', minute: '2-digit' });
    } catch(e) { return '--:--'; }
}

function getRelativeTimeStr(unixTime) {
    if (!unixTime) return '';
    const t = translations[currentLang];
    const now = Math.floor(Date.now() / 1000);
    const diff = unixTime - now, abs = Math.abs(diff);
    const h = Math.floor(abs / 3600), m = Math.floor((abs % 3600) / 60);
    const timeStr = h > 0 ? `${h}${t.h} ${m}${t.m}` : `${m}${t.m}`;
    return diff < 0 ? `(${timeStr} ${t.ago})` : `(${t.inTime} ${timeStr})`;
}

function calcFlightDuration(data) {
    const dep = data.actualDep || data.estDep || data.schedDep;
    const arr = data.actualArr || data.estArr || data.schedArr;
    if (!dep || !arr || arr <= dep) return null;
    const diff = Math.round((arr - dep) / 60);
    if (diff <= 0 || diff > 1800) return null;
    return `${Math.floor(diff/60)}h ${diff%60}m`;
}

// ===== CUSTOM AIRLINE FILTER DROPDOWN (multi-select) =====
function buildAirlineDropdown(airlines) {
    const list    = document.getElementById('airline-filter-list');
    const display = document.getElementById('airline-filter-display-text');
    if (!list) return;

    // Pending selection state (uncommitted until Apply)
    if (!window._pendingAirlines) window._pendingAirlines = new Set(airlineFilterValues);
    const pending = window._pendingAirlines;

    list.innerHTML = '';

    // ── Header with Apply + Clear All ──
    const header = document.createElement('div');
    header.style.cssText = 'display:flex;gap:6px;padding:8px 10px;border-bottom:1px solid #333;position:sticky;top:0;background:#111;z-index:2;';
    header.innerHTML = `
        <button id="af-apply"    style="flex:1;background:#033B7D;color:#fff;border:none;border-radius:4px;padding:5px 0;font-family:Oswald,sans-serif;font-size:1rem;cursor:pointer;font-weight:bold;">✔ APPLY</button>
        <button id="af-clearall" style="flex:1;background:#222;color:#aaa;border:1px solid #444;border-radius:4px;padding:5px 0;font-family:Oswald,sans-serif;font-size:1rem;cursor:pointer;">✖ ALL</button>`;
    list.appendChild(header);

    header.querySelector('#af-apply').addEventListener('click', (e) => {
        e.stopPropagation();
        airlineFilterValues = new Set(pending);
        _updateAirlineDisplay(display, airlines);
        closeAirlineDropdown();
        renderBoard();
    });
    header.querySelector('#af-clearall').addEventListener('click', (e) => {
        e.stopPropagation();
        pending.clear();
        airlineFilterValues = new Set();
        _updateAirlineDisplay(display, airlines);
        closeAirlineDropdown();
        renderBoard();
        // Rebuild to uncheck all
        buildAirlineDropdown(airlines);
    });

    // ── Airline options ──
    airlines.forEach(a => {
        const item = document.createElement('div');
        const checked = pending.has(a.name);
        item.className = 'cs-option' + (checked ? ' cs-selected' : '');
        item.innerHTML = `
            <span style="color:${checked?'#00ffff':'#555'};font-size:1.1rem;flex-shrink:0;">${checked?'☑':'☐'}</span>
            ${a.logoUrl ? `<img src="${a.logoUrl}" class="cs-logo" onerror="this.style.display='none'">` : ''}
            <span class="cs-option-name">${a.name}</span>`;
        item.addEventListener('click', (e) => {
            e.stopPropagation();
            if (pending.has(a.name)) pending.delete(a.name);
            else pending.add(a.name);
            buildAirlineDropdown(airlines); // re-render checkboxes
        });
        list.appendChild(item);
    });
}

function _updateAirlineDisplay(display, airlines) {
    if (!display) return;
    if (airlineFilterValues.size === 0) {
        display.innerHTML = 'All Airlines';
    } else if (airlineFilterValues.size === 1) {
        const name = [...airlineFilterValues][0];
        const a = airlines?.find(x => x.name === name);
        display.innerHTML = `${a?.logoUrl ? `<img src="${a.logoUrl}" class="cs-logo-sm" onerror="this.style.display='none'">` : ''}<span>${name}</span>`;
    } else {
        display.innerHTML = `<span style="color:#00ffff;">${airlineFilterValues.size} airlines</span>`;
    }
}

function toggleAirlineDropdown() {
    const list = document.getElementById('airline-filter-list');
    if (!list) return;
    const isOpen = list.classList.contains('cs-open');
    if (!isOpen) {
        // Reset pending to match current applied filter on each open
        window._pendingAirlines = new Set(airlineFilterValues);
    }
    list.classList.toggle('cs-open');
}
function closeAirlineDropdown()  { document.getElementById('airline-filter-list')?.classList.remove('cs-open'); }

function populateAirlineFilter() {
    const flights = (getCurrentFlightData())[currentTab === 'dep' ? 'departures' : 'arrivals'] || [];
    const seen = new Set(), airlines = [];
    flights.forEach(f => {
        if (f.airline && !seen.has(f.airline)) { seen.add(f.airline); airlines.push({ name: f.airline, logoUrl: f.logoUrl || '' }); }
    });
    airlines.sort((a,b) => a.name.localeCompare(b.name));
    // Remove any selected airlines no longer visible in this view
    let changed = false;
    airlineFilterValues.forEach(name => {
        if (!airlines.find(a => a.name === name)) { airlineFilterValues.delete(name); changed = true; }
    });
    if (changed) {
        const d = document.getElementById('airline-filter-display-text');
        _updateAirlineDisplay(d, airlines);
    }
    buildAirlineDropdown(airlines);
}

// ===== SCROLL TO CURRENT TIME =====
function scrollToCurrentTime() {
    if (currentDateTab !== 'today' || historyDateStr) return;
    const nowJST  = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo' }));
    const nowMins = nowJST.getHours() * 60 + nowJST.getMinutes();
    const rows    = document.querySelectorAll('#flight-list .flight-row');
    let bestRow = null, bestDiff = Infinity;
    currentDisplayedFlights.forEach((f, i) => {
        const [h, m] = (f.std || '00:00').split(':').map(Number);
        const diff   = (h*60+m) - nowMins;
        if (diff >= -30 && diff < bestDiff) { bestDiff = diff; bestRow = rows[i]; }
    });
    if (bestRow) {
        setTimeout(() => {
            bestRow.scrollIntoView({ behavior: 'smooth', block: 'center' });
            bestRow.classList.add('current-time-row');
            setTimeout(() => bestRow?.classList.remove('current-time-row'), 4000);
        }, 200);
    }
}

// ===== RENDER BOARD =====
function renderBoard() {
    const listEl  = document.getElementById('flight-list');
    const dataKey = currentTab === 'dep' ? 'departures' : 'arrivals';
    let flights   = (getCurrentFlightData())[dataKey] || [];

    const isArr = currentTab === 'arr';
    document.getElementById('col-std').textContent = isArr ? 'STA' : 'STD';
    document.getElementById('col-etd').textContent = isArr ? 'ETA / ATA' : 'ETD / ATD';
    document.getElementById('col-loc').textContent = isArr ? 'ORIGIN' : 'DESTINATION';

    populateAirlineFilter();

    if (airlineFilterValues.size > 0) flights = flights.filter(f => airlineFilterValues.has(f.airline));

    const searchText = document.getElementById('search-box').value.toLowerCase();
    if (searchText) flights = flights.filter(f =>
        (f.location||'').toLowerCase().includes(searchText) ||
        (f.flightNo||'').toLowerCase().includes(searchText) ||
        (f.airline||'').toLowerCase().includes(searchText)
    );

    const typeFilter = document.getElementById('type-filter').value;
    if (typeFilter !== 'ALL') flights = flights.filter(f => f.type === typeFilter);

    const termFilter = document.getElementById('terminal-filter').value;
    if (termFilter !== 'ALL') flights = flights.filter(f => (f.terminal||'').includes(termFilter));

    const statusFilter = document.getElementById('status-filter').value;
    if (statusFilter !== 'ALL') {
        flights = flights.filter(f => {
            const raw = f.status.includes('DELAYED|') ? f.status.split('|')[1] : f.status;
            const st  = raw.toUpperCase();
            const isCompleted = st.includes('ARRIV') || st.includes('DEPART') || st.includes('LANDED') ||
                st.includes('AIRBORNE') || st.includes('CUSTOMS') || st.includes('BAGGAGE') ||
                st.includes('TAKEOFF') || st.includes('TERMINAL');
            if (statusFilter === 'NORMAL')    return !f.status.toUpperCase().includes('DELAY') && !st.includes('CANCEL') && !isCompleted;
            if (statusFilter === 'DELAYED')   return f.status.toUpperCase().includes('DELAY') || st.includes('CHANGE');
            if (statusFilter === 'COMPLETED') return isCompleted;
            if (statusFilter === 'CANCELED')  return st.includes('CANCEL');
            return true;
        });
    }

    const targetDateStr = getTargetDateStr();
    let htmlStr = '';
    const filteredFlights = [];

    flights.forEach(flight => {
        let statusClass = 'status-green';
        const isFlipping = flight.status.includes('DELAYED|');
        const rawStatus  = isFlipping ? flight.status.split('|')[1] : flight.status;
        const st = rawStatus.toUpperCase();

        const isArrived  = st.includes('ARRIV') || st.includes('TERMINAL') || st.includes('LANDED') || st.includes('BAGGAGE') || st.includes('CUSTOMS');
        const isDeparted = st.includes('DEPART') || st.includes('AIRBORNE') || st.includes('TAKEOFF');

        if (isArrived || isDeparted) statusClass = 'status-arrived';
        else if (st.includes('CANCEL'))  statusClass = 'status-red';
        else if (st.includes('BOARD') || st.includes('FINAL') || st.includes('GATE OPEN') || st.includes('GATE')) statusClass = 'status-boarding';
        else if (st.includes('DELAY') || st.includes('CHANGE')) statusClass = 'status-yellow';

        let displayStatus;
        if (isFlipping) {
            displayStatus = `<div class="flip-status"><span class="c1 status-yellow">DELAYED</span><span class="c2 ${statusClass}">${rawStatus}</span></div>`;
            statusClass = '';
        } else if (st === 'DELAYED') {
            displayStatus = `<span class="status-yellow">DELAYED</span>`; statusClass = '';
        } else {
            displayStatus = rawStatus;
        }

        const cacheKey = flight.flightNo + '_' + targetDateStr;
        const rc = radarCache[cacheKey];
        let finalStd = flight.std, finalEtd = flight.etd;
        // isActual = true → show "A" label; false → show "E" label
        // Start from board status, but FR24 data can OVERRIDE this decision
        let isActual = isDeparted || isArrived;
        // isFlipping (DELAYED|STATUS) does NOT make isActual true by itself —
        // it only means the flight was delayed, not necessarily departed

        if (currentTab === 'arr' && rc) {
            if (rc.schedArr) finalStd = formatLocalTimeOnly(rc.schedArr, 'Asia/Tokyo');
            if (rc.actualArr) {
                finalEtd = formatLocalTimeOnly(rc.actualArr, 'Asia/Tokyo');
                isActual = true;   // FR24 confirms actual landing → A
            } else if (rc.estArr) {
                finalEtd = formatLocalTimeOnly(rc.estArr, 'Asia/Tokyo');
                isActual = false;  // FR24 only has estimate → E
            }
            if (!isArrived && rc.estArr && rc.schedArr && ((rc.estArr - rc.schedArr)/60 >= 15) && !isFlipping) {
                displayStatus = `<div class="flip-status"><span class="c1 status-yellow">DELAYED</span><span class="c2 ${statusClass}">${rawStatus}</span></div>`;
                statusClass = '';
            }
        }

        if (currentTab === 'dep' && rc) {
            if (rc.actualDep) {
                // FR24 confirms real takeoff → A label, use actual time
                const t = formatLocalTimeOnly(rc.actualDep, 'Asia/Tokyo');
                if (t && t !== '--:--') finalEtd = t;
                isActual = true;
            } else if (rc.estDep) {
                // FR24 has estimate only → E label, even if board says DEPARTED
                // This prevents showing "A 16:49" when flight hasn't taken off yet
                const t2 = formatLocalTimeOnly(rc.estDep, 'Asia/Tokyo');
                if (t2 !== '--:--') finalEtd = t2;
                isActual = false;  // ← KEY FIX: board DEPARTED doesn't override FR24 estimate
            }
            // If rc exists but has neither actualDep nor estDep, keep isActual from board status
        }

        // Smart remarks — live data only for destLanded (no cache restore on board)
        let smartRemark = flight.remark;
        if (rc) {
            if (rc.divertedTo && rc.divertedTo !== 'UNKNOWN') {
                smartRemark = `${translations[currentLang].divertedTo} ${rc.divertedTo}`;
                remarkCache[cacheKey] = { type: 'diverted', value: rc.divertedTo };
                try { localStorage.setItem('remarkCache', JSON.stringify(remarkCache)); } catch(e) {}
            } else if (currentTab === 'dep' && rc.actualDep && rc.actualArr && isDeparted) {
                // DEST LANDED: FR24 confirms takeoff + landing, and board confirms DEPARTED
                // NOTE: never restored from remarkCache here — stale cache causes false positives
                smartRemark = translations[currentLang].destLanded;
                remarkCache[cacheKey] = { type: 'destLanded' };
                try { localStorage.setItem('remarkCache', JSON.stringify(remarkCache)); } catch(e) {}
            }
        }
        // Diverted can restore from cache (no false-positive risk), but NOT destLanded
        if (!smartRemark || smartRemark === flight.remark) {
            const cached = remarkCache[cacheKey];
            if (cached?.type === 'diverted')
                smartRemark = `${translations[currentLang].divertedTo} ${cached.value}`;
            // ← destLanded intentionally NOT restored from remarkCache here.
            //   It MUST come from live rc to avoid cross-day / wrong-leg pollution.
        }

        filteredFlights.push(flight);
        const timeLabelLetter = isActual ? 'A' : 'E';
        const matchTime = finalEtd ? finalEtd.match(/\d{2}:\d{2}/) : null;
        const cleanEtd  = matchTime ? matchTime[0] : (finalEtd || '');
        let etdDisplay  = '';
        if (cleanEtd && cleanEtd !== '--:--' && cleanEtd !== finalStd)
            etdDisplay = `<span class="time-label">${timeLabelLetter}</span>${cleanEtd}`;
        else if (isActual && finalStd)
            etdDisplay = `<span class="time-label">${timeLabelLetter}</span>${finalStd}`;

        const gateDisplay    = flight.gate && flight.gate !== '-' ? `<span class="gate-badge">${flight.gate}</span>` : '-';
        const remarkColor    = smartRemark && smartRemark.toUpperCase().includes('DIVERT') ? '#ff3333' : '#00ff00';
        const remarkDisplay  = smartRemark ? `<span class="remark-badge" style="color:${remarkColor};">${smartRemark}</span>` : '';
        const airlineSafe    = (flight.airline  || '').replace(/"/g, '&quot;');
        const logoSafe       = (flight.logoUrl  || '').replace(/"/g, '&quot;');

        htmlStr += `<div class="flight-row" data-index="${filteredFlights.length - 1}">
            <span class="col-time">${finalStd}</span>
            <span class="col-etd">${etdDisplay}</span>
            <span class="col-loc">${flight.location}</span>
            <span class="col-term">${flight.terminal}</span>
            <span class="col-airline"><span class="airline-click" data-airline="${airlineSafe}" data-logo="${logoSafe}"><img src="${flight.logoUrl}" class="airline-logo" onerror="this.style.display='none'"><span class="airline-name">${flight.airline}</span></span></span>
            <span class="col-flt">${flight.flightNo}</span>
            <span class="col-gate">${gateDisplay}</span>
            <span class="col-rem">${remarkDisplay}</span>
            <span class="col-status ${statusClass}">${displayStatus}</span>
        </div>`;
    });

    currentDisplayedFlights = filteredFlights;
    listEl.innerHTML = htmlStr || `<p style="text-align:center;padding:40px;color:#666;">NO FLIGHTS MATCH YOUR FILTER...</p>`;

    if (shouldScrollToNow && currentDateTab === 'today' && !historyDateStr) {
        shouldScrollToNow = false;
        scrollToCurrentTime();
    }
}

function closeModals() {
    document.getElementById('flight-modal').classList.remove('show');
    document.getElementById('airline-modal').classList.remove('show');
}

// ===== DELAY DOT LOGIC =====
function calcRowLogic(sched, est, act, timezone, type) {
    let time = '--:--', label = type === 'dep' ? 'STD' : 'STA', color = 'text-white';
    let badge = '', dot = 'bg-gray', rel = '';
    if (!sched && !est && !act) return { time, label, color, badge, dot, rel };
    let targetUnix = null;
    if (act) { targetUnix = act; label = type === 'dep' ? 'ATD / Takeoff' : 'ATA / Landing'; color = 'text-green'; badge = '<span class="time-label" style="background:#222;border:1px solid #555;">A</span>'; }
    else if (est && sched && est !== sched) { targetUnix = est; label = type === 'dep' ? 'ETD / Takeoff' : 'ETA / Landing'; color = 'text-yellow'; badge = '<span class="time-label" style="background:#222;border:1px solid #555;">E</span>'; }
    else { targetUnix = sched; }
    time = formatLocalTimeOnly(targetUnix, timezone);
    if (targetUnix) rel = getRelativeTimeStr(targetUnix);
    if (targetUnix && sched) {
        const delay = (targetUnix - sched) / 60;
        dot = delay <= 15 ? 'bg-green' : delay <= 45 ? 'bg-yellow' : 'bg-red';
    } else { dot = 'bg-green'; }
    return { time, label, color, badge, dot, rel };
}

// ===== AIRCRAFT PHOTO =====
async function fetchAircraftPhoto(reg) {
    if (!reg || reg === 'N/A' || reg === 'TBD') return;
    try {
        const data = await fetch(`https://api.planespotters.net/pub/photos/reg/${reg}`).then(r => r.json());
        if (data.photos?.length > 0) {
            document.getElementById('aircraft-photo-container').style.display = 'block';
            document.getElementById('aircraft-info-text').innerHTML = `🛩️ ${reg} <span style="font-size:1rem;color:#fff;">(Planespotters.net)</span>`;
            const img = document.getElementById('aircraft-img');
            img.src = data.photos[0].thumbnail_large.src;
            img.onclick = () => window.open(data.photos[0].link, '_blank');
        } else { document.getElementById('aircraft-photo-container').style.display = 'none'; }
    } catch(e) { document.getElementById('aircraft-photo-container').style.display = 'none'; }
}

// ===== DESTINATION WEATHER =====
function fetchDestWeather(icaoCode) {
    const el = document.getElementById('fm-dest-weather');
    if (!el) return;
    if (!icaoCode) { el.innerText = 'N/A'; return; }
    el.innerText = 'Fetching...';
    fetch(`/api/weather?icao=${icaoCode}`)
        .then(r => r.json())
        .then(wdata => {
            if (wdata?.length > 0) {
                const m = wdata[0];
                const windStr = m.wspd === 0 ? 'Calm' : `${m.wdir}° at ${m.wspd}kt`;
                el.innerText = `${m.temp}°C, ${windStr}${m.wxString ? `, ${m.wxString}` : ''}${m.visib ? `, Vis ${m.visib}SM` : ''}`;
            } else { el.innerText = 'No METAR data'; }
        }).catch(() => { el.innerText = 'N/A'; });
}

// ===== FLIGHT DETAIL MODAL =====
function openFlightModal(flight) {
    document.getElementById('flight-modal').classList.add('show');
    document.getElementById('fm-title').innerHTML = `${flight.airline} ${flight.flightNo} - ${flight.location}`;
    document.getElementById('fm-logo').src         = flight.logoUrl;
    document.getElementById('fm-logo').style.display = flight.logoUrl ? 'block' : 'none';

    const matchTime = flight.etd ? flight.etd.match(/\d{2}:\d{2}/) : null;
    const cleanEtd  = matchTime ? matchTime[0] : (flight.etd || '');
    const st = (flight.status || '').toUpperCase();
    const isActual  = st.includes('ARRIV') || st.includes('DEPART') || st.includes('LANDED') || st.includes('TAKEOFF') || flight.status.includes('DELAYED|');
    const nrtLabel  = currentTab === 'dep' ? (isActual ? 'ATD' : 'ETD') : (isActual ? 'ATA' : 'ETA');
    const nrtTime   = cleanEtd && cleanEtd !== '--:--' && cleanEtd !== flight.std
        ? `<span class="time-label" style="background:#222;border:1px solid #555;">${isActual ? 'A' : 'E'}</span> <span class="${isActual ? 'text-green' : 'text-yellow'}">${cleanEtd}</span>`
        : `<span class="text-white">${flight.std}</span>`;
    document.getElementById('fm-line2').innerHTML = `<span class="tl-logo logo-nrt">NRT</span> <strong>${nrtLabel}:</strong> ${nrtTime}`;

    document.getElementById('fm-line1').innerHTML = `<strong>STD:</strong> --:-- &nbsp;&nbsp;&nbsp;&nbsp; <strong>STA:</strong> --:--`;
    document.getElementById('fm-line3').innerHTML = `<span class="tl-logo logo-fr24">FR24</span> <strong>${currentTab === 'dep' ? 'STD' : 'STA'}:</strong> <span class="text-white">--:--</span> <span class="dot bg-gray"></span>`;
    document.getElementById('fm-line4').innerHTML = `<span class="tl-logo logo-fr24">FR24</span> <strong>${currentTab === 'dep' ? 'STA' : 'STD'}:</strong> <span class="text-white">--:--</span> <span class="dot bg-gray"></span>`;

    document.getElementById('fm-terminal').innerText = flight.terminal || '-';
    document.getElementById('fm-floor').innerText    = flight.floor    || '-';
    document.getElementById('fm-gate').innerText     = flight.gate     || '-';
    document.getElementById('fm-flight-time').innerText  = 'Calculating...';
    document.getElementById('fm-dest-weather').innerText = 'Fetching...';

    if (currentTab === 'dep' && flight.checkin) {
        document.getElementById('fm-checkin-container').style.display = 'block';
        document.getElementById('fm-checkin').innerText = flight.checkin;
    } else { document.getElementById('fm-checkin-container').style.display = 'none'; }

    if (flight.remark) {
        document.getElementById('fm-remark-container').style.display = 'block';
        document.getElementById('fm-remark').innerText    = flight.remark;
        document.getElementById('fm-remark').style.color = '#ffcc00';
    } else { document.getElementById('fm-remark-container').style.display = 'none'; }

    const isFlipping = flight.status.includes('DELAYED|');
    document.getElementById('fm-status').innerText    = isFlipping ? flight.status.split('|')[1] + ' (DELAYED)' : flight.status;
    document.getElementById('fm-status').style.color  = flight.status.includes('CANCEL') ? '#ff3333' : '#00ff00';

    document.getElementById('fm-aircraft').innerText      = 'Scanning...';
    document.getElementById('fm-reg').style.display       = 'none';
    document.getElementById('aircraft-photo-container').style.display = 'none';

    const targetDateStr = getTargetDateStr();
    const cacheKey      = flight.flightNo + '_' + targetDateStr;

    function buildTimeline(data) {
        const tzNRT  = 'Asia/Tokyo';
        const tkofTz = currentTab === 'dep' ? tzNRT : (data.orgnTz || tzNRT);
        const landTz = currentTab === 'dep' ? (data.destTz || tzNRT) : tzNRT;

        const stdStr = data.schedDep ? formatLocalTimeOnly(data.schedDep, tkofTz) : '--:--';
        const staStr = data.schedArr ? formatLocalTimeOnly(data.schedArr, landTz) : '--:--';
        document.getElementById('fm-line1').innerHTML = currentTab === 'dep'
            ? `<strong>STD:</strong> ${stdStr} &nbsp;&nbsp;&nbsp;&nbsp; <strong>STA:</strong> ${staStr}`
            : `<strong>STA:</strong> ${staStr} &nbsp;&nbsp;&nbsp;&nbsp; <strong>STD:</strong> ${stdStr}`;

        const depLogic = calcRowLogic(data.schedDep, data.estDep, data.actualDep, tkofTz, 'dep');
        const arrLogic = calcRowLogic(data.schedArr, data.estArr, data.actualArr, landTz, 'arr');
        const row3 = currentTab === 'dep' ? depLogic : arrLogic;
        const row4 = currentTab === 'dep' ? arrLogic : depLogic;

        document.getElementById('fm-line3').innerHTML = `<span class="tl-logo logo-fr24">FR24</span> <strong>${row3.label}:</strong> ${row3.badge} <span class="${row3.color} tl-time">${row3.time}</span> <span class="rel-time">${row3.rel}</span> <span class="dot ${row3.dot}"></span>`;
        document.getElementById('fm-line4').innerHTML = `<span class="tl-logo logo-fr24">FR24</span> <strong>${row4.label}:</strong> ${row4.badge} <span class="${row4.color} tl-time">${row4.time}</span> <span class="rel-time">${row4.rel}</span> <span class="dot ${row4.dot}"></span>`;

        document.getElementById('fm-flight-time').innerText = calcFlightDuration(data) || 'N/A';
        fetchDestWeather(currentTab === 'dep' ? data.destIcao : data.originIcao);

        let smartRemark = null;
        if (data.divertedTo && data.divertedTo !== 'UNKNOWN') smartRemark = `${translations[currentLang].divertedTo} ${data.divertedTo}`;
        else if (currentTab === 'dep' && data.actualDep && data.actualArr) {
            smartRemark = translations[currentLang].destLanded;
            remarkCache[cacheKey] = { type: 'destLanded' };
            try { localStorage.setItem('remarkCache', JSON.stringify(remarkCache)); } catch(e) {}
        }
        // Only restore diverted from cache — NOT destLanded (stale cache risk)
        if (!smartRemark) {
            const cached = remarkCache[cacheKey];
            if (cached?.type === 'diverted') smartRemark = `${translations[currentLang].divertedTo} ${cached.value}`;
        }
        if (smartRemark) {
            document.getElementById('fm-remark-container').style.display = 'block';
            document.getElementById('fm-remark').innerText    = smartRemark;
            document.getElementById('fm-remark').style.color  = smartRemark.toUpperCase().includes('DIVERT') ? '#ff3333' : '#00ff00';
        }
    }

    const rc = radarCache[cacheKey];
    if (rc) {
        if (rc.aircraft && rc.aircraft !== 'Unknown') {
            document.getElementById('fm-aircraft').innerText    = rc.aircraft;
            document.getElementById('fm-aircraft').style.color = '#ffcc00';
        }
        if (rc.registration && rc.registration !== 'N/A') {
            document.getElementById('fm-reg').innerText       = `[${rc.registration}]`;
            document.getElementById('fm-reg').style.display   = 'inline-block';
        }
        buildTimeline(rc);
        if (rc.registration && rc.registration !== 'N/A') fetchAircraftPhoto(rc.registration);
    }

    fetch(`/api/aircraft?flightNo=${flight.flightNo}&date=${targetDateStr}&type=${currentTab === 'dep' ? 'dep' : 'arr'}`)
        .then(r => r.json())
        .then(data => {
            if (data.aircraft === 'Radar Blocked' || data.aircraft === 'Timeout/Error') {
                if (!rc) {
                    document.getElementById('fm-aircraft').innerText    = 'Offline';
                    document.getElementById('fm-aircraft').style.color  = '#ff3333';
                    document.getElementById('fm-flight-time').innerText = 'N/A';
                    document.getElementById('fm-dest-weather').innerText = 'N/A';
                }
            } else {
                document.getElementById('fm-aircraft').innerText    = data.aircraft !== 'Unknown' ? data.aircraft : 'Not Assigned';
                document.getElementById('fm-aircraft').style.color  = '#ffcc00';
                if (data.registration && data.registration !== 'N/A') {
                    document.getElementById('fm-reg').innerText     = `[${data.registration}]`;
                    document.getElementById('fm-reg').style.display = 'inline-block';
                }
                data.lastScan = Date.now();
                radarCache[cacheKey] = data;
                try { localStorage.setItem('radarCache', JSON.stringify(radarCache)); } catch(e) {}
                buildTimeline(data);
                if (data.registration && data.registration !== 'N/A') fetchAircraftPhoto(data.registration);
            }
        }).catch(() => {
            document.getElementById('fm-flight-time').innerText  = 'N/A';
            document.getElementById('fm-dest-weather').innerText = 'N/A';
        });
}

// ===== AIRLINE PROFILE MODAL =====
function openAirlineModal(airlineName, logoUrl) {
    if (!airlineName) return;
    currentAirlineName = airlineName;
    currentAirlineLogo = logoUrl || '';
    currentAirlineTab  = 'dep';

    document.getElementById('am-title').textContent = airlineName;
    document.getElementById('am-logo').src          = logoUrl || '';
    document.getElementById('am-logo').style.display = logoUrl ? 'block' : 'none';
    document.getElementById('am-dep-btn').classList.add('active');
    document.getElementById('am-arr-btn').classList.remove('active');

    // ── Find airline info ─────────────────────────────────────────────────────
    const allFlights = [
        ...((getCurrentFlightData().departures) || []),
        ...((getCurrentFlightData().arrivals)   || [])
    ];
    const sample = allFlights.find(f => f.airline === airlineName);
    const iata   = sample ? getAirlineIata(sample.flightNo) : null;

    // Primary: look up by IATA code from flight number prefix
    let info = iata ? AIRLINE_DB[iata] : null;

    // Fallback 1: exact name match (case-insensitive)
    if (!info) {
        info = Object.values(AIRLINE_DB).find(a =>
            a.name.toLowerCase() === airlineName.toLowerCase()
        ) || null;
    }

    // Fallback 2: airline name contains DB name (minimum 5 chars to avoid false "Air" matches)
    if (!info) {
        info = Object.values(AIRLINE_DB).find(a => {
            const dbName = a.name.toLowerCase();
            const searchName = airlineName.toLowerCase();
            // Require at least 5-char word match to avoid matching "Air" everywhere
            return dbName.length >= 5 && (
                searchName.includes(dbName) ||
                dbName.includes(searchName)
            );
        }) || null;
    }

    const infoEl = document.getElementById('am-info');
    if (info) {
        infoEl.innerHTML = `<div class="am-info-grid">
            <div><span class="am-label">IATA / ICAO</span><span class="am-value">${info.iata} / ${info.icao}</span></div>
            <div><span class="am-label">Founded</span><span class="am-value">${info.founded}</span></div>
            <div><span class="am-label">Alliance</span>${getAllianceBadge(info.alliance)}</div>
            <div><span class="am-label">Fleet</span><span class="am-value">${info.fleet} aircraft</span></div>
            <div style="grid-column:1/-1;"><span class="am-label">Main Hubs</span><span class="am-value">${info.hubs}</span></div>
        </div>`;
        infoEl.style.display = 'block';
    } else {
        infoEl.style.display = 'none';
    }

    renderAirlineModal();
    document.getElementById('airline-modal').classList.add('show');
}

function switchAirlineTab(tab) {
    currentAirlineTab = tab;
    document.getElementById('am-dep-btn').classList.toggle('active', tab === 'dep');
    document.getElementById('am-arr-btn').classList.toggle('active', tab === 'arr');
    renderAirlineModal();
}

function renderAirlineModal() {
    const dataKey = currentAirlineTab === 'dep' ? 'departures' : 'arrivals';
    const all     = (getCurrentFlightData())[dataKey] || [];
    const flights = all.filter(f => f.airline === currentAirlineName).sort((a,b) => a.std.localeCompare(b.std));
    currentAirlineFlights = flights;

    if (flights.length === 0) {
        document.getElementById('am-list').innerHTML = '<p style="text-align:center;padding:20px;color:#666;">No flights found.</p>';
        return;
    }

    const targetDateStr = getTargetDateStr();
    const nowJST  = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo' }));
    const nowMins = nowJST.getHours() * 60 + nowJST.getMinutes();

    let html = `<div class="am-flight-list">`;

    flights.forEach((f, idx) => {
        const rc    = radarCache[f.flightNo + '_' + targetDateStr];
        const [ph, pm] = (f.std || '00:00').split(':').map(Number);
        const fMins = ph*60 + pm;
        const isCurrent = Math.abs(fMins - nowMins) <= 30 && currentDateTab === 'today' && !historyDateStr;

        // ── Time display ──
        let updatedTime = '', updatedColor = '#ffcc00', timeIsActual = false;
        if (currentAirlineTab === 'arr' && rc) {
            if (rc.actualArr) { updatedTime = formatLocalTimeOnly(rc.actualArr, 'Asia/Tokyo'); updatedColor = '#00ff00'; timeIsActual = true; }
            else if (rc.estArr) { updatedTime = formatLocalTimeOnly(rc.estArr, 'Asia/Tokyo'); }
        } else if (currentAirlineTab === 'dep') {
            if (rc?.actualDep) { updatedTime = formatLocalTimeOnly(rc.actualDep, 'Asia/Tokyo'); updatedColor = '#00ff00'; timeIsActual = true; }
            else {
                const m2 = f.etd ? f.etd.match(/\d{2}:\d{2}/) : null;
                if (m2 && m2[0] !== f.std) updatedTime = m2[0];
                if (rc?.estDep) { const t2 = formatLocalTimeOnly(rc.estDep, 'Asia/Tokyo'); if (t2 !== '--:--') updatedTime = t2; }
            }
        }

        // ── Status ──
        const isFlip   = f.status.includes('DELAYED|');
        const rawSt    = isFlip ? f.status.split('|')[1] : f.status;
        const stUp     = rawSt.toUpperCase();
        let stColor    = '#00ff00';
        if (stUp.includes('CANCEL'))  stColor = '#ff3333';
        else if (stUp.includes('DELAY') || isFlip) stColor = '#ffcc00';
        else if (stUp.includes('ARRIV') || stUp.includes('DEPART') || stUp.includes('CUSTOMS') ||
                 stUp.includes('LANDED') || stUp.includes('BAGGAGE') || stUp.includes('TERMINAL') ||
                 stUp.includes('AIRBORNE')) stColor = '#555';
        const statusLabel = isFlip ? 'DELAYED' : rawSt;

        // ── Gate / Terminal / Checkin ──
        const gateHtml   = f.gate && f.gate !== '-' ? `<span class="gate-badge">${f.gate}</span>` : '';
        const termHtml   = f.terminal && f.terminal !== '-' ? `<span class="am-term-badge">${f.terminal}</span>` : '';
        const checkinHtml = (currentAirlineTab === 'dep' && f.checkin) ? `<span class="am-checkin-badge">CK ${f.checkin}</span>` : '';
        const remarkHtml  = f.remark ? `<span class="am-remark-sm">${f.remark}</span>` : '';

        // ── Label prefix ──
        const timePrefix = updatedTime
            ? (timeIsActual
                ? `<span class="am-timelabel" style="background:#00ff00;color:#000;">A</span>`
                : `<span class="am-timelabel">E</span>`)
            : '';

        html += `<div class="am-card${isCurrent ? ' am-current' : ''}" data-index="${idx}">
            <!-- Row 1: Full destination name -->
            <div class="am-card-row1">
                <span class="am-dest">${f.location}</span>
                <span class="am-fltno-lg">${f.flightNo}</span>
            </div>
            <!-- Row 2: Gate | Terminal | Checkin badges -->
            <div class="am-card-row2b">
                <span class="am-gate-col">${f.gate && f.gate !== '-' ? `<span class="gate-badge">${f.gate}</span>` : ''}</span>
                <span class="am-term-col">${f.terminal && f.terminal !== '-' ? `<span class="am-term-badge">${f.terminal}</span>` : ''}</span>
                <span class="am-ck-col">${(currentAirlineTab === 'dep' && f.checkin) ? `<span class="am-checkin-badge">CK&nbsp;${f.checkin}</span>` : ''}</span>
                <span class="am-row2-spacer"></span>
                <span class="am-status-sm" style="color:${stColor};">${statusLabel}</span>
            </div>
            <!-- Row 3: STD → updated time + remark -->
            <div class="am-card-row3">
                <span class="am-times">
                    <span class="am-std-sm">${f.std}</span>
                    ${updatedTime ? `<span class="am-arrow">→</span>${timePrefix}<span class="am-etd-sm" style="color:${updatedColor};">${updatedTime}</span>` : ''}
                </span>
                <span class="am-row2-mid">${remarkHtml}</span>
            </div>
        </div>`;
    });

    html += `</div>`;
    const amList = document.getElementById('am-list');
    amList.innerHTML = html;
    amList.onclick = (e) => {
        const row = e.target.closest('.am-card');
        if (!row) return;
        const flight = currentAirlineFlights[parseInt(row.dataset.index)];
        if (!flight) return;
        const savedTab = currentTab; currentTab = currentAirlineTab;
        closeModals();
        setTimeout(() => {
            if (savedTab !== currentAirlineTab) {
                document.getElementById('btn-dep').classList.toggle('active', currentTab === 'dep');
                document.getElementById('btn-arr').classList.toggle('active', currentTab === 'arr');
                renderBoard();
            }
            openFlightModal(flight);
        }, 100);
    };
}

// ===== BATCH + CONTINUOUS RADAR FETCH =====
function scheduleBatchFetch() {
    if (batchScheduled) return;
    batchScheduled = true;
    setTimeout(() => {
        batchScheduled = false;
        startBatchFetch('dep');
        startBatchFetch('arr');
    }, 1500);
}

function startBatchFetch(tabType) {
    const dataKey  = tabType === 'dep' ? 'departures' : 'arrivals';
    const flights  = (getCurrentFlightData())[dataKey] || [];
    const dateStr  = getTargetDateStr();
    const nowJST   = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo' }));
    const nowMins  = nowJST.getHours() * 60 + nowJST.getMinutes();

    flights.forEach(f => {
        const cacheKey = f.flightNo + '_' + dateStr;
        if (radarCache[cacheKey]) return;  // Already fetched — skip
        const [h, m] = (f.std || '00:00').split(':').map(Number);
        const fMins  = h*60 + m;
        if (Math.abs(fMins - nowMins) > 720) return;  // ±12h window
        if (!batchFetchQueue.find(q => q.flightNo === f.flightNo && q.tabType === tabType))
            batchFetchQueue.push({ flightNo: f.flightNo, tabType, cacheKey });
    });

    if (!batchFetchRunning && batchFetchQueue.length > 0) processBatchQueue();
}

// Continuous live refresh: re-fetch flights with estimated (but not actual) times every 4 min
function runLiveRadarRefresh() {
    if (historyDateStr) return;  // Skip in history mode
    const dateStr = getTargetDateStr();
    ['dep', 'arr'].forEach(tabType => {
        const dataKey = tabType === 'dep' ? 'departures' : 'arrivals';
        const flights = (getCurrentFlightData())[dataKey] || [];
        const nowJST  = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Tokyo' }));
        const nowMins = nowJST.getHours() * 60 + nowJST.getMinutes();

        flights.forEach(f => {
            const cacheKey = f.flightNo + '_' + dateStr;
            const rc = radarCache[cacheKey];
            const [h, m] = (f.std || '00:00').split(':').map(Number);
            const fMins  = h*60 + m;
            if (Math.abs(fMins - nowMins) > 360) return;  // ±6h for live

            // Re-fetch if: no data yet, OR has est but not actual (still active), OR data is >10 min old
            const isActive = rc && ((rc.estArr && !rc.actualArr) || (rc.estDep && !rc.actualDep));
            const isStale  = rc && rc.lastScan && (Date.now() - rc.lastScan) > 10 * 60 * 1000;
            if (!rc || isActive || isStale) {
                if (!batchFetchQueue.find(q => q.flightNo === f.flightNo && q.tabType === tabType)) {
                    // Force-refresh by temporarily removing from cache
                    if (rc) delete radarCache[cacheKey];
                    batchFetchQueue.push({ flightNo: f.flightNo, tabType, cacheKey });
                }
            }
        });
    });
    if (!batchFetchRunning && batchFetchQueue.length > 0) processBatchQueue();
}

async function processBatchQueue() {
    if (batchFetchQueue.length === 0) {
        batchFetchRunning = false;
        if (batchRenderNeeded) { batchRenderNeeded = false; renderBoard(); }
        return;
    }
    batchFetchRunning = true;
    const item      = batchFetchQueue.shift();
    const targetDateStr = getTargetDateStr();

    try {
        const data = await fetch(
            `/api/aircraft?flightNo=${item.flightNo}&date=${targetDateStr}&type=${item.tabType === 'dep' ? 'dep' : 'arr'}`
        ).then(r => r.json());

        if (data && data.aircraft !== 'Radar Blocked' && data.aircraft !== 'Timeout/Error' && data.aircraft !== 'Error') {
            data.lastScan = Date.now();
            radarCache[item.cacheKey] = data;
            try { localStorage.setItem('radarCache', JSON.stringify(radarCache)); } catch(e) {}

            // NOTE: destLanded is intentionally NOT written to remarkCache here.
            // renderBoard reads it directly from radarCache (rc.actualDep && rc.actualArr && isDeparted).
            // Writing to remarkCache early caused false positives for ON TIME flights.
            if (data.divertedTo && data.divertedTo !== 'UNKNOWN') {
                remarkCache[item.cacheKey] = { type: 'diverted', value: data.divertedTo };
                try { localStorage.setItem('remarkCache', JSON.stringify(remarkCache)); } catch(e) {}
            }
            batchRenderNeeded = true;
        }
    } catch(e) {}

    setTimeout(processBatchQueue, 2500);
}