const express = require('express');
const puppeteer = require('puppeteer');
const cors = require('cors');
const fs = require('fs'); // Thêm thư viện đọc/ghi file
const path = require('path');

const app = express();
app.use(cors());
app.use(express.static('public')); 

const DATA_FILE = path.join(__dirname, 'flights_data.json');

// --- HỆ THỐNG CACHE CỨNG ---
let cachedData = { departures: [], arrivals: [] };
let lastUpdate = null;
let isScraping = false;

// Đọc dữ liệu từ ổ cứng ngay khi khởi động server
if (fs.existsSync(DATA_FILE)) {
    try {
        const rawData = fs.readFileSync(DATA_FILE, 'utf8');
        const parsed = JSON.parse(rawData);
        cachedData = parsed.data;
        lastUpdate = new Date(parsed.lastUpdate);
        console.log('⚡ Đã tải dữ liệu tức thì từ file flights_data.json');
    } catch (e) {
        console.log('⚠️ Không thể đọc file cache, đang dùng dữ liệu trống.');
    }
}

async function scrapeFlights() {
    if (isScraping) return; 
    isScraping = true;
    
    let browser;
    try {
        console.log('-----------------------------------');
        console.log('Bắt đầu cào dữ liệu FULL NGÀY từ Narita...');
        browser = await puppeteer.launch({ 
            headless: "new",
            args: ['--no-sandbox', '--disable-setuid-sandbox'] 
        });
        
        const page = await browser.newPage();
        await page.setViewport({ width: 1280, height: 1080 }); 
        
        await page.setRequestInterception(true);
        page.on('request', (req) => {
            if (['image', 'media', 'font'].includes(req.resourceType())) {
                req.abort();
            } else {
                req.continue();
            }
        });

        const scrapePage = async (url, flightType) => {
            console.log(`Đang truy cập: ${url}`);
            await page.goto(url, { waitUntil: 'networkidle2', timeout: 90000 });
            await page.waitForSelector('a[class*="part-module"]', { timeout: 15000 }).catch(() => {});
            
            await page.evaluate(async () => {
                await new Promise((resolve) => {
                    let lastHeight = 0;
                    let retries = 0;
                    let timer = setInterval(() => {
                        window.scrollBy(0, 600); 
                        let currentHeight = document.body.scrollHeight;
                        
                        if (currentHeight === lastHeight) {
                            retries++;
                            if (retries >= 6) { 
                                clearInterval(timer);
                                resolve();
                            }
                        } else {
                            retries = 0;
                            lastHeight = currentHeight;
                        }
                    }, 300); 
                });
            });
            await new Promise(resolve => setTimeout(resolve, 2000)); 

            return await page.evaluate((type) => {
                const flightData = [];
                const rows = document.querySelectorAll('a[class*="part-module"]'); 
                
                const extractTime = (el) => {
                    if (!el) return '--:--';
                    const match = el.innerText.match(/\d{2}:\d{2}/);
                    return match ? match[0] : '--:--';
                };

                rows.forEach((row) => {
                    let std = extractTime(row.querySelector('[class*="prev"]'));
                    let etd = extractTime(row.querySelector('[class*="current"]'));

                    if (std === '--:--' && etd === '--:--') {
                        std = extractTime(row.querySelector('[class*="schedule"]'));
                    }
                    if (std === '--:--') std = extractTime(row);
                    if (std === '--:--') return;

                    let location = row.querySelector('[class*="location"]')?.innerText.trim() || 'UNKNOWN';
                    location = location.replace(/Departure|Destination/g, '').trim();

                    const airline = row.querySelector('[class*="airline"]')?.innerText.trim() || '';
                    const flightNum = row.querySelector('[class*="flight__"] [class*="text"]')?.innerText.trim() || '';
                    const flightNo = `${airline} ${flightNum}`.trim();

                    let gate = '-';
                    const facilities = row.querySelectorAll('[class*="default-module"]');
                    facilities.forEach(fac => {
                        const text = fac.innerText.trim();
                        if (text.startsWith('Gate')) {
                            gate = text.replace(/Gate[：:\s]+/, '').trim();
                        }
                    });

                    let status = 'ON TIME';
                    const statusBadge = row.querySelector('[class*="-has-border__"]');
                    if (statusBadge) {
                        status = statusBadge.innerText.trim().toUpperCase();
                    } else if (row.innerText.toUpperCase().includes('CANCELED')) {
                        status = 'CANCELED';
                    }

                    flightData.push({ std, etd, location, airline, flightNo, gate, status, type });
                });
                return flightData;
            }, flightType);
        };

        const depInt = await scrapePage('https://www.narita-airport.jp/en/flight/dep-search/?searchDepArr=dep-search&time=all&domInter=I', 'I');
        const depDom = await scrapePage('https://www.narita-airport.jp/en/flight/dep-search/?searchDepArr=dep-search&time=all&domInter=D', 'D');
        const arrInt = await scrapePage('https://www.narita-airport.jp/en/flight/arr-search/?searchDepArr=arr-search&time=all&domInter=I', 'I');
        const arrDom = await scrapePage('https://www.narita-airport.jp/en/flight/arr-search/?searchDepArr=arr-search&time=all&domInter=D', 'D');

        const sortFlights = (a, b) => a.std.localeCompare(b.std);
        
        cachedData = {
            departures: [...depInt, ...depDom].sort(sortFlights),
            arrivals: [...arrInt, ...arrDom].sort(sortFlights)
        };
        lastUpdate = new Date();
        
        // LƯU RA FILE ĐỂ BACKUP
        fs.writeFileSync(DATA_FILE, JSON.stringify({ lastUpdate: lastUpdate, data: cachedData }));
        
        console.log(`✅ Cập nhật xong! Departures: ${cachedData.departures.length}, Arrivals: ${cachedData.arrivals.length}`);

    } catch (error) {
        console.error('❌ Lỗi khi cào dữ liệu:', error.message);
    } finally {
        if (browser) await browser.close();
        isScraping = false;
        console.log('-----------------------------------');
    }
}

scrapeFlights();
setInterval(scrapeFlights, 5 * 60 * 1000); 

app.get('/api/flights', (req, res) => {
    res.json({ success: true, lastUpdate: lastUpdate, data: cachedData });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => { console.log(`🚀 Server đang chạy tại PORT ${PORT}`); });