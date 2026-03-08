let currentTab = 'dep'; 
let flightDataCache = { departures: [], arrivals: [] };
let selectedAirlines = new Set(); 

document.addEventListener('DOMContentLoaded', () => {
    fetchFlights();
    setInterval(fetchFlights, 30000); // Tăng tốc độ kiểm tra dữ liệu frontend lên mỗi 30s
    setInterval(updateClock, 1000); 

    window.onclick = function(event) {
        if (!event.target.matches('.select-box') && !event.target.closest('.dropdown-content')) {
            document.getElementById('airline-dropdown').classList.remove('show');
        }
    }
});

function updateClock() {
    const now = new Date();
    document.getElementById('clock').innerText = now.toLocaleTimeString('vi-VN');
}

function switchTab(tabName) {
    currentTab = tabName;
    
    document.getElementById('btn-dep').classList.remove('active');
    document.getElementById('btn-arr').classList.remove('active');
    document.getElementById(`btn-${tabName}`).classList.add('active');

    // Chỉ đổi Text nếu không ở chế độ Mobile (vì Mobile đã ẩn Header)
    const stdCol = document.getElementById('col-std');
    if (stdCol) {
        if (tabName === 'dep') {
            stdCol.innerText = 'STD';
            document.getElementById('col-etd').innerText = 'ETD';
            document.getElementById('col-loc').innerText = 'DESTINATION';
        } else {
            stdCol.innerText = 'STA';
            document.getElementById('col-etd').innerText = 'ETA';
            document.getElementById('col-loc').innerText = 'ORIGIN';
        }
    }

    renderBoard();
}

function toggleAirlineDropdown() {
    document.getElementById('airline-dropdown').classList.toggle('show');
}

function updateAirlineFilter(checkbox) {
    if (checkbox.checked) {
        selectedAirlines.add(checkbox.value);
    } else {
        selectedAirlines.delete(checkbox.value);
    }
    renderBoard();
}

function clearAirlineFilter() {
    selectedAirlines.clear();
    const checkboxes = document.querySelectorAll('#airline-list input[type="checkbox"]');
    checkboxes.forEach(cb => cb.checked = false);
    document.getElementById('airline-dropdown').classList.remove('show');
    renderBoard();
}

async function fetchFlights() {
    try {
        const response = await fetch('/api/flights');
        const resData = await response.json();
        flightDataCache = resData.data;
        
        // Cập nhật Last Update Time
        if (resData.lastUpdate) {
            const updateTime = new Date(resData.lastUpdate);
            document.getElementById('last-update').innerText = `Last Updated: ${updateTime.toLocaleTimeString('vi-VN')}`;
        }
        
        buildAirlineDropdown(); 
        renderBoard(); 
    } catch (error) {
        console.error('Lỗi API:', error);
    }
}

function buildAirlineDropdown() {
    const listContainer = document.getElementById('airline-list');
    listContainer.innerHTML = ''; 
    
    const allFlights = [...flightDataCache.departures, ...flightDataCache.arrivals];
    const airlines = [...new Set(allFlights.map(f => f.airline))].filter(a => a !== '');
    
    airlines.sort().forEach(airline => {
        const label = document.createElement('label');
        const isChecked = selectedAirlines.has(airline) ? 'checked' : '';
        label.innerHTML = `<input type="checkbox" value="${airline}" onchange="updateAirlineFilter(this)" ${isChecked}> ${airline}`;
        listContainer.appendChild(label);
    });
}

function renderBoard() {
    const listElement = document.getElementById('flight-list');
    listElement.innerHTML = ''; 

    let flights = currentTab === 'dep' ? flightDataCache.departures : flightDataCache.arrivals;

    const searchText = document.getElementById('search-box').value.toLowerCase();
    if (searchText) {
        flights = flights.filter(f => 
            f.location.toLowerCase().includes(searchText) || 
            f.flightNo.toLowerCase().includes(searchText) ||
            f.airline.toLowerCase().includes(searchText) // Cho phép tìm theo cả tên hãng
        );
    }

    const typeFilter = document.getElementById('type-filter').value;
    if (typeFilter !== 'ALL') {
        flights = flights.filter(f => f.type === typeFilter);
    }

    if (selectedAirlines.size > 0) {
        flights = flights.filter(f => selectedAirlines.has(f.airline));
    }

    if (!flights || flights.length === 0) {
        listElement.innerHTML = '<p style="text-align:center; padding: 40px; color: #666; font-size: 20px;">NO FLIGHTS MATCH YOUR FILTER OR WAITING FOR DATA...</p>';
        return;
    }

    flights.forEach((flight) => {
        const row = document.createElement('div');
        row.className = 'flight-row';

        let statusClass = 'status-green'; 
        const st = flight.status.toUpperCase();
        
        if (st.includes('ARRIV') || st.includes('TERMINAL') || st.includes('LANDED')) {
            statusClass = 'status-arrived';
        } else if (st.includes('DELAY') || st.includes('NEW') || st.includes('CHANGE')) {
            statusClass = 'status-yellow';
        } else if (st.includes('CANCEL')) {
            statusClass = 'status-red';
        } else if (st.includes('BOARD') || st.includes('FINAL') || st.includes('GO TO GATE')) {
            statusClass = 'status-boarding';
        }

        const etdDisplay = (flight.etd === '--:--' || flight.etd === flight.std) ? '' : flight.etd;

        row.innerHTML = `
            <span>${flight.std}</span>
            <span>${etdDisplay}</span>
            <span>${flight.location}</span>
            <span>${flight.flightNo}</span>
            <span>${flight.gate}</span>
            <span class="${statusClass}">${flight.status}</span>
        `;
        listElement.appendChild(row);
    });
}