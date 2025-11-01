import L from "leaflet";
import "leaflet.heat";

// --- Настройка карты ---
const map = L.map("map").setView([55.75, 37.6], 4);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 7,
}).addTo(map);

let activeLayers = [];

// --- Очистка карты ---
function clearMap() {
  activeLayers.forEach(layer => map.removeLayer(layer));
  activeLayers = [];
}

// --- Построение маршрутов ---
function plotFlights(flights) {
  clearMap();
  const heatPoints = [];

  flights.forEach(f => {
    const from = airportCoords[f.from_airport];
    const to = airportCoords[f.to_airport];
    if (!from || !to) return;

    const line = L.polyline([from, to], {
      color: "#0078ff",
      weight: 1.5,
      opacity: 0.8,
    }).addTo(map);
    activeLayers.push(line);

    heatPoints.push(from);
    heatPoints.push(to);
  });

  if (heatPoints.length) {
    const heatLayer = L.heatLayer(heatPoints, { radius: 15, blur: 20 }).addTo(map);
    activeLayers.push(heatLayer);
    map.setView(heatPoints[0], 4);
  }
}

// --- Получение пассажиров ---
async function loadPassengers() {
  const res = await fetch("http://localhost:8081/api/passengers");
  const data = await res.json();

  const list = document.getElementById("passenger-list");
  list.innerHTML = "";

  data.passengers.forEach(p => {
    const item = document.createElement("div");
    item.className = "passenger-item";
    item.textContent = `${p.first_name} ${p.last_name}`;
    item.onclick = () => selectPassenger(p.document, item);
    list.appendChild(item);
  });
}

// --- При выборе пассажира ---
async function selectPassenger(document, el) {
  const res = await fetch(`http://localhost:8081/api/flights/${encodeURIComponent(document)}`);
  const data = await res.json();

  // очистка карты
  map.eachLayer(l => {
    if (l instanceof L.Polyline || l instanceof L.CircleMarker) map.removeLayer(l);
  });

  let any = false;

  // маршруты
  data.flights.forEach(f => {
    const from = airportCoords[f.from_airport || f.flight?.from_airport || f.flight?.from];
    const to   = airportCoords[f.to_airport   || f.flight?.to_airport   || f.flight?.to];
    if (!from || !to) return;

    L.polyline([from, to], { color: "#0d6efd", weight: 2 }).addTo(map);
    L.circleMarker(from, { radius: 5, color: "green" }).addTo(map);
    L.circleMarker(to, { radius: 5, color: "red" }).addTo(map);
    any = true;
  });

  if (any) map.setView([55, 37], 4); // возвращаем камеру к центру России
}


// --- Пример координат аэропортов (добавим позже) ---
const airportCoords = {
  "SVO": [55.9728, 37.4146],
  "LED": [59.8003, 30.2625],
  "CDG": [49.0097, 2.5479],
  "AMS": [52.3105, 4.7683],
  "FCO": [41.7999, 12.2462],
  "ORY": [48.7233, 2.3794],
  "CAN": [23.3924, 113.2990],
  "TPE": [25.0777, 121.2328],
  "JED": [21.6702, 39.1528],
  "ATL": [33.6407, -84.4277],
  "PVG": [31.1443, 121.8083],
  "PRG": [50.1008, 14.26],
  "MXP": [45.63, 8.72],
  "MAD": [40.47, -3.56],
  "DEL": [28.5562, 77.1],
  "HKG": [22.308, 113.9185],
  "HEL": [60.3172, 24.9633],
  "RUH": [24.9578, 46.6988],
  "WAW": [52.1657, 20.9671],
  "LGA": [40.7769, -73.8740],
  "BOS": [42.3656, -71.0096],
  "FLL": [26.0726, -80.1527],
  "EVV": [38.0368, -87.5324]
};


// --- Инициализация ---
loadPassengers();
