// main.js

const API = "http://localhost:8081/api";

const map = L.map("map").setView([55, 37], 4);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 8,
  minZoom: 2
}).addTo(map);

const airportCoords = {
  "SVO": [55.9726, 37.4146],
  "OVB": [55.0126, 82.6507],
  "CDG": [49.0097, 2.5479],
  "BOS": [42.3656, -71.0096],
  "PIT": [40.4914, -80.2329],
  "VVO": [43.3980, 132.1484],
  "REA": [46.4926, 2.5580],
  "MOW": [55.7558, 37.6173],
  "LED": [59.9390, 30.3158],
  "MOSCOW": [55.7558, 37.6173],
  "PERM": [58.0105, 56.2502],
  "VOLGOGRAD": [48.7080, 44.5133],
  "IRKUTSK": [52.2870, 104.2810],
  "VLADIVOSTOK": [43.1198, 131.8869],
  "SEOUL": [37.5665, 126.9780],
  "PARIS": [48.8566, 2.3522]
  // ... добавь ещё города/коды по потребности
};

function clearMapRoutes() {
  map.eachLayer(l => {
    if (l instanceof L.TileLayer) return;
    try { map.removeLayer(l); } catch(e) {}
  });
}

async function loadAllFlights() {
  const res = await fetch(`${API}/flights/all`);
  const data = await res.json();

  map.eachLayer(l => {
    if (l instanceof L.Polyline || l instanceof L.CircleMarker) map.removeLayer(l);
  });

  data.forEach(f => {
    const from = airportCoords[f.from_airport];
    const to = airportCoords[f.to_airport];
    if (!from || !to) return;

    L.polyline([from, to], {
      color: "red",
      weight: 1.5,
      opacity: 0.6
    }).addTo(map);
  });

  if (data.length) {
    const first = airportCoords[data[0].from_airport];
    if (first) map.setView(first, 4);
  }
}



async function loadPassengers() {
  const res = await fetch(`${API}/passengers`);
  if (!res.ok) return;
  const data = await res.json();
  const listContainer = document.getElementById("passenger-items");
  listContainer.innerHTML = "";

  (data.passengers || []).forEach(p => {
    const div = document.createElement("div");
    div.className = "passenger-item";
    div.textContent = `${p.first_name || ""} ${p.last_name || ""}`;
    div.onclick = () => {
      selectPassengerByDoc(p.document, div);
      document.getElementById("passenger-selected").textContent = `${p.first_name} ${p.last_name} ▲`;
      document.getElementById("passenger-list").classList.add("hidden");
    };
    listContainer.appendChild(div);
  });

  const selectedBox = document.getElementById("passenger-selected");
  const list = document.getElementById("passenger-list");
  selectedBox.onclick = () => list.classList.toggle("hidden");

  const searchBox = document.getElementById("search-box");
  searchBox.addEventListener("input", () => {
    const term = searchBox.value.toLowerCase();
    document.querySelectorAll(".passenger-item").forEach(item => {
      item.style.display = item.textContent.toLowerCase().includes(term) ? "block" : "none";
    });
  });
}

async function selectPassengerByDoc(document, el) {
  clearMapRoutes();

  const res = await fetch(`${API}/spy/${encodeURIComponent(document)}/flights`);
  if (!res.ok) {
    alert("Ошибка загрузки рейсов для документа " + document);
    return;
  }
  const data = await res.json();

  const flights = (data.flights || []).map(f => ({
    from_airport: f.from_airport,
    to_airport: f.to_airport,
    number: f.flight_number,
    date: f.date,
    time: f.time
  }));

  const points = [];

  flights.forEach(f => {
    const fromKey = (f.from_airport || "").toUpperCase() || "MOSCOW";
    const toKey   = (f.to_airport   || "").toUpperCase();

    const from = airportCoords[fromKey];
    const to   = airportCoords[toKey];

    if (!from || !to) return;

    L.polyline([from, to], { color: "#0d6efd", weight: 2 }).addTo(map);
    L.circleMarker(from, { radius: 5, color: "green" }).addTo(map);
    L.circleMarker(to, { radius: 5, color: "red" }).addTo(map);

    points.push(from, to);
  });

  if (points.length) {
    map.fitBounds(points);
  } else {
    alert("Маршрутов для этого пассажира не найдено или не удалось определить аэропорты.");
  }
}

async function findSpies() {
  const res = await fetch(`${API}/spy?limit=20`);
  if (!res.ok) {
    document.getElementById("spy-results").textContent = "Ошибка запроса";
    return;
  }
  const data = await res.json();
  renderSpies(data.spies || []);
}

function renderSpies(list) {
  const box = document.getElementById("spy-results");
  box.innerHTML = "";

  if (!list.length) {
    box.innerHTML = "<div style='padding:8px'>Ничего не найдено</div>";
    return;
  }

  list.forEach(item => {
    const el = document.createElement("div");
    el.className = "spy-item";
    el.innerHTML = `<b>Документ:</b> ${item.document} <small>(${item.count})</small><br>`;
    item.aliases.forEach(a => {
      const row = document.createElement("div");
      row.style.paddingLeft = "8px";
      row.textContent = "→ " + a;
      el.appendChild(row);
    });

    const btn = document.createElement("button");
    btn.textContent = "Проследить";
    btn.style.marginTop = "6px";
    btn.onclick = async (ev) => {
      ev.stopPropagation();
      await selectPassengerByDoc(item.document, btn);
    };
    el.appendChild(btn);

    box.appendChild(el);
  });
}

// Старт
document.getElementById("find-spies").addEventListener("click", findSpies);
loadPassengers();
// Если есть stats API — вызывай:
// loadStats();
