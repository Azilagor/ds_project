import "./components/MapView.js";

window.addEventListener("DOMContentLoaded", async () => {
  const mapView = new MapView("map");

  const response = await fetch("/api/people");
  const people = await response.json();

  const select = document.getElementById("personSelect");
  select.innerHTML = people.map(p => 
    `<option value="${p.first_name}_${p.last_name}">${p.first_name} ${p.last_name}</option>`
  ).join("");

  select.addEventListener("change", async e => {
    const [first, last] = e.target.value.split("_");
    const flights = await fetch(`/api/flights?first=${first}&last=${last}`).then(r => r.json());
    mapView.showFlights(flights);
  });
});
