import L from "leaflet";
import "leaflet.heat";

export default class MapView {
  constructor(mapContainerId) {
    this.map = L.map(mapContainerId, {
      zoomControl: true,
      worldCopyJump: true,
    }).setView([45, 20], 3);

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 8,
      attribution: "© OpenStreetMap",
    }).addTo(this.map);

    this.markers = [];
  }

  clear() {
    this.markers.forEach(m => this.map.removeLayer(m));
    this.markers = [];
  }

  plotFlights(flights, airportCoords) {
    this.clear();
    const heatPoints = [];

    flights.forEach(f => {
      const from = airportCoords[f.from_airport];
      const to = airportCoords[f.to_airport];
      if (!from || !to) return;

      const line = L.polyline([from, to], {
        color: "#0078ff",
        weight: 1.5,
        opacity: 0.7,
      }).addTo(this.map);
      this.markers.push(line);

      heatPoints.push(from, to);
    });

    if (heatPoints.length) {
      const heatLayer = L.heatLayer(heatPoints, {
        radius: 20,
        blur: 25,
        maxZoom: 6,
      }).addTo(this.map);
      this.markers.push(heatLayer);
    }
  }
}
