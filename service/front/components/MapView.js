import L from "leaflet";
import "leaflet.heat";

export default class MapView {
  constructor(mapContainerId) {
    this.map = L.map(mapContainerId).setView([30, 20], 3);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 7,
    }).addTo(this.map);

    this.markers = [];
  }

  clear() {
    this.markers.forEach(m => this.map.removeLayer(m));
    this.markers = [];
  }

  async plotFlights(flights, airportCoords) {
    this.clear();

    const lines = [];
    const heatPoints = [];

    flights.forEach(f => {
      const from = airportCoords[f.from_airport];
      const to = airportCoords[f.to_airport];
      if (!from || !to) return;

      const line = L.polyline([from, to], {
        color: "#0078ff",
        weight: 1.5,
        opacity: 0.8
      }).addTo(this.map);
      this.markers.push(line);

      // точки для heatmap
      heatPoints.push(from);
      heatPoints.push(to);
    });

    // Добавим тепловую карту
    if (heatPoints.length) {
      const heatLayer = L.heatLayer(heatPoints, {
        radius: 15,
        blur: 20,
        maxZoom: 5
      }).addTo(this.map);
      this.markers.push(heatLayer);
    }

    if (flights.length) {
      const firstRoute = airportCoords[flights[0].from_airport];
      if (firstRoute) this.map.setView(firstRoute, 4);
    }
  }
}
