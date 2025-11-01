// Initialize chart animations and count-up numbers on page load
document.addEventListener("DOMContentLoaded", () => {
  animateChart()
  animateStatistics()
})

// Animate the bar chart on page load
function animateChart() {
  const bars = document.querySelectorAll(".bar")
  bars.forEach((bar, index) => {
    setTimeout(() => {
      bar.style.width = bar.style.getPropertyValue("--bar-value")
    }, index * 100)
  })
}

// Animate statistics count-up
function animateStatistics() {
  const statNumbers = document.querySelectorAll(".stat-number")

  statNumbers.forEach((stat) => {
    const targetValue = Number.parseFloat(stat.getAttribute("data-target"))
    const isDecimal = targetValue % 1 !== 0
    const duration = 2000 // 2 seconds
    const steps = 60
    const stepDuration = duration / steps
    let currentStep = 0

    const interval = setInterval(() => {
      currentStep++
      const progress = currentStep / steps
      const currentValue = targetValue * progress

      if (isDecimal) {
        stat.textContent = currentValue.toFixed(2)
      } else {
        stat.textContent = Math.floor(currentValue)
      }

      if (currentStep >= steps) {
        clearInterval(interval)
        stat.textContent = targetValue
      }
    }, stepDuration)
  })
}

// Handle airline filter change
document.getElementById("airline-select").addEventListener("change", (e) => {
  const selectedAirline = e.target.value
  updateChartColors(selectedAirline)
})

// Update bar chart colors based on selected airline
function updateChartColors(airline) {
  const bars = document.querySelectorAll(".bar")
  let newColor = "#0d6efd" // default blue

  if (airline === "SkyWings") {
    newColor = "#6f42c1" // purple
  } else if (airline === "AeroVenture") {
    newColor = "#fd7e14" // orange
  }

  bars.forEach((bar) => {
    bar.style.setProperty("--bar-color", newColor)
    bar.style.backgroundColor = newColor
  })
}

// Handle Apply Filters button
document.getElementById("apply-filters").addEventListener("click", () => {
  const airline = document.getElementById("airline-select").value
  const city = document.getElementById("departure-select").value

  let message = "Filters applied"
  if (airline) message += ` - Airline: ${airline}`
  if (city) message += ` - Departure: ${city}`

  console.log(message)
  // In a real application, this would trigger data fetching or filtering
})
