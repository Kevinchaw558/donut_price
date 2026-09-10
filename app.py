from flask import Flask, jsonify, render_template_string
import requests
import threading
import time
from collections import deque

app = Flask(__name__)

API_URL = "https://api.donut.auction/v2/tickers/"

# Keep the last 5 minutes (300 seconds)
history = deque(maxlen=300)

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Content-Type": "application/json",
    "Origin": "https://donut.auction",
    "Pragma": "no-cache",
    "Referer": "https://donut.auction/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/152.0.0.0 Safari/537.36"
    ),
}


def get_elytra_price():
    response = requests.get(
        API_URL,
        headers=HEADERS,
        timeout=10,
    )
    response.raise_for_status()

    tickers = response.json()

    elytra = next(
        item for item in tickers
        if item["itemName"] == "elytra"
        and not item["isStale"]
    )

    return elytra["unitPrice"]


def collector():
    while True:
        try:
            price = get_elytra_price()

            history.append({
                "time": time.time(),
                "price": price
            })

            print(f"Elytra: {price:,}")

        except Exception as e:
            print(f"Error collecting price: {e}")

        time.sleep(1)


@app.route("/")
def home():
    return render_template_string("""
<!DOCTYPE html>
<html>
<head>
    <title>Donut Elytra Price</title>

    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

    <style>
        body {
            background: #111;
            color: white;
            font-family: Arial, sans-serif;
            max-width: 1000px;
            margin: 40px auto;
            padding: 20px;
        }

        h1 {
            text-align: center;
        }

        #price {
            text-align: center;
            font-size: 40px;
            font-weight: bold;
            margin: 20px;
        }

        canvas {
            background: #181818;
            border-radius: 12px;
            padding: 10px;
        }
    </style>
</head>

<body>

<h1>Donut SMP Elytra Price</h1>

<div id="price">Loading...</div>

<canvas id="chart"></canvas>

<script>
const ctx = document.getElementById("chart");

const chart = new Chart(ctx, {
    type: "line",

    data: {
        labels: [],
        datasets: [{
            label: "Elytra Price",
            data: [],
            borderColor: "#00ff88",
            backgroundColor: "rgba(0,255,136,0.1)",
            borderWidth: 2,
            tension: 0.2,
            pointRadius: 0,
            fill: true
        }]
    },

    options: {
        responsive: true,

        scales: {
            x: {
                title: {
                    display: true,
                    text: "Time"
                }
            },

            y: {
                title: {
                    display: true,
                    text: "Price"
                }
            }
        }
    }
});


async function update() {
    const response = await fetch("/history");
    const data = await response.json();

    chart.data.labels = data.map(x =>
        new Date(x.time * 1000).toLocaleTimeString()
    );

    chart.data.datasets[0].data =
        data.map(x => x.price);

    chart.update();

    if (data.length > 0) {
        const latest = data[data.length - 1].price;

        document.getElementById("price").textContent =
            latest.toLocaleString() + " coins";
    }
}


update();

setInterval(update, 1000);
</script>

</body>
</html>
""")


@app.route("/history")
def get_history():
    return jsonify(list(history))


if __name__ == "__main__":
    # Start the price collector
    thread = threading.Thread(target=collector, daemon=True)
    thread.start()

    app.run(host="0.0.0.0", port=10000)
