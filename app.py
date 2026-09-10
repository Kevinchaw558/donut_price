from flask import Flask, jsonify, render_template_string
import requests
import threading
import time
import struct
from collections import deque

app = Flask(__name__)

API_URL = "https://api.donut.auction/v2/tickers/"

# ============================================================
# SETTINGS
# ============================================================

SAMPLE_INTERVAL = 1          # 1 price check per second
CALIBRATION_INTERVAL = 1800  # 30 minutes
MAX_AGE = 60 * 60 * 24 * 30 * 6  # ~6 months

# We only keep the first 4 meaningful digits.
#
# Example:
#
# 343,512,847 -> 3435 -> 343,500,000
# 343,587,291 -> 3435 -> 343,500,000
# 343,600,000 -> 3436 -> 343,600,000
#
PRICE_DIVISOR = 100_000


# ============================================================
# API HEADERS
# ============================================================

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
        "Chrome/151.0.0.0 Safari/537.36"
    ),
}


# ============================================================
# COMPRESSED STORAGE
# ============================================================

"""
We store the data in 30-minute blocks.

Each block looks like:

    [8-byte timestamp]
    [2-byte absolute price]

    [price delta]
    [price delta]
    [price delta]
    ...
    1800 samples

Then another:

    [8-byte timestamp]
    [2-byte absolute price]

    [price delta]
    ...

The timestamp is only written once every 30 minutes.

The price is stored as:

    actual_price // 100000

So ~343 million becomes ~3435.

Normal deltas are stored in ONE BYTE.

To allow larger deltas, 0x80 is used as an escape:

    0x00 - 0x7F:
        signed delta from -64 to +63

    0x80:
        next 2 bytes contain signed 16-bit delta

This means normal price movement costs only ONE BYTE.
"""


# All compressed data lives here.
compressed_data = bytearray()

# Calibration positions in compressed_data.
#
# Each entry:
#
#     (timestamp, byte_position)
#
calibration_index = []

# Current state
current_price = None
current_timestamp = None

# Number of samples since the latest calibration
samples_since_calibration = 0

# Protects the bytearray and state from simultaneous
# collector/web requests.
data_lock = threading.RLock()


# ============================================================
# PRICE CONVERSION
# ============================================================

def compress_price(price):
    """
    Convert real price into our 4-digit representation.

    Example:
        343,512,847 -> 3435
    """

    return int(price) // PRICE_DIVISOR


def decompress_price(price):
    """
    Convert compressed price back into display price.

    Example:
        3435 -> 343,500,000
    """

    return price * PRICE_DIVISOR


# ============================================================
# DELTA ENCODING
# ============================================================

def write_delta(delta):
    """
    Store a signed price delta.

    If the delta fits inside -64..63:
        one byte

    Otherwise:
        0x80 + signed 16-bit integer
    """

    # Normal one-byte delta.
    if -64 <= delta <= 63:

        # Convert signed range:
        #
        # -64 -> 0
        #   0 -> 64
        # +63 -> 127
        #
        encoded = delta + 64

        compressed_data.append(encoded)

    else:

        # Escape marker.
        compressed_data.append(0x80)

        # Signed 16-bit delta.
        compressed_data.extend(
            struct.pack("<h", delta)
        )


def read_delta(data, position):
    """
    Read one price delta.

    Returns:
        (delta, new_position)
    """

    value = data[position]
    position += 1

    # Normal delta.
    if value != 0x80:

        return value - 64, position

    # Large delta.
    delta = struct.unpack(
        "<h",
        data[position:position + 2]
    )[0]

    position += 2

    return delta, position


# ============================================================
# ADD PRICE
# ============================================================

def add_price(timestamp, real_price):

    global current_price
    global current_timestamp
    global samples_since_calibration

    price = compress_price(real_price)

    with data_lock:

        # ----------------------------------------------------
        # FIRST SAMPLE
        # ----------------------------------------------------

        if current_price is None:

            # Store absolute timestamp.
            compressed_data.extend(
                struct.pack(
                    "<d",
                    timestamp
                )
            )

            # Store absolute price.
            compressed_data.extend(
                struct.pack(
                    "<H",
                    price
                )
            )

            calibration_index.append(
                (
                    timestamp,
                    0
                )
            )

            current_price = price
            current_timestamp = timestamp
            samples_since_calibration = 0

            return

        # ----------------------------------------------------
        # CALIBRATION
        # ----------------------------------------------------

        if samples_since_calibration >= CALIBRATION_INTERVAL:

            offset = len(compressed_data)

            # Absolute timestamp.
            compressed_data.extend(
                struct.pack(
                    "<d",
                    timestamp
                )
            )

            # Absolute price.
            compressed_data.extend(
                struct.pack(
                    "<H",
                    price
                )
            )

            calibration_index.append(
                (
                    timestamp,
                    offset
                )
            )

            current_price = price
            current_timestamp = timestamp
            samples_since_calibration = 0

            return

        # ----------------------------------------------------
        # NORMAL DELTA
        # ----------------------------------------------------

        delta = price - current_price

        write_delta(delta)

        current_price = price
        current_timestamp = timestamp

        samples_since_calibration += 1


# ============================================================
# DECODE HISTORY
# ============================================================

def decode_history():

    with data_lock:

        if not compressed_data:
            return []

        results = []

        position = 0

        current_timestamp = None
        current_price = None

        first_block = True

        while position < len(compressed_data):

            # ------------------------------------------------
            # CALIBRATION / BLOCK START
            # ------------------------------------------------

            if position + 10 > len(compressed_data):
                break

            current_timestamp = struct.unpack(
                "<d",
                compressed_data[
                    position:position + 8
                ]
            )[0]

            position += 8

            current_price = struct.unpack(
                "<H",
                compressed_data[
                    position:position + 2
                ]
            )[0]

            position += 2

            # Add calibration point.
            results.append({
                "time": current_timestamp,
                "price": decompress_price(
                    current_price
                )
            })

            # ------------------------------------------------
            # READ DELTAS
            # ------------------------------------------------

            for _ in range(CALIBRATION_INTERVAL):

                if position >= len(compressed_data):
                    break

                delta, position = read_delta(
                    compressed_data,
                    position
                )

                current_price += delta
                current_timestamp += SAMPLE_INTERVAL

                results.append({
                    "time": current_timestamp,
                    "price": decompress_price(
                        current_price
                    )
                })

        return results


# ============================================================
# API REQUEST
# ============================================================

def get_elytra_price():

    response = requests.get(
        API_URL,
        headers=HEADERS,
        timeout=10,
    )

    response.raise_for_status()

    tickers = response.json()

    elytra = next(
        item
        for item in tickers
        if item["itemName"] == "elytra"
        and not item["isStale"]
    )

    return elytra["unitPrice"]


# ============================================================
# COLLECTOR
# ============================================================

def collector():

    while True:

        try:

            price = get_elytra_price()

            timestamp = time.time()

            add_price(
                timestamp,
                price
            )

            with data_lock:
                size = len(compressed_data)

            print(
                f"Elytra: {price:,} "
                f"| RAM: {size / 1024 / 1024:.2f} MB"
            )

        except Exception as e:

            print(
                f"Error collecting price: {e}"
            )

        time.sleep(
            SAMPLE_INTERVAL
        )


# ============================================================
# HOME PAGE
# ============================================================

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

const ctx =
    document.getElementById("chart");

const chart = new Chart(ctx, {

    type: "line",

    data: {

        labels: [],

        datasets: [{

            label: "Elytra Price",

            data: [],

            borderColor: "#00ff88",

            backgroundColor:
                "rgba(0,255,136,0.1)",

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

    try {

        const response =
            await fetch("/history");

        const data =
            await response.json();

        chart.data.labels =
            data.map(x =>
                new Date(
                    x.time * 1000
                ).toLocaleTimeString()
            );

        chart.data.datasets[0].data =
            data.map(x => x.price);

        chart.update();

        if (data.length > 0) {

            const latest =
                data[data.length - 1].price;

            document
                .getElementById("price")
                .textContent =
                latest.toLocaleString()
                + " coins";

        }

    } catch (error) {

        console.error(
            "History update failed:",
            error
        );

    }

}


update();

setInterval(update, 2000);

</script>

</body>

</html>
""")


# ============================================================
# HISTORY ENDPOINT
# ============================================================

@app.route("/history")
def history():

    return jsonify(
        decode_history()
    )


# ============================================================
# STORAGE STATS
# ============================================================

@app.route("/stats")
def stats():

    with data_lock:

        return jsonify({

            "compressed_bytes":
                len(compressed_data),

            "compressed_mb":
                round(
                    len(compressed_data)
                    / 1024
                    / 1024,
                    3
                ),

            "calibrations":
                len(calibration_index),

            "price_scale":
                PRICE_DIVISOR,

            "calibration_seconds":
                CALIBRATION_INTERVAL,

            "current_price":
                (
                    None
                    if current_price is None
                    else
                    decompress_price(
                        current_price
                    )
                )

        })


# ============================================================
# START SERVER
# ============================================================

if __name__ == "__main__":

    collector_thread = threading.Thread(
        target=collector,
        daemon=True
    )

    collector_thread.start()

    app.run(
        host="0.0.0.0",
        port=10000
    )
