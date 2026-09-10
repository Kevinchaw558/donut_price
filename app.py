from flask import Flask, jsonify, render_template_string, request
import requests
import threading
import time
import struct

app = Flask(__name__)

API_URL = "https://api.donut.auction/v2/tickers/"

# ============================================================
# SETTINGS
# ============================================================

SAMPLE_INTERVAL = 1
CALIBRATION_INTERVAL = 30 * 60
PRICE_DIVISOR = 100_000

# Default granularity for each time range.
DEFAULT_GRANULARITY = {
    "minute": 1,
    "5minutes": 1,
    "hour": 60,
    "day": 300,
    "week": 3600,
    "month": 86400,
    "max": 86400,
}

TIME_RANGES = {
    "minute": 60,
    "5minutes": 5 * 60,
    "hour": 60 * 60,
    "day": 24 * 60 * 60,
    "week": 7 * 24 * 60 * 60,
    "month": 30 * 24 * 60 * 60,
}


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
# COMPRESSED RAM STORAGE
# ============================================================

# C = calibration:
#
#     C + 8 byte timestamp + 2 byte price
#
# D = normal delta:
#
#     D + 2 byte signed price delta
#
# Every 30 minutes we store a calibration.
# A calibration is also stored if we detect a time gap.
#
# The actual recording ALWAYS happens at full resolution.

history = bytearray()

last_price = None
last_timestamp = None
last_calibration = None

data_lock = threading.Lock()


# ============================================================
# PRICE COMPRESSION
# ============================================================

def compress_price(price):
    return round(price / PRICE_DIVISOR)


def decompress_price(price):
    return price * PRICE_DIVISOR


# ============================================================
# STORE PRICE
# ============================================================

def store_price(timestamp, real_price):
    global last_price
    global last_timestamp
    global last_calibration

    price = compress_price(real_price)

    with data_lock:

        # First sample.
        if last_price is None:
            history.extend(b"C")
            history.extend(
                struct.pack("<dH", timestamp, price)
            )

            last_price = price
            last_timestamp = timestamp
            last_calibration = timestamp

            return

        time_gap = timestamp - last_timestamp

        # Recalibrate every 30 minutes or after a gap.
        if (
            timestamp - last_calibration >= CALIBRATION_INTERVAL
            or time_gap > SAMPLE_INTERVAL * 1.5
        ):
            history.extend(b"C")
            history.extend(
                struct.pack("<dH", timestamp, price)
            )

            last_calibration = timestamp

        else:
            delta = price - last_price

            history.extend(b"D")
            history.extend(
                struct.pack("<h", delta)
            )

        last_price = price
        last_timestamp = timestamp


# ============================================================
# DECODE HISTORY
# ============================================================

def decode_history(start_time=None, granularity=1):
    """
    Decode the compressed history.

    The recording remains at 1-second resolution, but only
    every `granularity` seconds is returned to the browser.

    Example:
        granularity=1     -> every second
        granularity=10    -> every 10 seconds
        granularity=60    -> every minute
        granularity=300   -> every 5 minutes
        granularity=3600  -> every hour
        granularity=86400 -> every day
    """

    with data_lock:

        if not history:
            return []

        position = 0
        timestamp = None
        price = None

        results = []

        # Used so we only send points at the requested interval.
        last_sent_timestamp = None

        while position < len(history):

            record_type = history[position:position + 1]
            position += 1

            # ------------------------------------------------
            # CALIBRATION
            # ------------------------------------------------

            if record_type == b"C":

                if position + 10 > len(history):
                    break

                timestamp, price = struct.unpack(
                    "<dH",
                    history[position:position + 10]
                )

                position += 10

                # A calibration is an exact known point.
                if (
                    start_time is None
                    or timestamp >= start_time
                ):
                    results.append({
                        "time": timestamp,
                        "price": decompress_price(price)
                    })

                    last_sent_timestamp = timestamp

            # ------------------------------------------------
            # NORMAL DELTA
            # ------------------------------------------------

            elif record_type == b"D":

                if position + 2 > len(history):
                    break

                delta = struct.unpack(
                    "<h",
                    history[position:position + 2]
                )[0]

                position += 2

                price += delta
                timestamp += SAMPLE_INTERVAL

                # Don't bother creating points before the
                # requested range.
                if start_time is not None and timestamp < start_time:
                    continue

                # Send only the requested granularity.
                if (
                    last_sent_timestamp is None
                    or timestamp - last_sent_timestamp >= granularity
                ):
                    results.append({
                        "time": timestamp,
                        "price": decompress_price(price)
                    })

                    last_sent_timestamp = timestamp

            else:
                break

        return results


# ============================================================
# GET ELYTRA PRICE
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

            store_price(
                timestamp,
                price
            )

            with data_lock:
                size = len(history)

            print(
                f"Elytra: {price:,} "
                f"| RAM: {size / 1024 / 1024:.2f} MB"
            )

        except Exception as e:
            print(
                f"Error collecting price: {e}"
            )

        time.sleep(SAMPLE_INTERVAL)


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

        .controls {
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
            justify-content: center;
            margin: 20px 0 30px;
        }

        .control {
            text-align: center;
        }

        .control label {
            display: block;
            margin-bottom: 8px;
            color: #aaa;
            font-size: 14px;
        }

        .buttons {
            display: flex;
            gap: 5px;
            flex-wrap: wrap;
            justify-content: center;
        }

        button {
            background: #222;
            color: white;
            border: 1px solid #444;
            border-radius: 7px;
            padding: 8px 12px;
            cursor: pointer;
        }

        button:hover {
            background: #333;
        }

        button.active {
            background: #00a866;
            border-color: #00ff88;
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

<div class="controls">

    <div class="control">

        <label>Time Range</label>

        <div class="buttons" id="rangeButtons">

            <button data-range="minute">1 min</button>
            <button data-range="5minutes">5 min</button>
            <button data-range="hour">1 hour</button>
            <button data-range="day">1 day</button>
            <button data-range="week">1 week</button>
            <button data-range="month">1 month</button>
            <button data-range="max">Max</button>

        </div>

    </div>


    <div class="control">

        <label>Granularity</label>

        <div class="buttons" id="granularityButtons">

            <button data-granularity="1">1 sec</button>
            <button data-granularity="10">10 sec</button>
            <button data-granularity="60">1 min</button>
            <button data-granularity="300">5 min</button>
            <button data-granularity="3600">1 hour</button>
            <button data-granularity="86400">1 day</button>

        </div>

    </div>


    <div class="control">

        <label>Tension</label>

        <div class="buttons" id="tensionButtons">

            <button data-tension="0">0</button>
            <button data-tension="0.2">0.2</button>
            <button data-tension="0.4">0.4</button>
            <button data-tension="0.6">0.6</button>
            <button data-tension="0.8">0.8</button>
            <button data-tension="1">1</button>

        </div>

    </div>

</div>


<canvas id="chart"></canvas>


<script>

const ctx =
    document.getElementById("chart");


// ----------------------------------------------------------
// SETTINGS
// ----------------------------------------------------------

const defaultGranularity = {

    minute: 1,
    "5minutes": 1,
    hour: 60,
    day: 300,
    week: 3600,
    month: 86400,
    max: 86400

};


const rangeSeconds = {

    minute: 60,
    "5minutes": 300,
    hour: 3600,
    day: 86400,
    week: 604800,
    month: 2592000

};


let selectedRange = "hour";

let selectedGranularity =
    defaultGranularity[selectedRange];

let selectedTension = 0.4;


// ----------------------------------------------------------
// CHART
// ----------------------------------------------------------

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

            tension: selectedTension,

            pointRadius: 0,

            fill: true

        }]

    },

    options: {

        responsive: true,

        animation: false,

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


// ----------------------------------------------------------
// BUTTON STATE
// ----------------------------------------------------------

function updateButtons() {

    document
        .querySelectorAll("#rangeButtons button")
        .forEach(button => {

            button.classList.toggle(
                "active",
                button.dataset.range === selectedRange
            );

        });


    document
        .querySelectorAll("#granularityButtons button")
        .forEach(button => {

            button.classList.toggle(
                "active",
                Number(button.dataset.granularity)
                === selectedGranularity
            );

        });


    document
        .querySelectorAll("#tensionButtons button")
        .forEach(button => {

            button.classList.toggle(
                "active",
                Number(button.dataset.tension)
                === selectedTension
            );

        });

}


// ----------------------------------------------------------
// GET HISTORY
// ----------------------------------------------------------

async function update() {

    try {

        let url =
            "/history?range="
            + selectedRange
            + "&granularity="
            + selectedGranularity;


        const response =
            await fetch(url);

        const data =
            await response.json();


        chart.data.labels =
            data.map(x =>
                new Date(
                    x.time * 1000
                ).toLocaleString()
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


// ----------------------------------------------------------
// RANGE BUTTONS
// ----------------------------------------------------------

document
    .querySelectorAll("#rangeButtons button")
    .forEach(button => {

        button.addEventListener(
            "click",
            () => {

                selectedRange =
                    button.dataset.range;

                // Automatically choose the sensible
                // granularity for this range.
                selectedGranularity =
                    defaultGranularity[
                        selectedRange
                    ];

                updateButtons();
                update();

            }
        );

    });


// ----------------------------------------------------------
// GRANULARITY BUTTONS
// ----------------------------------------------------------

document
    .querySelectorAll("#granularityButtons button")
    .forEach(button => {

        button.addEventListener(
            "click",
            () => {

                selectedGranularity =
                    Number(
                        button.dataset.granularity
                    );

                updateButtons();
                update();

            }
        );

    });


// ----------------------------------------------------------
// TENSION BUTTONS
// ----------------------------------------------------------

document
    .querySelectorAll("#tensionButtons button")
    .forEach(button => {

        button.addEventListener(
            "click",
            () => {

                selectedTension =
                    Number(
                        button.dataset.tension
                    );

                chart.data.datasets[0].tension =
                    selectedTension;

                chart.update();

                updateButtons();

            }
        );

    });


// ----------------------------------------------------------
// INITIAL LOAD
// ----------------------------------------------------------

updateButtons();

update();


// Refresh the current graph every 5 seconds.
setInterval(update, 5000);

</script>

</body>

</html>
""")


# ============================================================
# HISTORY ENDPOINT
# ============================================================

@app.route("/history")
def history_endpoint():

    selected_range = request.args.get(
        "range",
        "hour"
    )

    selected_granularity = request.args.get(
        "granularity",
        type=int
    )

    # Unknown range → hour.
    if selected_range not in DEFAULT_GRANULARITY:
        selected_range = "hour"

    # If the browser didn't specify granularity,
    # use the sensible default.
    if selected_granularity is None:
        selected_granularity = DEFAULT_GRANULARITY[
            selected_range
        ]

    # Prevent invalid values.
    if selected_granularity not in {
        1,
        10,
        60,
        300,
        3600,
        86400
    }:
        selected_granularity = DEFAULT_GRANULARITY[
            selected_range
        ]

    # Determine the beginning of the requested range.
    if selected_range == "max":
        start_time = None
    else:
        start_time = (
            time.time()
            - TIME_RANGES[selected_range]
        )

    return jsonify(
        decode_history(
            start_time=start_time,
            granularity=selected_granularity
        )
    )


# ============================================================
# STORAGE STATS
# ============================================================

@app.route("/stats")
def stats():

    with data_lock:

        return jsonify({

            "compressed_bytes":
                len(history),

            "compressed_mb":
                round(
                    len(history)
                    / 1024
                    / 1024,
                    3
                ),

            "current_price":
                (
                    None
                    if last_price is None
                    else decompress_price(
                        last_price
                    )
                ),

            "calibration_interval":
                CALIBRATION_INTERVAL,

            "price_divisor":
                PRICE_DIVISOR

        })


# ============================================================
# START
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
