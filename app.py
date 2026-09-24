from flask import Flask, jsonify, render_template_string, request
import requests
import threading
import time
import struct
import statistics
import os

from b2sdk.v2 import InMemoryAccountInfo, B2Api


app = Flask(__name__)


# ============================================================
# BACKBLAZE B2 CONFIGURATION
# ============================================================

B2_APPLICATION_KEY_ID = os.environ["B2_APPLICATION_KEY_ID"]
B2_APPLICATION_KEY = os.environ["B2_APPLICATION_KEY"]
B2_BUCKET_NAME = os.environ["B2_BUCKET_NAME"]

b2_info = InMemoryAccountInfo()
b2_api = B2Api(b2_info)

b2_api.authorize_account(
    "production",
    B2_APPLICATION_KEY_ID,
    B2_APPLICATION_KEY
)

b2_bucket = b2_api.get_bucket_by_name(
    B2_BUCKET_NAME
)

B2_HISTORY_FILENAME = "price_history.bin"
B2_UPLOAD_INTERVAL = 1800  # 30 minutes


# ============================================================
# ERROR HANDLING
# ============================================================

@app.errorhandler(Exception)
def handle_error(error):
    app.logger.exception("Unhandled error")
    return jsonify({"error": str(error)}), 500


# ============================================================
# PRICE API CONFIGURATION
# ============================================================

API_URL = "https://api.donut.auction/v2/tickers/"

SAMPLE_INTERVAL = 1
CALIBRATION_INTERVAL = 1800
PRICE_DIVISOR = 100_000


DEFAULT_GRANULARITY = {
    "minute": 1,
    "5minutes": 1,
    "hour": 60,
    "day": 300,
    "week": 3600,
    "month": 86400,
    "max": 86400
}


TIME_RANGES = {
    "minute": 60,
    "5minutes": 300,
    "hour": 3600,
    "day": 86400,
    "week": 604800,
    "month": 2592000
}


VALID_GRANULARITIES = {
    1,
    10,
    60,
    300,
    3600,
    86400
}


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
    )
}


# ============================================================
# COMPRESSED HISTORY
# ============================================================

# Binary format:
#
# C + 8-byte timestamp + 8-byte absolute price
# D + variable-length signed price delta
#
# Calibration records reset the timestamp and price.
# This makes the stream resilient to missed samples.

compressed_data = bytearray()

# (timestamp, byte_offset)
calibration_index = []

current_price = None
current_timestamp = None
last_calibration = None

data_lock = threading.RLock()


# ============================================================
# PRICE COMPRESSION
# ============================================================

def compress_price(price):
    return int(price) // PRICE_DIVISOR


def decompress_price(price):
    return price * PRICE_DIVISOR


# ============================================================
# VARIABLE-LENGTH SIGNED INTEGER
# ============================================================

def write_delta(delta):
    compressed_data.append(ord("D"))

    # ZigZag encoding.
    value = (delta << 1) ^ (delta >> 63)

    while value >= 128:
        compressed_data.append((value & 127) | 128)
        value >>= 7

    compressed_data.append(value)


def read_delta(data, pos):
    value = 0
    shift = 0

    while True:

        if pos >= len(data):
            raise ValueError("Incomplete delta")

        byte = data[pos]
        pos += 1

        value |= (byte & 127) << shift

        if not byte & 128:
            break

        shift += 7

        if shift > 63:
            raise ValueError(
                "Invalid/corrupt delta"
            )

    delta = (value >> 1) ^ -(value & 1)

    return delta, pos


# ============================================================
# CALIBRATION RECORD
# ============================================================

def write_calibration(timestamp, price):

    offset = len(compressed_data)

    compressed_data.extend(b"C")

    compressed_data.extend(
        struct.pack(
            "<dQ",
            timestamp,
            price
        )
    )

    calibration_index.append(
        (timestamp, offset)
    )


# ============================================================
# ADD PRICE
# ============================================================

def add_price(timestamp, real_price):

    global current_price
    global current_timestamp
    global last_calibration

    price = compress_price(real_price)

    if price < 0:
        raise ValueError(
            "Price cannot be negative"
        )

    with data_lock:

        if current_price is None:

            write_calibration(
                timestamp,
                price
            )

            last_calibration = timestamp

        elif (
            timestamp - last_calibration
            >= CALIBRATION_INTERVAL

            or

            timestamp - current_timestamp
            > SAMPLE_INTERVAL * 1.5
        ):

            write_calibration(
                timestamp,
                price
            )

            last_calibration = timestamp

        else:

            write_delta(
                price - current_price
            )

        current_price = price
        current_timestamp = timestamp


# ============================================================
# RESTORE STATE FROM HISTORY
# ============================================================

def rebuild_state_from_history():

    global current_price
    global current_timestamp
    global last_calibration

    calibration_index.clear()

    if not compressed_data:

        current_price = None
        current_timestamp = None
        last_calibration = None

        print("B2 history is empty.")

        return

    pos = 0

    timestamp = None
    price = None

    latest_calibration = None

    records = 0

    while pos < len(compressed_data):

        record_offset = pos

        record = compressed_data[pos]
        pos += 1

        if record == ord("C"):

            if pos + 16 > len(compressed_data):
                raise ValueError(
                    "Incomplete calibration record"
                )

            timestamp, price = struct.unpack(
                "<dQ",
                compressed_data[
                    pos:pos + 16
                ]
            )

            pos += 16

            calibration_index.append(
                (
                    timestamp,
                    record_offset
                )
            )

            latest_calibration = timestamp

            records += 1

        elif record == ord("D"):

            if (
                timestamp is None
                or price is None
            ):
                raise ValueError(
                    "Delta before calibration"
                )

            delta, pos = read_delta(
                compressed_data,
                pos
            )

            price += delta
            timestamp += SAMPLE_INTERVAL

            if (
                price < 0
                or price > 0xFFFFFFFFFFFFFFFF
            ):
                raise ValueError(
                    f"Invalid decoded price: {price}"
                )

            records += 1

        else:

            raise ValueError(
                f"Unknown record marker: {record}"
            )

    if (
        timestamp is None
        or price is None
    ):
        raise ValueError(
            "History contains no usable data"
        )

    current_timestamp = timestamp
    current_price = price
    last_calibration = latest_calibration

    print(
        f"History restored: "
        f"{len(compressed_data) / 1024 / 1024:.2f} MB, "
        f"{records:,} records, "
        f"{len(calibration_index):,} calibrations"
    )

    print(
        f"Latest restored price: "
        f"{decompress_price(current_price):,}"
    )

    print(
        f"Latest restored timestamp: "
        f"{current_timestamp}"
    )


# ============================================================
# DOWNLOAD HISTORY FROM B2
# ============================================================

def load_history_from_b2():

    global compressed_data

    try:

        print(
            f"Checking B2 for "
            f"{B2_HISTORY_FILENAME}..."
        )

        downloaded = (
            b2_bucket.download_file_by_name(
                B2_HISTORY_FILENAME
            )
        )

        data = downloaded.response.read()

        if not data:

            print(
                "B2 history file exists "
                "but is empty."
            )

            return

        with data_lock:

            compressed_data.clear()
            compressed_data.extend(data)

            rebuild_state_from_history()

        print(
            f"B2 history loaded successfully: "
            f"{len(data) / 1024 / 1024:.2f} MB"
        )

    except Exception as e:

        error_text = str(e).lower()

        # Missing file is expected on the first run.
        if (
            "not found" in error_text
            or "404" in error_text
            or "no such file" in error_text
        ):

            print(
                "No existing B2 history found. "
                "Starting a new history."
            )

            return

        print(
            f"B2 history download failed: {e}"
        )

        raise


# ============================================================
# DECODE HISTORY
# ============================================================

def decode_history(
    start_time=None,
    granularity=1
):

    with data_lock:

        if not compressed_data:
            return []

        pos = 0

        timestamp = None
        price = None

        bucket = []
        results = []

        def finish_bucket():

            if not bucket:
                return

            values = [
                x["price"]
                for x in bucket
            ]

            results.append({
                "time": bucket[-1]["time"],
                "price": bucket[-1]["price"],
                "min": min(values),
                "max": max(values),
                "median": statistics.median(values)
            })

            bucket.clear()

        while pos < len(compressed_data):

            record = compressed_data[pos]
            pos += 1

            if record == ord("C"):

                if pos + 16 > len(compressed_data):
                    raise ValueError(
                        "Incomplete calibration record"
                    )

                timestamp, price = struct.unpack(
                    "<dQ",
                    compressed_data[
                        pos:pos + 16
                    ]
                )

                pos += 16

            elif record == ord("D"):

                if (
                    timestamp is None
                    or price is None
                ):
                    raise ValueError(
                        "Delta before calibration"
                    )

                delta, pos = read_delta(
                    compressed_data,
                    pos
                )

                price += delta
                timestamp += SAMPLE_INTERVAL

                if (
                    price < 0
                    or price > 0xFFFFFFFFFFFFFFFF
                ):
                    raise ValueError(
                        f"Invalid decoded price: {price}"
                    )

            else:

                raise ValueError(
                    f"Unknown record marker: {record}"
                )

            if (
                start_time is not None
                and timestamp < start_time
            ):
                continue

            point = {
                "time": timestamp,
                "price": decompress_price(price)
            }

            if not bucket:

                bucket.append(point)

                continue

            if (
                timestamp
                - bucket[0]["time"]
                >= granularity
            ):

                finish_bucket()

            bucket.append(point)

        finish_bucket()

        return results


# ============================================================
# UPLOAD HISTORY TO B2
# ============================================================

def upload_history_to_b2():

    with data_lock:

        data = bytes(compressed_data)

    if not data:

        print(
            "B2 upload skipped: "
            "no history yet."
        )

        return

    b2_bucket.upload_bytes(
        data,
        B2_HISTORY_FILENAME,
        content_type="application/octet-stream"
    )

    print(
        f"B2 upload complete: "
        f"{len(data) / 1024 / 1024:.2f} MB"
    )


# ============================================================
# FETCH ELYTRA PRICE
# ============================================================

def get_elytra_price():

    response = requests.get(
        API_URL,
        headers=HEADERS,
        timeout=10
    )

    response.raise_for_status()

    return next(
        item["unitPrice"]
        for item in response.json()
        if (
            item["itemName"] == "elytra"
            and not item["isStale"]
        )
    )


# ============================================================
# PRICE COLLECTOR
# ============================================================

def collector():

    while True:

        try:

            price = get_elytra_price()

            add_price(
                time.time(),
                price
            )

            with data_lock:
                size = len(compressed_data)

            print(
                f"Elytra: {price:,} | "
                f"RAM: "
                f"{size / 1024 / 1024:.2f} MB"
            )

        except Exception as e:

            print(
                f"Error collecting price: {e}"
            )

        time.sleep(
            SAMPLE_INTERVAL
        )


# ============================================================
# B2 UPLOADER
# ============================================================

def b2_uploader():

    # Wait 30 minutes before the first upload.
    while True:

        time.sleep(
            B2_UPLOAD_INTERVAL
        )

        try:

            upload_history_to_b2()

        except Exception as e:

            print(
                f"B2 upload failed: {e}"
            )


# ============================================================
# WEBSITE
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

body{
    background:#111;
    color:white;
    font-family:Arial,sans-serif;
    max-width:1000px;
    margin:40px auto;
    padding:20px
}

h1{
    text-align:center
}

#price{
    text-align:center;
    font-size:40px;
    font-weight:bold;
    margin:20px
}

.controls{
    display:flex;
    flex-wrap:wrap;
    gap:20px;
    justify-content:center;
    margin:20px 0
}

.control{
    text-align:center
}

.control label{
    display:block;
    margin-bottom:8px;
    color:#aaa;
    font-size:14px
}

.buttons{
    display:flex;
    gap:5px;
    flex-wrap:wrap;
    justify-content:center
}

button{
    background:#222;
    color:white;
    border:1px solid #444;
    border-radius:7px;
    padding:8px 12px;
    cursor:pointer
}

button:hover{
    background:#333
}

button.active{
    background:#00a866;
    border-color:#00ff88
}

#stats{
    background:#181818;
    border-radius:10px;
    padding:12px 16px;
    margin-bottom:15px;
    display:flex;
    justify-content:center;
    flex-wrap:wrap;
    gap:25px;
    color:#ccc
}

.stat strong{
    color:white
}

canvas{
    background:#181818;
    border-radius:12px;
    padding:10px
}

</style>

</head>

<body>

<h1>Donut SMP Elytra Price</h1>

<div id="price">
    Loading...
</div>


<div class="controls">


<div class="control">

<label>Time Range</label>

<div class="buttons" id="rangeButtons">

<button data-range="minute">
1 min
</button>

<button data-range="5minutes">
5 min
</button>

<button data-range="hour">
1 hour
</button>

<button data-range="day">
1 day
</button>

<button data-range="week">
1 week
</button>

<button data-range="month">
1 month
</button>

<button data-range="max">
Max
</button>

</div>

</div>


<div class="control">

<label>Granularity</label>

<div class="buttons" id="granularityButtons">

<button data-granularity="1">
1 sec
</button>

<button data-granularity="10">
10 sec
</button>

<button data-granularity="60">
1 min
</button>

<button data-granularity="300">
5 min
</button>

<button data-granularity="3600">
1 hour
</button>

<button data-granularity="86400">
1 day
</button>

</div>

</div>


<div class="control">

<label>Smoothness</label>

<div class="buttons" id="smoothnessButtons">

<button data-smoothness="0">
Off
</button>

<button data-smoothness="1">
Low
</button>

<button data-smoothness="2">
Medium
</button>

<button data-smoothness="3">
High
</button>

<button data-smoothness="4">
Very High
</button>

</div>

</div>

</div>


<div id="stats">

<div class="stat">
Time:
<strong id="hoverTime">
Move over graph
</strong>
</div>

<div class="stat">
Price:
<strong id="hoverPrice">
—
</strong>
</div>

<div class="stat">
Min:
<strong id="statMin">
—
</strong>
</div>

<div class="stat">
Max:
<strong id="statMax">
—
</strong>
</div>

<div class="stat">
Median:
<strong id="statMedian">
—
</strong>
</div>

</div>


<canvas id="chart"></canvas>


<script>

const ctx =
    document.getElementById("chart");


const defaultGranularity = {

    minute:1,

    "5minutes":1,

    hour:60,

    day:300,

    week:3600,

    month:86400,

    max:86400

};


let selectedRange = "hour";

let selectedGranularity =
    defaultGranularity[selectedRange];

let selectedSmoothness = 0;

let chartHistory = [];


function smoothData(data, level){

    if(
        level === 0
        || data.length < 3
    ){

        return data.map(
            x => x.price
        );

    }


    const radius = level * 3;


    return data.map((point, i) => {

        const start =
            Math.max(0, i - radius);

        const end =
            Math.min(
                data.length - 1,
                i + radius
            );


        let total = 0;


        for(
            let j = start;
            j <= end;
            j++
        ){

            total += data[j].price;

        }


        return total /
            (end - start + 1);

    });

}


const chart = new Chart(
    ctx,
    {

        type:"line",

        data:{

            labels:[],

            datasets:[{

                label:"Elytra Price",

                data:[],

                borderColor:"#00ff88",

                backgroundColor:
                    "rgba(0,255,136,0.1)",

                borderWidth:2,

                tension:0.15,

                pointRadius:0,

                fill:true

            }]

        },


        options:{

            responsive:true,

            animation:false,


            interaction:{

                mode:"index",

                intersect:false

            },


            plugins:{

                tooltip:{
                    enabled:false
                }

            },


            scales:{

                x:{

                    title:{

                        display:true,

                        text:"Time"

                    }

                },


                y:{

                    title:{

                        display:true,

                        text:"Price"

                    },


                    ticks:{

                        callback:value =>
                            Number(value)
                                .toLocaleString()

                    }

                }

            }

        }

    }
);


function updateButtons(){

    document
        .querySelectorAll(
            "#rangeButtons button"
        )
        .forEach(button => {

            button.classList.toggle(
                "active",

                button.dataset.range
                === selectedRange
            );

        });


    document
        .querySelectorAll(
            "#granularityButtons button"
        )
        .forEach(button => {

            button.classList.toggle(
                "active",

                Number(
                    button.dataset.granularity
                )
                === selectedGranularity
            );

        });


    document
        .querySelectorAll(
            "#smoothnessButtons button"
        )
        .forEach(button => {

            button.classList.toggle(
                "active",

                Number(
                    button.dataset.smoothness
                )
                === selectedSmoothness
            );

        });

}


function showStats(point){

    if(!point)
        return;


    document
        .getElementById("hoverTime")
        .textContent =
        new Date(
            point.time * 1000
        ).toLocaleString();


    document
        .getElementById("hoverPrice")
        .textContent =
        Math.round(
            point.price
        ).toLocaleString()
        + " coins";


    document
        .getElementById("statMin")
        .textContent =
        Math.round(
            point.min
        ).toLocaleString();


    document
        .getElementById("statMax")
        .textContent =
        Math.round(
            point.max
        ).toLocaleString();


    document
        .getElementById("statMedian")
        .textContent =
        Math.round(
            point.median
        ).toLocaleString();

}


ctx.addEventListener(
    "mousemove",
    event => {

        const elements =
            chart.getElementsAtEventForMode(
                event,
                "index",
                {
                    intersect:false
                },
                false
            );


        if(elements.length){

            showStats(
                chartHistory[
                    elements[0].index
                ]
            );

        }

    }
);


async function update(){

    try{

        const response =
            await fetch(
                "/history?range="
                + selectedRange
                + "&granularity="
                + selectedGranularity
            );


        if(!response.ok){

            throw new Error(
                "HTTP " + response.status
            );

        }


        chartHistory =
            await response.json();


        chart.data.labels =
            chartHistory.map(
                x =>
                    new Date(
                        x.time * 1000
                    ).toLocaleString()
            );


        chart.data.datasets[0].data =
            smoothData(
                chartHistory,
                selectedSmoothness
            );


        chart.update();


        if(chartHistory.length){

            const latest =
                chartHistory[
                    chartHistory.length - 1
                ].price;


            document
                .getElementById("price")
                .textContent =
                Math.round(
                    latest
                ).toLocaleString()
                + " coins";

        }

    }
    catch(error){

        console.error(
            "History update failed:",
            error
        );

    }

}


document
    .querySelectorAll(
        "#rangeButtons button"
    )
    .forEach(button => {

        button.addEventListener(
            "click",
            () => {

                selectedRange =
                    button.dataset.range;


                selectedGranularity =
                    defaultGranularity[
                        selectedRange
                    ];


                updateButtons();

                update();

            }
        );

    });


document
    .querySelectorAll(
        "#granularityButtons button"
    )
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


document
    .querySelectorAll(
        "#smoothnessButtons button"
    )
    .forEach(button => {

        button.addEventListener(
            "click",
            () => {

                selectedSmoothness =
                    Number(
                        button.dataset.smoothness
                    );


                chart.data.datasets[0].data =
                    smoothData(
                        chartHistory,
                        selectedSmoothness
                    );


                chart.update();

                updateButtons();

            }
        );

    });


updateButtons();

update();

setInterval(
    update,
    5000
);

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

    if selected_range not in DEFAULT_GRANULARITY:

        selected_range = "hour"


    granularity = request.args.get(
        "granularity",
        type=int
    )

    if granularity not in VALID_GRANULARITIES:

        granularity = DEFAULT_GRANULARITY[
            selected_range
        ]


    if selected_range == "max":

        start_time = None

    else:

        start_time = (
            time.time()
            - TIME_RANGES[selected_range]
        )


    return jsonify(
        decode_history(
            start_time,
            granularity
        )
    )


# ============================================================
# STATS ENDPOINT
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
                    / 1024 / 1024,
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
                    else decompress_price(
                        current_price
                    )
                )

        })


# ============================================================
# STARTUP
# ============================================================

if __name__ == "__main__":

    # Restore previous history BEFORE
    # starting the collector.
    load_history_from_b2()


    threading.Thread(
        target=collector,
        daemon=True
    ).start()


    threading.Thread(
        target=b2_uploader,
        daemon=True
    ).start()


    app.run(
        host="0.0.0.0",
        port=10000
    )
