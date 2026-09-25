from flask import Flask, jsonify, render_template_string, request
import requests
import threading
import time
import struct
import statistics
import os
import bisect

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

# Upload the complete current history every 30 minutes.
B2_UPLOAD_INTERVAL = 1800


# ============================================================
# ERROR HANDLING
# ============================================================

@app.errorhandler(Exception)
def handle_error(error):
    app.logger.exception("Unhandled error")
    return jsonify({
        "error": str(error)
    }), 500


# ============================================================
# PRICE API CONFIGURATION
# ============================================================

API_URL = "https://api.donut.auction/v2/tickers/"

SAMPLE_INTERVAL = 1
CALIBRATION_INTERVAL = 1800

# Prices are stored divided by 100,000.
PRICE_DIVISOR = 100_000


# ============================================================
# GRAPH CONFIGURATION
# ============================================================

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


# ============================================================
# HTTP HEADERS
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
    )
}


# ============================================================
# COMPRESSED HISTORY
# ============================================================

# Binary format:
#
# Calibration:
#
#   C
#   8-byte timestamp
#   8-byte absolute compressed price
#
#
# Delta:
#
#   D
#   variable-length signed price delta
#
#
# A calibration resets both timestamp and price.
#
# This allows us to recover from missed samples and also gives
# us anchor points that can be used to avoid decoding the entire
# history for every graph request.


compressed_data = bytearray()


# Sorted by timestamp:
#
# [
#     (timestamp, byte_offset),
#     ...
# ]
#
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

        compressed_data.append(
            (value & 127) | 128
        )

        value >>= 7

    compressed_data.append(value)


def read_delta(data, pos):

    value = 0
    shift = 0

    while True:

        if pos >= len(data):
            raise ValueError(
                "Incomplete delta"
            )

        byte = data[pos]
        pos += 1

        value |= (
            (byte & 127)
            << shift
        )

        if not byte & 128:
            break

        shift += 7

        if shift > 63:
            raise ValueError(
                "Invalid/corrupt delta"
            )

    delta = (
        (value >> 1)
        ^ -(value & 1)
    )

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
        (
            timestamp,
            offset
        )
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
# RESTORE STATE / BUILD CALIBRATION INDEX
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

        print(
            "B2 history is empty."
        )

        return

    print(
        "Scanning history and rebuilding index..."
    )

    pos = 0

    timestamp = None
    price = None

    latest_calibration = None

    records = 0

    data_length = len(compressed_data)

    while pos < data_length:

        record_offset = pos

        record = compressed_data[pos]
        pos += 1

        # ----------------------------------------------------
        # CALIBRATION
        # ----------------------------------------------------

        if record == ord("C"):

            if pos + 16 > data_length:

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

        # ----------------------------------------------------
        # DELTA
        # ----------------------------------------------------

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
    global current_price
    global current_timestamp
    global last_calibration

    print(
        f"Checking B2 for {B2_HISTORY_FILENAME}..."
    )

    temp_filename = "/tmp/price_history.bin"

    try:

        downloaded = b2_bucket.download_file_by_name(
            B2_HISTORY_FILENAME
        )

        # b2sdk expects save_to() to receive a FILE PATH,
        # not an open file object.
        downloaded.save_to(temp_filename)

        with open(temp_filename, "rb") as f:
            data = f.read()

        if not data:

            print(
                "B2 history file exists but is empty."
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

        error_name = type(e).__name__
        error_text = str(e).lower()

        # B2 file does not exist yet.
        if (
            error_name == "FileNotPresent"
            or "file not present" in error_text
            or "not found" in error_text
            or "404" in error_text
        ):

            print(
                "No existing B2 history found."
            )

            # Create a valid one-record history.
            #
            # Format:
            # C
            # + 8-byte timestamp
            # + 8-byte compressed price
            timestamp = time.time()
            price = 0

            initial_data = bytearray()

            initial_data.extend(b"C")

            initial_data.extend(
                struct.pack(
                    "<dQ",
                    timestamp,
                    price
                )
            )

            with data_lock:

                compressed_data.clear()
                compressed_data.extend(
                    initial_data
                )

                calibration_index.clear()

                calibration_index.append(
                    (timestamp, 0)
                )

                current_timestamp = timestamp
                current_price = price
                last_calibration = timestamp

            # Create the initial B2 file immediately.
            b2_bucket.upload_bytes(
                bytes(initial_data),
                B2_HISTORY_FILENAME,
                content_type="application/octet-stream"
            )

            print(
                "Created new price_history.bin "
                "with one initial timestamp."
            )

            return

        print(
            f"B2 history download failed "
            f"({error_name}): {e}"
        )

        raise

# ============================================================
# FIND STARTING OFFSET
# ============================================================

def find_start_offset(start_time):

    """
    Find the calibration immediately before start_time.

    This is the important optimization.

    Previously /history decoded the entire file starting
    from byte 0.

    Now we jump to the nearest calibration before the
    requested time and decode only from there.
    """

    if not calibration_index:

        return 0

    timestamps = [
        item[0]
        for item in calibration_index
    ]

    index = bisect.bisect_right(
        timestamps,
        start_time
    ) - 1

    if index < 0:

        return 0

    return calibration_index[index][1]


# ============================================================
# DECODE HISTORY
# ============================================================

def decode_history(
    start_time=None,
    granularity=1
):

    # --------------------------------------------------------
    # Take a snapshot of the data.
    #
    # This prevents us from holding data_lock while doing
    # potentially expensive decoding.
    # --------------------------------------------------------

    with data_lock:

        if not compressed_data:
            return []

        data = bytes(compressed_data)

        index = list(calibration_index)

    # --------------------------------------------------------
    # Find the closest calibration before the requested time.
    # --------------------------------------------------------

    if start_time is None:

        pos = 0

    elif index:

        timestamps = [
            item[0]
            for item in index
        ]

        calibration_number = (
            bisect.bisect_right(
                timestamps,
                start_time
            ) - 1
        )

        if calibration_number < 0:

            pos = 0

        else:

            pos = index[
                calibration_number
            ][1]

    else:

        pos = 0

    # --------------------------------------------------------
    # Decode from the selected calibration.
    # --------------------------------------------------------

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

    data_length = len(data)

    while pos < data_length:

        record = data[pos]
        pos += 1

        # ----------------------------------------------------
        # CALIBRATION
        # ----------------------------------------------------

        if record == ord("C"):

            if pos + 16 > data_length:

                raise ValueError(
                    "Incomplete calibration record"
                )

            timestamp, price = struct.unpack(
                "<dQ",
                data[
                    pos:pos + 16
                ]
            )

            pos += 16

        # ----------------------------------------------------
        # DELTA
        # ----------------------------------------------------

        elif record == ord("D"):

            if (
                timestamp is None
                or price is None
            ):

                raise ValueError(
                    "Delta before calibration"
                )

            delta, pos = read_delta(
                data,
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

        # ----------------------------------------------------
        # Ignore points before requested range.
        # ----------------------------------------------------

        if (
            start_time is not None
            and timestamp < start_time
        ):

            continue

        point = {
            "time": timestamp,
            "price": decompress_price(price)
        }

        # ----------------------------------------------------
        # Start first bucket.
        # ----------------------------------------------------

        if not bucket:

            bucket.append(point)

            continue

        # ----------------------------------------------------
        # Start a new bucket when granularity is reached.
        # ----------------------------------------------------

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

    started = time.time()

    print(
        f"Starting B2 upload: "
        f"{len(data) / 1024 / 1024:.2f} MB"
    )

    b2_bucket.upload_bytes(
        data,
        B2_HISTORY_FILENAME,
        content_type="application/octet-stream"
    )

    elapsed = time.time() - started

    print(
        f"B2 upload complete: "
        f"{len(data) / 1024 / 1024:.2f} MB "
        f"in {elapsed:.1f}s"
    )


# ============================================================
# FETCH ELYTRA PRICE
# ============================================================

def get_elytra_price():

    print("DEBUG: requesting Donut API...")

    response = requests.get(
        API_URL,
        headers=HEADERS,
        timeout=10
    )

    print(
        f"DEBUG: Donut API status = {response.status_code}"
    )

    response.raise_for_status()

    data = response.json()

    print(
        f"DEBUG: API returned {len(data)} items"
    )

    for item in data:

        print(
            f"DEBUG: item={item.get('itemName')} "
            f"price={item.get('unitPrice')} "
            f"stale={item.get('isStale')}"
        )

        if (
            item.get("itemName") == "elytra"
            and not item.get("isStale")
        ):

            return item["unitPrice"]

    raise ValueError(
        "Elytra was not found in the API response "
        "as a non-stale item."
    )
#temprary code here


# ============================================================
# PRICE COLLECTOR
# ============================================================

def collector():

    while True:

        started = time.time()

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
                f"History: "
                f"{size / 1024 / 1024:.2f} MB"
            )

        except Exception as e:

            print(
                f"Error collecting price: "
                f"{type(e).__name__}: {e}"
            )

        elapsed = time.time() - started

        sleep_time = max(
            0,
            SAMPLE_INTERVAL - elapsed
        )

        time.sleep(
            sleep_time
        )


# ============================================================
# B2 UPLOADER
# ============================================================

def b2_uploader():

    # Wait 30 minutes before first upload.
    while True:

        time.sleep(
            B2_UPLOAD_INTERVAL
        )

        try:

            upload_history_to_b2()

        except Exception as e:

            print(
                f"B2 upload failed: "
                f"{type(e).__name__}: {e}"
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


// Prevent multiple history requests
// from running simultaneously.

let updateInProgress = false;

let updateAgain = false;


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

    // If an update is already running,
    // don't start another one.

    if(updateInProgress){

        updateAgain = true;

        return;

    }


    updateInProgress = true;


    try{

        const response =
            await fetch(
                "/history?range="
                + encodeURIComponent(
                    selectedRange
                )
                + "&granularity="
                + encodeURIComponent(
                    selectedGranularity
                ),
                {
                    cache:"no-store"
                }
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
    finally{

        updateInProgress = false;


        if(updateAgain){

            updateAgain = false;

            update();

        }

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

    if (
        selected_range
        not in DEFAULT_GRANULARITY
    ):

        selected_range = "hour"


    granularity = request.args.get(
        "granularity",
        type=int
    )

    if (
        granularity
        not in VALID_GRANULARITIES
    ):

        granularity = (
            DEFAULT_GRANULARITY[
                selected_range
            ]
        )


    if selected_range == "max":

        start_time = None

    else:

        start_time = (
            time.time()
            - TIME_RANGES[
                selected_range
            ]
        )


    started = time.time()


    result = decode_history(
        start_time,
        granularity
    )


    elapsed = time.time() - started


    print(
        f"/history "
        f"range={selected_range} "
        f"granularity={granularity} "
        f"points={len(result):,} "
        f"time={elapsed:.3f}s"
    )


    return jsonify(result)


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
                    else decompress_price(
                        current_price
                    )
                ),

            "current_timestamp":
                current_timestamp

        })


# ============================================================
# STARTUP
# ============================================================

if __name__ == "__main__":

    print(
        "================================================"
    )

    print(
        "Starting Donut Elytra price tracker..."
    )

    print(
        "Loading history from B2..."
    )

    print(
        "================================================"
    )


    # --------------------------------------------------------
    # IMPORTANT:
    #
    # Load old history BEFORE collector starts.
    #
    # This ensures the first new price continues from the
    # restored state rather than creating a broken stream.
    # --------------------------------------------------------

    load_history_from_b2()


    print(
        "History initialization complete."
    )


    # --------------------------------------------------------
    # Start collector.
    # --------------------------------------------------------

    threading.Thread(
        target=collector,
        daemon=True,
        name="price-collector"
    ).start()


    # --------------------------------------------------------
    # Start B2 uploader.
    # --------------------------------------------------------

    threading.Thread(
        target=b2_uploader,
        daemon=True,
        name="b2-uploader"
    ).start()


    print(
        "Collector started."
    )

    print(
        "B2 uploader started."
    )

    print(
        "Web server starting..."
    )


    app.run(
        host="0.0.0.0",
        port=10000,
        threaded=True
    )
