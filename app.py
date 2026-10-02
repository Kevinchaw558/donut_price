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


# ------------------------------------------------------------
# MAIN HISTORY FILE
# ------------------------------------------------------------

B2_HISTORY_FILENAME = "price_history.bin"


# ------------------------------------------------------------
# MAIN HISTORY UPLOAD
#
# The complete history is uploaded every 30 minutes.
# We clean up old B2 versions so this does not create
# hundreds of old versions.
# ------------------------------------------------------------

B2_UPLOAD_INTERVAL = 30 * 60


# ------------------------------------------------------------
# RECOVERY SNAPSHOTS
#
# A snapshot is created every 6 hours.
#
# Snapshots older than 6 hours are deleted.
#
# Therefore there will normally be one snapshot available.
# ------------------------------------------------------------

B2_SNAPSHOT_PREFIX = "snapshots/price_history_"

B2_SNAPSHOT_INTERVAL = 6 * 60 * 60

B2_SNAPSHOT_RETENTION = 6 * 60 * 60


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

compressed_data = bytearray()


# Sorted by timestamp:
#
# [
#     (timestamp, byte_offset),
#     ...
# ]

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

        raise ValueError(
            "History is empty."
        )

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
# DOWNLOAD CURRENT MAIN HISTORY FROM B2
# ============================================================

def load_current_history_from_b2():

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

        downloaded.save_to(
            temp_filename
        )

        with open(
            temp_filename,
            "rb"
        ) as f:

            data = f.read()


        if not data:

            raise ValueError(
                "B2 history file is empty."
            )


        with data_lock:

            compressed_data.clear()

            compressed_data.extend(
                data
            )

            # This validates the entire file.
            rebuild_state_from_history()


        print(
            f"B2 history loaded successfully: "
            f"{len(data) / 1024 / 1024:.2f} MB"
        )

        return True


    except Exception as e:

        print(
            f"Current B2 history could not be loaded: "
            f"{type(e).__name__}: {e}"
        )

        return False


# ============================================================
# CREATE NEW EMPTY HISTORY
# ============================================================

def create_new_history():

    global compressed_data
    global current_price
    global current_timestamp
    global last_calibration

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
            (
                timestamp,
                0
            )
        )

        current_timestamp = timestamp

        current_price = price

        last_calibration = timestamp


    print(
        "Created new local history."
    )


    try:

        b2_bucket.upload_bytes(
            bytes(initial_data),
            B2_HISTORY_FILENAME,
            content_type="application/octet-stream"
        )

        print(
            "Created new price_history.bin on B2."
        )

    except Exception as e:

        print(
            f"Warning: could not create initial B2 "
            f"history: {e}"
        )


# ============================================================
# LOAD SNAPSHOT
# ============================================================

def load_snapshot_from_b2(snapshot):

    global compressed_data
    global current_price
    global current_timestamp
    global last_calibration

    temp_filename = (
        "/tmp/price_history_snapshot.bin"
    )

    print(
        f"Downloading recovery snapshot: "
        f"{snapshot.file_name}"
    )

    downloaded = (
        b2_bucket.download_file_by_id(
            snapshot.id_
        )
    )

    downloaded.save_to(
        temp_filename
    )

    with open(
        temp_filename,
        "rb"
    ) as f:

        data = f.read()


    if not data:

        raise ValueError(
            "Snapshot is empty."
        )


    with data_lock:

        compressed_data.clear()

        compressed_data.extend(
            data
        )

        # Validate the snapshot.
        rebuild_state_from_history()


    print(
        f"Recovery snapshot loaded successfully: "
        f"{len(data) / 1024 / 1024:.2f} MB"
    )


# ============================================================
# FIND NEWEST SNAPSHOT
# ============================================================

def get_snapshots():

    snapshots = []

    try:

        for version, _folder in b2_bucket.ls(
            "snapshots/",
            latest_only=True,
            recursive=True
        ):

            if (
                version.action == "upload"
                and version.file_name.startswith(
                    B2_SNAPSHOT_PREFIX
                )
                and version.file_name.endswith(
                    ".bin"
                )
            ):

                snapshots.append(version)

    except Exception as e:

        print(
            f"Could not list B2 snapshots: {e}"
        )

        return []


    snapshots.sort(
        key=lambda x: x.upload_timestamp or 0,
        reverse=True
    )

    return snapshots


# ============================================================
# RECOVER FROM NEWEST SNAPSHOT
# ============================================================

def load_latest_snapshot_from_b2():

    snapshots = get_snapshots()

    if not snapshots:

        print(
            "No recovery snapshots found."
        )

        return False


    newest = snapshots[0]

    try:

        load_snapshot_from_b2(
            newest
        )

        print(
            f"Recovered from snapshot: "
            f"{newest.file_name}"
        )

        return True

    except Exception as e:

        print(
            f"Newest snapshot failed validation: "
            f"{type(e).__name__}: {e}"
        )


    # If the newest snapshot is corrupt, try older
    # snapshots rather than giving up immediately.

    for snapshot in snapshots[1:]:

        try:

            print(
                f"Trying older snapshot: "
                f"{snapshot.file_name}"
            )

            load_snapshot_from_b2(
                snapshot
            )

            print(
                f"Recovered from snapshot: "
                f"{snapshot.file_name}"
            )

            return True

        except Exception as e:

            print(
                f"Snapshot failed: "
                f"{type(e).__name__}: {e}"
            )


    print(
        "No usable recovery snapshots found."
    )

    return False


# ============================================================
# STARTUP RECOVERY
# ============================================================

def load_history_with_recovery():

    print(
        "================================================"
    )

    print(
        "Loading history..."
    )

    print(
        "================================================"
    )


    # --------------------------------------------------------
    # FIRST: Try the normal main history.
    # --------------------------------------------------------

    if load_current_history_from_b2():

        print(
            "Main history is healthy."
        )

        return


    # --------------------------------------------------------
    # SECOND: Main history failed.
    #
    # Try the newest recovery snapshot.
    # --------------------------------------------------------

    print(
        "Main history is unavailable or corrupt."
    )

    print(
        "Attempting recovery from newest snapshot..."
    )


    if load_latest_snapshot_from_b2():

        print(
            "================================================"
        )

        print(
            "RECOVERY SUCCESSFUL"
        )

        print(
            "================================================"
        )


        # ----------------------------------------------------
        # Promote recovered history back to the main file.
        # ----------------------------------------------------

        try:

            upload_history_to_b2()

            print(
                "Recovered history uploaded as "
                "the new main history."
            )

        except Exception as e:

            print(
                f"WARNING: recovered history could not "
                f"be uploaded as main history: {e}"
            )


        return


    # --------------------------------------------------------
    # THIRD: Nothing exists.
    #
    # Start a brand-new history.
    # --------------------------------------------------------

    print(
        "No usable main history or snapshots found."
    )

    print(
        "Starting a new history."
    )

    create_new_history()


# ============================================================
# CLEAN OLD MAIN FILE VERSIONS
# ============================================================

def cleanup_old_history_versions():

    print(
        f"Cleaning old B2 versions of "
        f"{B2_HISTORY_FILENAME}..."
    )


    try:

        versions = list(
            b2_bucket.list_file_versions(
                B2_HISTORY_FILENAME
            )
        )

    except Exception as e:

        print(
            f"Could not list old history versions: {e}"
        )

        return


    if len(versions) <= 1:

        print(
            "No old main-history versions to delete."
        )

        return


    # --------------------------------------------------------
    # Sort newest first.
    # --------------------------------------------------------

    versions.sort(
        key=lambda x: x.upload_timestamp or 0,
        reverse=True
    )


    newest = versions[0]

    deleted = 0


    # --------------------------------------------------------
    # Keep only the newest main-history version.
    # --------------------------------------------------------

    for version in versions[1:]:

        try:

            b2_bucket.delete_file_version(
                version.id_,
                version.file_name
            )

            deleted += 1

        except Exception as e:

            print(
                f"Could not delete old version "
                f"{version.id_}: {e}"
            )


    print(
        f"Deleted {deleted} old main-history versions."
    )

    print(
        f"Kept current version: {newest.id_}"
    )


# ============================================================
# CREATE 6-HOUR RECOVERY SNAPSHOT
# ============================================================

def create_history_snapshot():

    with data_lock:

        data = bytes(
            compressed_data
        )


    if not data:

        print(
            "Snapshot skipped: history is empty."
        )

        return


    now = time.time()


    snapshot_name = (

        B2_SNAPSHOT_PREFIX

        +

        time.strftime(
            "%Y%m%d_%H%M%S",
            time.gmtime(now)
        )

        +

        ".bin"

    )


    started = time.time()


    print(
        f"Creating recovery snapshot: "
        f"{snapshot_name}"
    )


    b2_bucket.upload_bytes(
        data,
        snapshot_name,
        content_type="application/octet-stream"
    )


    elapsed = time.time() - started


    print(
        f"Snapshot upload complete: "
        f"{len(data) / 1024 / 1024:.2f} MB "
        f"in {elapsed:.1f}s"
    )


    # --------------------------------------------------------
    # Immediately clean snapshots older than 6 hours.
    # --------------------------------------------------------

    cleanup_old_snapshots()


# ============================================================
# DELETE SNAPSHOTS OLDER THAN 6 HOURS
# ============================================================

def cleanup_old_snapshots():

    snapshots = get_snapshots()

    if not snapshots:

        return


    cutoff = (
        time.time()
        - B2_SNAPSHOT_RETENTION
    )


    deleted = 0


    for snapshot in snapshots:

        upload_timestamp = (
            snapshot.upload_timestamp
            or 0
        )


        # ----------------------------------------------------
        # Keep snapshots from the last 6 hours.
        # ----------------------------------------------------

        if upload_timestamp >= cutoff:

            continue


        try:

            b2_bucket.delete_file_version(
                snapshot.id_,
                snapshot.file_name
            )

            deleted += 1

            print(
                f"Deleted old snapshot: "
                f"{snapshot.file_name}"
            )

        except Exception as e:

            print(
                f"Could not delete old snapshot "
                f"{snapshot.file_name}: {e}"
            )


    if deleted:

        print(
            f"Deleted {deleted} snapshot(s) "
            f"older than 6 hours."
        )


# ============================================================
# FIND STARTING OFFSET
# ============================================================

def find_start_offset(start_time):

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

    with data_lock:

        if not compressed_data:

            return []


        data = bytes(
            compressed_data
        )


        index = list(
            calibration_index
        )


    # --------------------------------------------------------
    # Find closest calibration.
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
            )

            - 1

        )


        if calibration_number < 0:

            pos = 0

        else:

            pos = index[
                calibration_number
            ][1]

    else:

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

            "time":
                bucket[-1]["time"],

            "price":
                bucket[-1]["price"],

            "min":
                min(values),

            "max":
                max(values),

            "median":
                statistics.median(values)

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

            "time":
                timestamp,

            "price":
                decompress_price(price)

        }


        # ----------------------------------------------------
        # Start first bucket.
        # ----------------------------------------------------

        if not bucket:

            bucket.append(point)

            continue


        # ----------------------------------------------------
        # Start a new bucket.
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
# UPLOAD MAIN HISTORY TO B2
# ============================================================

def upload_history_to_b2():

    with data_lock:

        data = bytes(
            compressed_data
        )


    if not data:

        print(
            "B2 upload skipped: "
            "no history yet."
        )

        return


    started = time.time()


    print(
        f"Starting B2 main-history upload: "
        f"{len(data) / 1024 / 1024:.2f} MB"
    )


    b2_bucket.upload_bytes(
        data,
        B2_HISTORY_FILENAME,
        content_type="application/octet-stream"
    )


    elapsed = time.time() - started


    print(
        f"B2 main-history upload complete: "
        f"{len(data) / 1024 / 1024:.2f} MB "
        f"in {elapsed:.1f}s"
    )


# ============================================================
# FETCH ELYTRA PRICE
# ============================================================

def get_elytra_price():

    print(
        "DEBUG: requesting Donut API..."
    )


    response = requests.get(
        API_URL,
        headers=HEADERS,
        timeout=10
    )


    print(
        f"DEBUG: Donut API status = "
        f"{response.status_code}"
    )


    response.raise_for_status()


    data = response.json()


    print(
        f"DEBUG: API returned "
        f"{len(data)} items"
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

                size = len(
                    compressed_data
                )


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


        elapsed = (
            time.time()
            - started
        )


        sleep_time = max(
            0,
            SAMPLE_INTERVAL - elapsed
        )


        time.sleep(
            sleep_time
        )


# ============================================================
# B2 MAIN HISTORY UPLOADER
# ============================================================

def b2_uploader():

    while True:

        time.sleep(
            B2_UPLOAD_INTERVAL
        )


        try:

            upload_history_to_b2()


            # ------------------------------------------------
            # Keep only one version of the main file.
            # ------------------------------------------------

            cleanup_old_history_versions()


        except Exception as e:

            print(
                f"B2 upload failed: "
                f"{type(e).__name__}: {e}"
            )


# ============================================================
# B2 6-HOUR SNAPSHOTTER
# ============================================================

def b2_snapshotter():

    while True:

        time.sleep(
            B2_SNAPSHOT_INTERVAL
        )


        try:

            create_history_snapshot()

        except Exception as e:

            print(
                f"6-hour snapshot failed: "
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

        } else {

            document
                .getElementById("price")
                .textContent =
                "No fresh data";

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

            -

            TIME_RANGES[
                selected_range
            ]

        )


    started = time.time()


    result = decode_history(
        start_time,
        granularity
    )


    elapsed = (
        time.time()
        - started
    )


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
        "================================================"
    )


    # --------------------------------------------------------
    # Load main history.
    #
    # If it is broken/missing, automatically use the newest
    # recovery snapshot.
    # --------------------------------------------------------

    load_history_with_recovery()


    print(
        "History initialization complete."
    )


    # --------------------------------------------------------
    # IMPORTANT:
    #
    # Clean the old 372+ versions created by the previous
    # system.
    #
    # This keeps ONLY the newest version of the main file.
    # --------------------------------------------------------

    cleanup_old_history_versions()


    # --------------------------------------------------------
    # Clean old recovery snapshots too.
    #
    # This is especially useful when the server starts after
    # being offline for a while.
    # --------------------------------------------------------

    cleanup_old_snapshots()


    # --------------------------------------------------------
    # Start price collector.
    # --------------------------------------------------------

    threading.Thread(
        target=collector,
        daemon=True,
        name="price-collector"
    ).start()


    # --------------------------------------------------------
    # Start main B2 uploader.
    # --------------------------------------------------------

    threading.Thread(
        target=b2_uploader,
        daemon=True,
        name="b2-uploader"
    ).start()


    # --------------------------------------------------------
    # Start 6-hour snapshotter.
    # --------------------------------------------------------

    threading.Thread(
        target=b2_snapshotter,
        daemon=True,
        name="b2-snapshotter"
    ).start()


    print(
        "Collector started."
    )

    print(
        "B2 uploader started."
    )

    print(
        "6-hour snapshotter started."
    )

    print(
        "Web server starting..."
    )


    app.run(
    host="0.0.0.0",
    port=int(os.environ.get("PORT", 10000)),
    threaded=True
)

