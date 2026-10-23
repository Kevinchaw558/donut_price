from flask import Flask, jsonify, render_template_string, request
import requests
import threading
import time
import struct
import statistics
import os
import bisect
import re

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

b2_bucket = b2_api.get_bucket_by_name(B2_BUCKET_NAME)


# ============================================================
# MAIN HISTORY FILE
# ============================================================

B2_HISTORY_FILENAME = "price_history.bin"

B2_UPLOAD_INTERVAL = 30 * 60


# ============================================================
# NEW INCREMENTAL BACKUP FOLDER
#
# IMPORTANT:
# This is deliberately different from the previous "backups/"
# folder. Existing backups/ and snapshots/ files are untouched.
# ============================================================

B2_BACKUP_PREFIX = "backups_v2/price_history_"

B2_BACKUP_INTERVAL = 6 * 60 * 60

B2_BACKUP_MAX_SEGMENT_SECONDS = 6 * 60 * 60


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
    1, 10, 60, 300, 3600, 86400
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
# COMPRESSED HISTORY STATE
# ============================================================

compressed_data = bytearray()

# Sorted by timestamp: [(timestamp, byte_offset), ...]
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
            raise ValueError("Invalid/corrupt delta")

    delta = (value >> 1) ^ -(value & 1)

    return delta, pos


# ============================================================
# CALIBRATION RECORD
# ============================================================

def write_calibration(timestamp, price):
    offset = len(compressed_data)

    compressed_data.extend(b"C")
    compressed_data.extend(struct.pack("<dQ", timestamp, price))

    calibration_index.append((timestamp, offset))


# ============================================================
# ADD PRICE
#
# Reject timestamps that are equal to or older than the latest
# stored timestamp. This protects the main history from new
# duplicate or out-of-order samples.
# ============================================================

def add_price(timestamp, real_price):
    global current_price
    global current_timestamp
    global last_calibration

    price = compress_price(real_price)

    if price < 0:
        raise ValueError("Price cannot be negative")

    with data_lock:

        if (
            current_timestamp is not None
            and timestamp <= current_timestamp
        ):
            print(
                "Skipping duplicate or out-of-order timestamp: "
                f"{timestamp} <= {current_timestamp}"
            )
            return False

        if current_price is None:

            write_calibration(timestamp, price)
            last_calibration = timestamp

        elif (
            timestamp - last_calibration >= CALIBRATION_INTERVAL
            or timestamp - current_timestamp > SAMPLE_INTERVAL * 1.5
        ):

            write_calibration(timestamp, price)
            last_calibration = timestamp

        else:

            write_delta(price - current_price)

        current_price = price
        current_timestamp = timestamp

        return True


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
        raise ValueError("History is empty.")

    print("Scanning history and rebuilding index...")

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

        if record == ord("C"):

            if pos + 16 > data_length:
                raise ValueError("Incomplete calibration record")

            timestamp, price = struct.unpack(
                "<dQ",
                compressed_data[pos:pos + 16]
            )

            pos += 16

            calibration_index.append((timestamp, record_offset))
            latest_calibration = timestamp
            records += 1

        elif record == ord("D"):

            if timestamp is None or price is None:
                raise ValueError("Delta before calibration")

            delta, pos = read_delta(compressed_data, pos)

            price += delta
            timestamp += SAMPLE_INTERVAL

            if price < 0 or price > 0xFFFFFFFFFFFFFFFF:
                raise ValueError(f"Invalid decoded price: {price}")

            records += 1

        else:
            raise ValueError(f"Unknown record marker: {record}")

    if timestamp is None or price is None:
        raise ValueError("History contains no usable data")

    current_timestamp = timestamp
    current_price = price
    last_calibration = latest_calibration

    print(
        f"History restored: "
        f"{len(compressed_data) / 1024 / 1024:.2f} MB, "
        f"{records:,} records, "
        f"{len(calibration_index):,} calibrations"
    )

    print(f"Latest restored price: {decompress_price(current_price):,}")
    print(f"Latest restored timestamp: {current_timestamp}")


# ============================================================
# DOWNLOAD MAIN HISTORY FROM B2
# ============================================================

def load_current_history_from_b2():
    global compressed_data

    print(f"Checking B2 for {B2_HISTORY_FILENAME}...")

    temp_filename = "/tmp/price_history.bin"

    try:

        downloaded = b2_bucket.download_file_by_name(
            B2_HISTORY_FILENAME
        )

        downloaded.save_to(temp_filename)

        with open(temp_filename, "rb") as f:
            data = f.read()

        if not data:
            raise ValueError("B2 history file is empty.")

        with data_lock:

            old_data = bytes(compressed_data)

            try:
                compressed_data.clear()
                compressed_data.extend(data)
                rebuild_state_from_history()

            except Exception:
                compressed_data.clear()
                compressed_data.extend(old_data)

                if old_data:
                    rebuild_state_from_history()

                raise

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
    initial_data.extend(struct.pack("<dQ", timestamp, price))

    with data_lock:

        compressed_data.clear()
        compressed_data.extend(initial_data)

        calibration_index.clear()
        calibration_index.append((timestamp, 0))

        current_timestamp = timestamp
        current_price = price
        last_calibration = timestamp

    print("Created new local history.")

    try:

        b2_bucket.upload_bytes(
            bytes(initial_data),
            B2_HISTORY_FILENAME,
            content_type="application/octet-stream"
        )

        print("Created new price_history.bin on B2.")

    except Exception as e:

        print(f"Warning: could not create initial B2 history: {e}")


# ============================================================
# DECODE COMPRESSED HISTORY INTO TIMESTAMPED RECORDS
# ============================================================

def decode_compressed_records(data):
    records = []

    pos = 0
    timestamp = None
    price = None

    data_length = len(data)

    while pos < data_length:

        marker = data[pos]
        pos += 1

        if marker == ord("C"):

            if pos + 16 > data_length:
                raise ValueError("Incomplete calibration record")

            timestamp, price = struct.unpack(
                "<dQ",
                data[pos:pos + 16]
            )

            pos += 16

            records.append((timestamp, price))

        elif marker == ord("D"):

            if timestamp is None or price is None:
                raise ValueError("Delta before calibration")

            delta, pos = read_delta(data, pos)

            price += delta
            timestamp += SAMPLE_INTERVAL

            if price < 0 or price > 0xFFFFFFFFFFFFFFFF:
                raise ValueError(f"Invalid decoded price: {price}")

            records.append((timestamp, price))

        else:
            raise ValueError(f"Unknown record marker: {marker}")

    return records


# ============================================================
# ENCODE AN INDEPENDENT BACKUP SEGMENT
# ============================================================

def encode_backup_segment(records):
    if not records:
        return b""

    output = bytearray()

    previous_timestamp = None
    previous_price = None
    last_calibration_time = None

    for timestamp, price in records:

        must_calibrate = (
            previous_timestamp is None
            or timestamp <= previous_timestamp
            or timestamp - previous_timestamp > SAMPLE_INTERVAL * 1.5
            or last_calibration_time is None
            or timestamp - last_calibration_time >= CALIBRATION_INTERVAL
        )

        if must_calibrate:

            output.append(ord("C"))
            output.extend(struct.pack("<dQ", timestamp, price))

            last_calibration_time = timestamp

        else:

            output.append(ord("D"))

            delta = price - previous_price
            value = (delta << 1) ^ (delta >> 63)

            while value >= 128:
                output.append((value & 127) | 128)
                value >>= 7

            output.append(value)

        previous_timestamp = timestamp
        previous_price = price

    return bytes(output)


# ============================================================
# LIST ONLY THE NEW V2 BACKUP SEGMENTS
#
# This deliberately ignores the old backups/ and snapshots/
# folders. It also ignores files that don't match our naming
# convention.
# ============================================================

def get_backup_segments():

    segments = []

    try:

        for version, _folder in b2_bucket.ls(
            "backups_v2/",
            latest_only=True,
            recursive=True
        ):

            name = version.file_name

            if (
                version.action != "upload"
                or not name.startswith(B2_BACKUP_PREFIX)
                or not name.endswith(".bin")
            ):
                continue

            match = re.search(r"_(\d+)_(\d+)\.bin$", name)

            if not match:
                print(f"WARNING: unrecognized V2 backup filename: {name}")
                continue

            first_ms = int(match.group(1))
            last_ms = int(match.group(2))

            segments.append({
                "version": version,
                "name": name,
                "first_timestamp": first_ms / 1000.0,
                "last_timestamp": last_ms / 1000.0,
            })

    except Exception as e:

        # Do not pretend the archive is empty when B2 listing failed.
        # The caller must know the archive state is unknown.
        raise RuntimeError(
            f"Could not list V2 incremental backups: {e}"
        ) from e

    segments.sort(
        key=lambda item: (
            item["first_timestamp"],
            item["last_timestamp"],
            item["name"]
        )
    )

    return segments


# ============================================================
# UPLOAD NEW HISTORY AS NON-OVERLAPPING SEGMENTS
#
# On the first run, the existing main history is migrated into
# independent <=6-hour segments in backups_v2/.
#
# Subsequent runs only upload records newer than the latest
# timestamp already archived in that folder.
# ============================================================

def archive_new_history_to_b2():

    with data_lock:
        data = bytes(compressed_data)

    if not data:
        print("Incremental backup skipped: history is empty.")
        return

    records = decode_compressed_records(data)

    if not records:
        print("Incremental backup skipped: no decoded records.")
        return

    segments = get_backup_segments()

    # Do not archive into an unknown state if an existing segment
    # has an invalid timestamp or if segment ranges overlap.
    segments.sort(
        key=lambda item: (
            item["first_timestamp"],
            item["last_timestamp"],
            item["name"]
        )
    )

    for i in range(1, len(segments)):

        previous = segments[i - 1]
        current = segments[i]

        if current["first_timestamp"] <= previous["last_timestamp"]:

            # Exact duplicate filenames/ranges should not normally
            # occur. Stop rather than silently advancing a checkpoint
            # across potentially conflicting segment contents.
            if current["name"] != previous["name"]:
                raise ValueError(
                    "Overlapping V2 backup segments detected: "
                    f"{previous['name']} and {current['name']}. "
                    "No new backup was uploaded."
                )

    last_archived_timestamp = (
        max(s["last_timestamp"] for s in segments)
        if segments else None
    )

    if last_archived_timestamp is not None:

        new_records = [
            record for record in records
            if record[0] > last_archived_timestamp
        ]

    else:

        # One-time migration into the new folder.
        # Old backup folders are not consulted.
        new_records = records

    if not new_records:
        print("Incremental backup: no new records to archive.")
        return

    # Split using actual timestamps, not a fixed record count.
    chunks = []
    chunk = []
    chunk_start = None

    for record in new_records:

        timestamp = record[0]

        if (
            chunk
            and chunk_start is not None
            and timestamp - chunk_start >= B2_BACKUP_MAX_SEGMENT_SECONDS
        ):

            chunks.append(chunk)

            chunk = []
            chunk_start = None

        if not chunk:
            chunk_start = timestamp

        chunk.append(record)

    if chunk:
        chunks.append(chunk)

    # Recheck the existing filenames before uploads. Upload chunks
    # chronologically so a failed later upload can safely catch up.
    existing_names = {s["name"] for s in get_backup_segments()}

    for chunk_records in chunks:

        segment_data = encode_backup_segment(chunk_records)

        first_timestamp = chunk_records[0][0]
        last_timestamp = chunk_records[-1][0]

        first_ms = int(round(first_timestamp * 1000))
        last_ms = int(round(last_timestamp * 1000))

        segment_name = (
            f"{B2_BACKUP_PREFIX}{first_ms}_{last_ms}.bin"
        )

        if segment_name in existing_names:

            # Do not assume a matching name proves the segment is
            # valid or identical. Validate the existing object.
            existing = next(
                (
                    s for s in get_backup_segments()
                    if s["name"] == segment_name
                ),
                None
            )

            if existing is not None:

                downloaded = b2_bucket.download_file_by_id(
                    existing["version"].id_
                )

                temp_filename = (
                    "/tmp/price_history_existing_v2_segment.bin"
                )

                downloaded.save_to(temp_filename)

                with open(temp_filename, "rb") as f:
                    existing_data = f.read()

                # Validate existing data before considering it complete.
                existing_records = decode_compressed_records(existing_data)

                if not existing_records:
                    raise ValueError(
                        f"Existing backup segment is empty: {segment_name}"
                    )

                if (
                    existing_records[0][0] != first_timestamp
                    or existing_records[-1][0] != last_timestamp
                ):
                    raise ValueError(
                        "Existing backup filename does not match "
                        f"its contents: {segment_name}"
                    )

                if existing_data != segment_data:
                    raise ValueError(
                        "Existing backup segment differs from the "
                        "expected data. Refusing to overwrite: "
                        f"{segment_name}"
                    )

                print(
                    "Verified existing backup segment; skipping: "
                    f"{segment_name}"
                )

                continue

        started = time.time()

        print(
            f"Uploading incremental backup: {segment_name} "
            f"({len(chunk_records):,} records, "
            f"{len(segment_data) / 1024 / 1024:.2f} MB)"
        )

        b2_bucket.upload_bytes(
            segment_data,
            segment_name,
            content_type="application/octet-stream"
        )

        # Verify that the segment is present and decodable after upload.
        uploaded_segments = get_backup_segments()

        uploaded_segment = next(
            (
                s for s in uploaded_segments
                if s["name"] == segment_name
            ),
            None
        )

        if uploaded_segment is None:
            raise RuntimeError(
                f"Uploaded segment could not be found in B2: {segment_name}"
            )

        print(
            f"Incremental backup uploaded: {segment_name} "
            f"in {time.time() - started:.1f}s"
        )

        existing_names.add(segment_name)


# ============================================================
# RECOVER FULL HISTORY FROM V2 INCREMENTAL BACKUPS
# ============================================================

def load_incremental_backups_from_b2():

    global compressed_data

    segments = get_backup_segments()

    if not segments:
        print("No V2 incremental backup segments found.")
        return False

    combined = bytearray()
    loaded = 0
    previous_last_timestamp = None

    for segment in segments:

        print(f"Downloading backup segment: {segment['name']}")

        downloaded = b2_bucket.download_file_by_id(
            segment["version"].id_
        )

        temp_filename = "/tmp/price_history_v2_segment.bin"

        downloaded.save_to(temp_filename)

        with open(temp_filename, "rb") as f:
            segment_data = f.read()

        if not segment_data:
            raise ValueError(
                f"Backup segment is empty: {segment['name']}"
            )

        decoded = decode_compressed_records(segment_data)

        if not decoded:
            raise ValueError(
                f"Backup segment has no records: {segment['name']}"
            )

        if (
            decoded[0][0] != segment["first_timestamp"]
            or decoded[-1][0] != segment["last_timestamp"]
        ):
            raise ValueError(
                "Backup segment timestamps do not match its filename: "
                f"{segment['name']}"
            )

        if previous_last_timestamp is not None:

            if decoded[0][0] <= previous_last_timestamp:
                raise ValueError(
                    "Overlapping or out-of-order backup segments found: "
                    f"{segment['name']}"
                )

        previous_last_timestamp = decoded[-1][0]

        combined.extend(segment_data)
        loaded += len(decoded)

    with data_lock:

        old_data = bytes(compressed_data)

        try:
            compressed_data.clear()
            compressed_data.extend(combined)
            rebuild_state_from_history()

        except Exception:
            compressed_data.clear()
            compressed_data.extend(old_data)

            if old_data:
                rebuild_state_from_history()

            raise

    print(
        f"Recovered {loaded:,} records from "
        f"{len(segments)} V2 incremental backup segment(s)."
    )

    return True


# ============================================================
# UPLOAD MAIN HISTORY TO B2
# ============================================================

def upload_history_to_b2():

    with data_lock:
        data = bytes(compressed_data)

    if not data:
        print("B2 upload skipped: no history yet.")
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

    print(
        f"B2 main-history upload complete: "
        f"{len(data) / 1024 / 1024:.2f} MB "
        f"in {time.time() - started:.1f}s"
    )


# ============================================================
# CLEAN UP OLD MAIN-HISTORY VERSIONS ONLY
#
# This function only deletes older uploaded versions whose exact
# filename is price_history.bin. It never targets backup folders.
# ============================================================

def cleanup_old_history_versions():

    try:

        versions = []

        for version, _folder in b2_bucket.ls(
            "",
            latest_only=False,
            recursive=True
        ):

            if (
                version.file_name == B2_HISTORY_FILENAME
                and version.action == "upload"
            ):
                versions.append(version)

        if len(versions) <= 1:
            print("Main-history cleanup: nothing to delete.")
            return

        versions.sort(
            key=lambda version: version.upload_timestamp,
            reverse=True
        )

        newest = versions[0]

        print(
            "Main-history cleanup: keeping newest version "
            f"(file ID {newest.id_}); "
            f"removing {len(versions) - 1} older version(s)."
        )

        # Keep the newest version. Delete only older versions of the
        # exact main-history filename.
        for old_version in versions[1:]:

            if old_version.file_name != B2_HISTORY_FILENAME:
                continue

            try:

                b2_api.delete_file_version(
                    old_version.id_,
                    old_version.file_name
                )

                print(
                    "Deleted old main-history version: "
                    f"{old_version.id_}"
                )

            except Exception as e:

                print(
                    "Could not delete old main-history version "
                    f"{old_version.id_}: {e}"
                )

    except Exception as e:

        print(
            "Main-history cleanup failed; no other files are "
            f"intentionally targeted: {type(e).__name__}: {e}"
        )


# ============================================================
# STARTUP RECOVERY
# ============================================================

def load_history_with_recovery():

    print("================================================")
    print("Loading history...")
    print("================================================")

    if load_current_history_from_b2():

        print("Main history is healthy.")
        return

    print("Main history is unavailable or corrupt.")
    print("Attempting recovery from V2 incremental backups...")

    if load_incremental_backups_from_b2():

        print("================================================")
        print("RECOVERY FROM V2 INCREMENTAL BACKUPS SUCCESSFUL")
        print("================================================")

        try:

            upload_history_to_b2()

            print("Recovered history uploaded as the new main history.")

        except Exception as e:

            print(
                "WARNING: recovered history could not be uploaded "
                f"as main history: {e}"
            )

        return

    print("No usable main history or V2 backups found.")
    print("Starting a new history.")
    create_new_history()


# ============================================================
# FIND STARTING OFFSET
# ============================================================

def find_start_offset(start_time):

    if not calibration_index:
        return 0

    timestamps = [item[0] for item in calibration_index]

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

def decode_history(start_time=None, granularity=1):

    with data_lock:

        if not compressed_data:
            return []

        data = bytes(compressed_data)
        index = list(calibration_index)

    if start_time is None:

        pos = 0

    elif index:

        timestamps = [item[0] for item in index]

        calibration_number = (
            bisect.bisect_right(timestamps, start_time) - 1
        )

        if calibration_number < 0:
            pos = 0
        else:
            pos = index[calibration_number][1]

    else:
        pos = 0

    timestamp = None
    price = None

    bucket = []
    results = []

    def finish_bucket():

        if not bucket:
            return

        values = [x["price"] for x in bucket]

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

        if record == ord("C"):

            if pos + 16 > data_length:
                raise ValueError("Incomplete calibration record")

            timestamp, price = struct.unpack(
                "<dQ",
                data[pos:pos + 16]
            )

            pos += 16

        elif record == ord("D"):

            if timestamp is None or price is None:
                raise ValueError("Delta before calibration")

            delta, pos = read_delta(data, pos)

            price += delta
            timestamp += SAMPLE_INTERVAL

            if price < 0 or price > 0xFFFFFFFFFFFFFFFF:
                raise ValueError(f"Invalid decoded price: {price}")

        else:
            raise ValueError(f"Unknown record marker: {record}")

        if start_time is not None and timestamp < start_time:
            continue

        point = {
            "time": timestamp,
            "price": decompress_price(price)
        }

        if not bucket:
            bucket.append(point)
            continue

        if timestamp - bucket[0]["time"] >= granularity:
            finish_bucket()

        bucket.append(point)

    finish_bucket()

    return results


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

    print(f"DEBUG: Donut API status = {response.status_code}")

    response.raise_for_status()

    data = response.json()

    print(f"DEBUG: API returned {len(data)} items")

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

            accepted = add_price(
                time.time(),
                price
            )

            with data_lock:
                size = len(compressed_data)

            if accepted:

                print(
                    f"Elytra: {price:,} | "
                    f"History: {size / 1024 / 1024:.2f} MB"
                )

        except Exception as e:

            print(
                f"Error collecting price: "
                f"{type(e).__name__}: {e}"
            )

        elapsed = time.time() - started

        sleep_time = max(0, SAMPLE_INTERVAL - elapsed)

        time.sleep(sleep_time)


# ============================================================
# B2 MAIN HISTORY UPLOADER
# ============================================================

def b2_uploader():

    while True:

        time.sleep(B2_UPLOAD_INTERVAL)

        try:

            upload_history_to_b2()

            cleanup_old_history_versions()

        except Exception as e:

            print(
                f"B2 upload failed: "
                f"{type(e).__name__}: {e}"
            )


# ============================================================
# B2 SIX-HOUR INCREMENTAL BACKUP WORKER
#
# Runs immediately at startup, then attempts a backup every
# six hours. If a run fails, the next run retries from the
# latest segment successfully present in backups_v2/.
# ============================================================

def b2_backup_worker():

    while True:

        try:

            archive_new_history_to_b2()

        except Exception as e:

            print(
                "Incremental backup failed; source history is "
                "retained for retry: "
                f"{type(e).__name__}: {e}"
            )

        time.sleep(B2_BACKUP_INTERVAL)


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
h1{text-align:center}
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
.control{text-align:center}
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
button:hover{background:#333}
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
.stat strong{color:white}
canvas{
    background:#181818;
    border-radius:12px;
    padding:10px
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
<label>Smoothness</label>
<div class="buttons" id="smoothnessButtons">
<button data-smoothness="0">Off</button>
<button data-smoothness="1">Low</button>
<button data-smoothness="2">Medium</button>
<button data-smoothness="3">High</button>
<button data-smoothness="4">Very High</button>
</div>
</div>

</div>

<div id="stats">
<div class="stat">Time: <strong id="hoverTime">Move over graph</strong></div>
<div class="stat">Price: <strong id="hoverPrice">—</strong></div>
<div class="stat">Min: <strong id="statMin">—</strong></div>
<div class="stat">Max: <strong id="statMax">—</strong></div>
<div class="stat">Median: <strong id="statMedian">—</strong></div>
</div>

<canvas id="chart"></canvas>

<script>
const ctx = document.getElementById("chart");

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
let selectedGranularity = defaultGranularity[selectedRange];
let selectedSmoothness = 0;
let chartHistory = [];
let updateInProgress = false;
let updateAgain = false;

function smoothData(data, level){
    if(level === 0 || data.length < 3){
        return data.map(x => x.price);
    }

    const radius = level * 3;

    return data.map((point, i) => {
        const start = Math.max(0, i - radius);
        const end = Math.min(data.length - 1, i + radius);
        let total = 0;

        for(let j = start; j <= end; j++){
            total += data[j].price;
        }

        return total / (end - start + 1);
    });
}

const chart = new Chart(ctx, {
    type:"line",
    data:{
        labels:[],
        datasets:[{
            label:"Elytra Price",
            data:[],
            borderColor:"#00ff88",
            backgroundColor:"rgba(0,255,136,0.1)",
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
            tooltip:{enabled:false}
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
                    callback:value => Number(value).toLocaleString()
                }
            }
        }
    }
});

function updateButtons(){

    document.querySelectorAll("#rangeButtons button")
        .forEach(button => {
            button.classList.toggle(
                "active",
                button.dataset.range === selectedRange
            );
        });

    document.querySelectorAll("#granularityButtons button")
        .forEach(button => {
            button.classList.toggle(
                "active",
                Number(button.dataset.granularity) === selectedGranularity
            );
        });

    document.querySelectorAll("#smoothnessButtons button")
        .forEach(button => {
            button.classList.toggle(
                "active",
                Number(button.dataset.smoothness) === selectedSmoothness
            );
        });
}

function showStats(point){
    if(!point) return;

    document.getElementById("hoverTime").textContent =
        new Date(point.time * 1000).toLocaleString();

    document.getElementById("hoverPrice").textContent =
        Math.round(point.price).toLocaleString() + " coins";

    document.getElementById("statMin").textContent =
        Math.round(point.min).toLocaleString();

    document.getElementById("statMax").textContent =
        Math.round(point.max).toLocaleString();

    document.getElementById("statMedian").textContent =
        Math.round(point.median).toLocaleString();
}

ctx.addEventListener("mousemove", event => {

    const elements = chart.getElementsAtEventForMode(
        event,
        "index",
        {intersect:false},
        false
    );

    if(elements.length){
        showStats(chartHistory[elements[0].index]);
    }
});

async function update(){

    if(updateInProgress){
        updateAgain = true;
        return;
    }

    updateInProgress = true;

    try{

        const response = await fetch(
            "/history?range="
            + encodeURIComponent(selectedRange)
            + "&granularity="
            + encodeURIComponent(selectedGranularity),
            {cache:"no-store"}
        );

        if(!response.ok){
            throw new Error("HTTP " + response.status);
        }

        chartHistory = await response.json();

        chart.data.labels = chartHistory.map(
            x => new Date(x.time * 1000).toLocaleString()
        );

        chart.data.datasets[0].data =
            smoothData(chartHistory, selectedSmoothness);

        chart.update();

        if(chartHistory.length){

            const latest = chartHistory[chartHistory.length - 1].price;

            document.getElementById("price").textContent =
                Math.round(latest).toLocaleString() + " coins";

        }else{

            document.getElementById("price").textContent = "No fresh data";
        }

    }catch(error){

        console.error("History update failed:", error);

    }finally{

        updateInProgress = false;

        if(updateAgain){
            updateAgain = false;
            update();
        }
    }
}

document.querySelectorAll("#rangeButtons button")
    .forEach(button => {

        button.addEventListener("click", () => {

            selectedRange = button.dataset.range;

            selectedGranularity = defaultGranularity[selectedRange];

            updateButtons();
            update();
        });
    });

document.querySelectorAll("#granularityButtons button")
    .forEach(button => {

        button.addEventListener("click", () => {

            selectedGranularity = Number(button.dataset.granularity);

            updateButtons();
            update();
        });
    });

document.querySelectorAll("#smoothnessButtons button")
    .forEach(button => {

        button.addEventListener("click", () => {

            selectedSmoothness = Number(button.dataset.smoothness);

            chart.data.datasets[0].data =
                smoothData(chartHistory, selectedSmoothness);

            chart.update();
            updateButtons();
        });
    });

updateButtons();
update();

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

    selected_range = request.args.get("range", "hour")

    if selected_range not in DEFAULT_GRANULARITY:
        selected_range = "hour"

    granularity = request.args.get("granularity", type=int)

    if granularity not in VALID_GRANULARITIES:
        granularity = DEFAULT_GRANULARITY[selected_range]

    if selected_range == "max":
        start_time = None
    else:
        start_time = time.time() - TIME_RANGES[selected_range]

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

            "compressed_bytes": len(compressed_data),

            "compressed_mb": round(
                len(compressed_data) / 1024 / 1024,
                3
            ),

            "calibrations": len(calibration_index),

            "price_scale": PRICE_DIVISOR,

            "calibration_seconds": CALIBRATION_INTERVAL,

            "current_price": (
                None
                if current_price is None
                else decompress_price(current_price)
            ),

            "current_timestamp": current_timestamp

        })


# ============================================================
# STARTUP
# ============================================================

if __name__ == "__main__":

    print("================================================")
    print("Starting Donut Elytra price tracker...")
    print("================================================")

    # Load existing main history first. If it is unavailable,
    # recovery uses only the new backups_v2/ folder.
    load_history_with_recovery()

    print("History initialization complete.")

    # Clean old versions of the exact main-history filename.
    # This does not target backups_v2/, backups/, or snapshots/.
    cleanup_old_history_versions()

    threading.Thread(
        target=collector,
        daemon=True,
        name="price-collector"
    ).start()

    threading.Thread(
        target=b2_uploader,
        daemon=True,
        name="b2-uploader"
    ).start()

    threading.Thread(
        target=b2_backup_worker,
        daemon=True,
        name="b2-backup-worker"
    ).start()

    print("Collector started.")
    print("B2 uploader started.")
    print("Six-hour V2 backup worker started.")
    print("New backup folder: backups_v2/")
    print("Web server starting...")

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000)),
        threaded=True
    )
