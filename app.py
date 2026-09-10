from flask import Flask, jsonify, render_template_string, request
import requests
import threading
import time
import struct
import statistics

app = Flask(__name__)

API_URL = "https://api.donut.auction/v2/tickers/"
SAMPLE_INTERVAL = 1
CALIBRATION_INTERVAL = 1800
PRICE_DIVISOR = 100_000

DEFAULT_GRANULARITY = {
    "minute": 1, "5minutes": 1, "hour": 60,
    "day": 300, "week": 3600, "month": 86400, "max": 86400
}

TIME_RANGES = {
    "minute": 60, "5minutes": 300, "hour": 3600,
    "day": 86400, "week": 604800, "month": 2592000
}

VALID_GRANULARITIES = {1, 10, 60, 300, 3600, 86400}

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

# C = timestamp + absolute price
# D = signed price delta
compressed_data = bytearray()
calibration_index = []
current_price = None
current_timestamp = None
last_calibration = None
data_lock = threading.RLock()


def compress_price(price):
    return int(price) // PRICE_DIVISOR


def decompress_price(price):
    return price * PRICE_DIVISOR


def write_delta(delta):
    if -64 <= delta <= 63:
        compressed_data.append(delta + 64)
    else:
        compressed_data.append(128)
        compressed_data.extend(struct.pack("<h", delta))


def read_delta(data, pos):
    value = data[pos]
    pos += 1
    if value != 128:
        return value - 64, pos
    return struct.unpack("<h", data[pos:pos + 2])[0], pos + 2


def add_price(timestamp, real_price):
    global current_price, current_timestamp, last_calibration

    price = compress_price(real_price)

    with data_lock:
        if current_price is None:
            offset = len(compressed_data)
            compressed_data.extend(struct.pack("<dH", timestamp, price))
            calibration_index.append((timestamp, offset))
            last_calibration = timestamp

        elif (
            timestamp - last_calibration >= CALIBRATION_INTERVAL
            or timestamp - current_timestamp > SAMPLE_INTERVAL * 1.5
        ):
            offset = len(compressed_data)
            compressed_data.extend(struct.pack("<dH", timestamp, price))
            calibration_index.append((timestamp, offset))
            last_calibration = timestamp

        else:
            write_delta(price - current_price)

        current_price = price
        current_timestamp = timestamp


def decode_history(start_time=None, granularity=1):
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

            values = [x["price"] for x in bucket]
            results.append({
                "time": bucket[-1]["time"],
                "price": bucket[-1]["price"],
                "min": min(values),
                "max": max(values),
                "median": statistics.median(values)
            })
            bucket.clear()

        while pos < len(compressed_data):
            kind = compressed_data[pos:pos + 1]
            pos += 1

            if kind == b"C":
                if pos + 10 > len(compressed_data):
                    break

                timestamp, price = struct.unpack(
                    "<dH", compressed_data[pos:pos + 10]
                )
                pos += 10

            elif kind == b"D":
                if pos + 2 > len(compressed_data):
                    break

                delta, pos = read_delta(compressed_data, pos)
                price += delta
                timestamp += SAMPLE_INTERVAL

            else:
                break

            if start_time is not None and timestamp < start_time:
                continue

            bucket.append({
                "time": timestamp,
                "price": decompress_price(price)
            })

            if (
                granularity == 1
                or timestamp - bucket[0]["time"] >= granularity
            ):
                finish_bucket()

        finish_bucket()
        return results


def get_elytra_price():
    response = requests.get(API_URL, headers=HEADERS, timeout=10)
    response.raise_for_status()

    return next(
        x["unitPrice"] for x in response.json()
        if x["itemName"] == "elytra" and not x["isStale"]
    )


def collector():
    while True:
        try:
            price = get_elytra_price()
            add_price(time.time(), price)

            with data_lock:
                size = len(compressed_data)

            print(
                f"Elytra: {price:,} | "
                f"RAM: {size / 1024 / 1024:.2f} MB"
            )
        except Exception as e:
            print(f"Error collecting price: {e}")

        time.sleep(SAMPLE_INTERVAL)


@app.route("/")
def home():
    return render_template_string("""
<!DOCTYPE html>
<html>
<head>
<title>Donut Elytra Price</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
body{background:#111;color:white;font-family:Arial,sans-serif;max-width:1000px;
margin:40px auto;padding:20px}
h1{text-align:center}
#price{text-align:center;font-size:40px;font-weight:bold;margin:20px}
.controls{display:flex;flex-wrap:wrap;gap:20px;justify-content:center;margin:20px 0}
.control{text-align:center}
.control label{display:block;margin-bottom:8px;color:#aaa;font-size:14px}
.buttons{display:flex;gap:5px;flex-wrap:wrap;justify-content:center}
button{background:#222;color:white;border:1px solid #444;border-radius:7px;
padding:8px 12px;cursor:pointer}
button:hover{background:#333}
button.active{background:#00a866;border-color:#00ff88}
#stats{background:#181818;border-radius:10px;padding:12px 16px;margin-bottom:15px;
display:flex;justify-content:center;flex-wrap:wrap;gap:25px;color:#ccc}
.stat strong{color:white}
canvas{background:#181818;border-radius:12px;padding:10px}
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
const ctx=document.getElementById("chart");

const defaultGranularity={
    minute:1,"5minutes":1,hour:60,day:300,
    week:3600,month:86400,max:86400
};

let selectedRange="hour";
let selectedGranularity=defaultGranularity[selectedRange];
let selectedSmoothness=0;
let chartHistory=[];

function smoothData(data,level){
    if(!level||data.length<3)return data.map(x=>x.price);

    const radius=level*3;

    return data.map((point,i)=>{
        let start=Math.max(0,i-radius);
        let end=Math.min(data.length-1,i+radius);
        let total=0;

        for(let j=start;j<=end;j++) total+=data[j].price;

        return total/(end-start+1);
    });
}

const chart=new Chart(ctx,{
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
        interaction:{mode:"index",intersect:false},
        plugins:{tooltip:{enabled:false}},
        scales:{
            x:{title:{display:true,text:"Time"}},
            y:{
                title:{display:true,text:"Price"},
                ticks:{callback:value=>Number(value).toLocaleString()}
            }
        }
    }
});

function updateButtons(){
    document.querySelectorAll("#rangeButtons button").forEach(b=>
        b.classList.toggle("active",b.dataset.range===selectedRange));

    document.querySelectorAll("#granularityButtons button").forEach(b=>
        b.classList.toggle("active",
            Number(b.dataset.granularity)===selectedGranularity));

    document.querySelectorAll("#smoothnessButtons button").forEach(b=>
        b.classList.toggle("active",
            Number(b.dataset.smoothness)===selectedSmoothness));
}

function showStats(point){
    if(!point)return;

    document.getElementById("hoverTime").textContent=
        new Date(point.time*1000).toLocaleString();

    document.getElementById("hoverPrice").textContent=
        Math.round(point.price).toLocaleString()+" coins";

    document.getElementById("statMin").textContent=
        Math.round(point.min).toLocaleString();

    document.getElementById("statMax").textContent=
        Math.round(point.max).toLocaleString();

    document.getElementById("statMedian").textContent=
        Math.round(point.median).toLocaleString();
}

ctx.addEventListener("mousemove",event=>{
    const elements=chart.getElementsAtEventForMode(
        event,"index",{intersect:false},false
    );

    if(elements.length)showStats(chartHistory[elements[0].index]);
});

async function update(){
    try{
        const response=await fetch(
            "/history?range="+selectedRange+
            "&granularity="+selectedGranularity
        );

        chartHistory=await response.json();

        chart.data.labels=chartHistory.map(x=>
            new Date(x.time*1000).toLocaleString()
        );

        chart.data.datasets[0].data=smoothData(
            chartHistory,selectedSmoothness
        );

        chart.update();

        if(chartHistory.length){
            document.getElementById("price").textContent=
                Math.round(
                    chartHistory[chartHistory.length-1].price
                ).toLocaleString()+" coins";
        }
    }catch(error){
        console.error("History update failed:",error);
    }
}

document.querySelectorAll("#rangeButtons button").forEach(b=>
    b.addEventListener("click",()=>{
        selectedRange=b.dataset.range;
        selectedGranularity=defaultGranularity[selectedRange];
        updateButtons();
        update();
    })
);

document.querySelectorAll("#granularityButtons button").forEach(b=>
    b.addEventListener("click",()=>{
        selectedGranularity=Number(b.dataset.granularity);
        updateButtons();
        update();
    })
);

document.querySelectorAll("#smoothnessButtons button").forEach(b=>
    b.addEventListener("click",()=>{
        selectedSmoothness=Number(b.dataset.smoothness);

        chart.data.datasets[0].data=smoothData(
            chartHistory,selectedSmoothness
        );

        chart.update();
        updateButtons();
    })
);

updateButtons();
update();
setInterval(update,5000);
</script>
</body>
</html>
""")


@app.route("/history")
def history_endpoint():
    selected_range=request.args.get("range","hour")

    if selected_range not in DEFAULT_GRANULARITY:
        selected_range="hour"

    granularity=request.args.get("granularity",type=int)

    if granularity not in VALID_GRANULARITIES:
        granularity=DEFAULT_GRANULARITY[selected_range]

    start_time=None if selected_range=="max" else (
        time.time()-TIME_RANGES[selected_range]
    )

    return jsonify(decode_history(start_time,granularity))


@app.route("/stats")
def stats():
    with data_lock:
        return jsonify({
            "compressed_bytes":len(compressed_data),
            "compressed_mb":round(
                len(compressed_data)/1024/1024,3
            ),
            "calibrations":len(calibration_index),
            "price_scale":PRICE_DIVISOR,
            "calibration_seconds":CALIBRATION_INTERVAL,
            "current_price":(
                None if current_price is None
                else decompress_price(current_price)
            )
        })


if __name__=="__main__":
    threading.Thread(target=collector,daemon=True).start()
    app.run(host="0.0.0.0",port=10000)
