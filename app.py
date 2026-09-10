from flask import Flask
import requests

app = Flask(__name__)

API_URL = "https://api.donut.auction/v2/tickers/"

@app.route("/")
def home():
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()

        tickers = response.json()

        elytra = next(
            item for item in tickers
            if item["itemName"] == "elytra"
            and not item["isStale"]
        )

        return {
            "status": "ok",
            "elytra_price": elytra["unitPrice"],
            "observed_at": elytra["observedAt"]
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
