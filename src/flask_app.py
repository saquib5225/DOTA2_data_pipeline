from flask import Flask, jsonify
import sqlite3
import pandas as pd

from pipeline_core import (
    MatchPipeline,
    OpenDotaClient,
    MatchTransformer,
    DatabaseManager,
    DB_PATH,
)

app = Flask(__name__)


def fetch_matches_from_db(limit=50):
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(f"SELECT * FROM matches LIMIT {limit};", conn)
    return df


@app.route("/")
def home():
    return jsonify({"message": "Dota 2 Match Pipeline API running (Flask)"})


@app.route("/matches", methods=["GET"])
def get_matches():
    df = fetch_matches_from_db()
    return jsonify(df.to_dict(orient="records"))


@app.route("/run_pipeline", methods=["POST"])
def run_pipeline_api():
    pipeline = MatchPipeline(
        OpenDotaClient(), MatchTransformer(), DatabaseManager(DB_PATH)
    )
    clean_df = pipeline.run(limit=150)
    return jsonify({"status": "pipeline executed", "rows_saved": len(clean_df)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9090, debug=True)
