from fastapi import FastAPI
import sqlite3
import pandas as pd

from pipeline_core import (
    MatchPipeline,
    OpenDotaClient,
    MatchTransformer,
    DatabaseManager,
    DB_PATH,
)

app_fast = FastAPI(title="Dota 2 Match Pipeline API (FastAPI)")


def load_matches(limit=50):
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(f"SELECT * FROM matches LIMIT {limit}", conn)
    return df


@app_fast.get("/")
def root():
    return {"message": "FastAPI service running"}


@app_fast.get("/matches")
def get_matches(limit: int = 50):
    df = load_matches(limit)
    return df.to_dict(orient="records")


@app_fast.post("/run")
def run_pipeline_endpoint(limit: int = 100):
    client = OpenDotaClient()
    transformer = MatchTransformer()
    db = DatabaseManager(DB_PATH)

    pipeline = MatchPipeline(client, transformer, db)
    clean = pipeline.run(limit=limit)

    return {"status": "pipeline executed", "rows_saved": len(clean)}
  
