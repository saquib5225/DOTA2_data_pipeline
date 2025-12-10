import requests
import pandas as pd
import sqlite3
import logging

BASE_URL = "https://api.opendota.com/api"
DB_PATH = "dota_matches.db"

logging.basicConfig(level=logging.INFO)


def fetch_pro_matches(limit=100):
    logging.info("Fetching data from OpenDota API...")
    url = f"{BASE_URL}/proMatches"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data[:limit]


def clean_pro_matches(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning raw match data and engineering features...")

    target_cols = [
        "match_id", "duration", "start_time", "radiant_win",
        "radiant_name", "dire_name", "league_name",
        "radiant_score", "dire_score"
    ]

    cols = [c for c in target_cols if c in df.columns]
    clean = df[cols].copy()

    if "start_time" in clean.columns:
        clean["start_time"] = pd.to_datetime(clean["start_time"], unit="s")

    if "duration" in clean.columns:
        clean["duration_minutes"] = clean["duration"] / 60

    if "radiant_win" in clean.columns:
        clean["winner_side"] = clean["radiant_win"].map(
            {True: "Radiant", False: "Dire"}
        )

    if "start_time" in clean.columns:
        clean = clean.sort_values("start_time", ascending=False)

    return clean


def save_matches_to_db(
    df: pd.DataFrame,
    db_path: str = DB_PATH,
    table_name: str = "matches"
):
    logging.info(f"Saving cleaned data into database at {db_path}...")
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    logging.info(f"Saved {len(df)} rows to table '{table_name}'.")


def run_pro_matches_pipeline(limit=200, db_path: str = DB_PATH) -> pd.DataFrame:
    logging.info(f"Running functional pipeline with limit={limit}...")
    raw = fetch_pro_matches(limit=limit)
    df = pd.DataFrame(raw)
    clean = clean_pro_matches(df)
    save_matches_to_db(clean, db_path=db_path)
    logging.info("Functional pipeline run complete.")
    return clean


class OpenDotaClient:
    def __init__(self, base_url="https://api.opendota.com/api"):
        self.base_url = base_url

    def fetch_pro_matches(self, limit=100):
        url = f"{self.base_url}/proMatches"
        logging.info("Fetching data from OpenDota via OOP client...")
        response = requests.get(url)
        response.raise_for_status()
        return response.json()[:limit]


class MatchTransformer:
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Cleaning match data in OOP transformer...")

        target_cols = [
            "match_id", "duration", "start_time",
            "radiant_win", "radiant_name", "dire_name",
            "league_name", "radiant_score", "dire_score"
        ]

        cols = [c for c in target_cols if c in df.columns]
        clean = df[cols].copy()

        if "start_time" in clean.columns:
            clean["start_time"] = pd.to_datetime(clean["start_time"], unit="s")

        if "duration" in clean.columns:
            clean["duration_minutes"] = clean["duration"] / 60

        if "radiant_win" in clean.columns:
            clean["winner_side"] = clean["radiant_win"].map(
                {True: "Radiant", False: "Dire"}
            )

        if "start_time" in clean.columns:
            clean = clean.sort_values("start_time", ascending=False)

        return clean


class DatabaseManager:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    def save(self, df: pd.DataFrame, table_name: str = "matches"):
        logging.info(
            f"Saving {len(df)} records into '{table_name}' via DatabaseManager..."
        )
        with sqlite3.connect(self.db_path) as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
        logging.info("Save complete via DatabaseManager.")


class MatchPipeline:
    def __init__(
        self,
        api_client: OpenDotaClient,
        transformer: MatchTransformer,
        database: DatabaseManager,
    ):
        self.api_client = api_client
        self.transformer = transformer
        self.database = database

    def run(self, limit=200) -> pd.DataFrame:
        logging.info("Running OOP match pipeline...")
        raw_data = self.api_client.fetch_pro_matches(limit=limit)
        df = pd.DataFrame(raw_data)
        clean_df = self.transformer.clean(df)
        self.database.save(clean_df)
        logging.info("OOP pipeline run complete.")
        return clean_df


if __name__ == "__main__":
    run_pro_matches_pipeline(limit=200)
