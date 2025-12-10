import streamlit as st
import pandas as pd
import sqlite3

DB_PATH = "dota_matches.db"


def load_matches(limit=200):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(f"SELECT * FROM matches LIMIT {limit}", conn)


st.title("Dota 2 Pro Match Dashboard")

data = load_matches()

st.subheader("Match Records")
st.dataframe(data)

st.subheader("Winrate Summary")
if "winner_side" in data.columns:
    st.bar_chart(data["winner_side"].value_counts())
