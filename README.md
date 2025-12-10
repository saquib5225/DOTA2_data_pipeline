# DOTA2_data_pipeline

This repository contains a complete Dota 2 data acquisition and preprocessing pipeline built on the OpenDota API.

## Structure

- `notebooks/` – Colab / Jupyter notebooks used for exploration and documentation  
- `src/` – Production-ready Python scripts:
  - `pipeline_core.py` – pipeline logic and classes
  - `flask_app.py` – Flask REST API
  - `fast_app.py` – FastAPI microservice
  - `streamlit_app.py` – Streamlit dashboard
- `docs/` – written documentation and reports

## Technologies

- Python, pandas, requests  
- SQLite for storage  
- Flask and FastAPI for web APIs  
- Streamlit for interactive dashboards  
- Deployed via Google Cloud VM and GitHub
