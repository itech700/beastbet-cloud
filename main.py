from fastapi import FastAPI
from typing import List
import csv
import os

app = FastAPI()

MASTER_FILE = "master_matches.csv"

# Ensure master CSV exists
if not os.path.exists(MASTER_FILE):
    with open(MASTER_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date","home","away","home_strength","away_strength",
            "home_odds","draw_odds","away_odds",
            "over25","btts","prediction","actual"
        ])

@app.get("/")
def home():
    return {"status": "BEASTBET Cloud API Running"}

@app.get("/sync")
def sync_matches():
    with open(MASTER_FILE, "r") as f:
        data = f.read()
    return {"file": data}

@app.post("/upload")
def upload(match: dict):
    rows = []
    with open(MASTER_FILE, "r") as f:
        reader = csv.reader(f)
        rows = list(reader)

    # Append new row
    with open(MASTER_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(match.values())

    return {"status": "ok", "message": "Match uploaded"}
