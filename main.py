# main.py
import os
import csv
import sqlite3
import threading
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

# -------------------------
# CONFIG
# -------------------------
DB_PATH = os.environ.get("BEASTBET_DB", "beastbet_master.db")
MASTER_CSV = os.environ.get("BEASTBET_CSV", "beastbet_master.csv")
API_KEY = os.environ.get("BEASTBET_API_KEY", "supersecret_change_me")  # set in Render env
ALLOW_ORIGINS = ["*"]  # replace with specific origins if you want

# -------------------------
# APP + CORS
# -------------------------
app = FastAPI(title="BeastBet Cloud API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# DB init + concurrency lock
# -------------------------
_write_lock = threading.Lock()

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id INTEGER PRIMARY KEY,
            home TEXT,
            away TEXT,
            odds_h REAL,
            odds_x REAL,
            odds_a REAL,
            source TEXT,
            created_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER,
            ht_score TEXT,
            ft_score TEXT,
            result_at TEXT,
            source TEXT
        )
    """)
    conn.commit()
    conn.close()

# ensure csv exists (header)
def init_csv():
    if not os.path.exists(MASTER_CSV):
        with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["match_id","home","away","odds_h","odds_x","odds_a","source","created_at"])

init_db()
init_csv()

# -------------------------
# Pydantic models
# -------------------------
class MatchIn(BaseModel):
    match_id: int
    home: str
    away: str
    odds_h: float
    odds_x: float
    odds_a: float
    source: Optional[str] = "client"  # optional source tag (client id, EXE id, etc.)

class ResultIn(BaseModel):
    match_id: int
    ht_score: Optional[str] = None
    ft_score: Optional[str] = None
    source: Optional[str] = "client"

class BulkMatchesIn(BaseModel):
    matches: List[MatchIn]
    source: Optional[str] = "bulk"

# -------------------------
# AUTH utility
# -------------------------
def require_api_key(x_api_key: Optional[str]):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing API key")
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

# -------------------------
# HELPERS: insert, dedupe, csv append
# -------------------------
def insert_or_update_match(match: MatchIn):
    # Acquire lock to protect sqlite + csv consistency
    with _write_lock:
        conn = get_conn()
        cur = conn.cursor()
        # Check if match exists
        cur.execute("SELECT * FROM matches WHERE match_id=?", (match.match_id,))
        existing = cur.fetchone()
        now = datetime.utcnow().isoformat()
        if existing:
            # update odds and metadata
            cur.execute("""
                UPDATE matches
                SET home=?, away=?, odds_h=?, odds_x=?, odds_a=?, source=?, created_at=?
                WHERE match_id=?
            """, (match.home, match.away, match.odds_h, match.odds_x, match.odds_a, match.source, now, match.match_id))
            conn.commit()
            conn.close()
            # also append a line to CSV with updated_at (keeps history)
            with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([match.match_id, match.home, match.away, match.odds_h, match.odds_x, match.odds_a, match.source, now])
            return {"status": "updated", "match_id": match.match_id}
        else:
            cur.execute("""
                INSERT INTO matches (match_id, home, away, odds_h, odds_x, odds_a, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (match.match_id, match.home, match.away, match.odds_h, match.odds_x, match.odds_a, match.source, now))
            conn.commit()
            conn.close()
            with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([match.match_id, match.home, match.away, match.odds_h, match.odds_x, match.odds_a, match.source, now])
            return {"status": "inserted", "match_id": match.match_id}

def insert_result(result: ResultIn):
    with _write_lock:
        conn = get_conn()
        cur = conn.cursor()
        now = datetime.utcnow().isoformat()
        cur.execute("""
            INSERT INTO results (match_id, ht_score, ft_score, result_at, source)
            VALUES (?, ?, ?, ?, ?)
        """, (result.match_id, result.ht_score, result.ft_score, now, result.source))
        conn.commit()
        conn.close()
        # Optionally: append to master CSV as a separate result history file or same CSV (we keep match CSV separate)
        return {"status": "inserted_result", "match_id": result.match_id}

# -------------------------
# ROUTES
# -------------------------
@app.post("/add_match/")
async def add_match(match: MatchIn, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    # Basic validation
    if match.odds_h <= 1 or match.odds_x <= 1 or match.odds_a <= 1:
        raise HTTPException(status_code=400, detail="Odds must be > 1.0")
    return insert_or_update_match(match)

@app.post("/add_result/")
async def add_result(result: ResultIn, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    return insert_result(result)

@app.post("/upload_matches/")
async def upload_matches(payload: BulkMatchesIn, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    responses = []
    for m in payload.matches:
        responses.append(insert_or_update_match(m))
    return {"status": "ok", "count": len(responses), "results": responses}

@app.get("/show_matches/")
async def show_matches(x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM matches ORDER BY created_at DESC")
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/get_master_csv/")
async def get_master_csv(x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    if not os.path.exists(MASTER_CSV):
        raise HTTPException(status_code=404, detail="Master CSV not found")
    return {"download_url": f"/download_csv/"}  # small helper; actual file served on /download_csv/

@app.get("/download_csv/")
async def download_csv(x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    if not os.path.exists(MASTER_CSV):
        raise HTTPException(status_code=404, detail="Master CSV not found")
    # Serve as plain text CSV
    return RequestResponseFile(MASTER_CSV)

# Small helper to return file content (keeps dependencies minimal)
from fastapi.responses import FileResponse
def RequestResponseFile(path):
    return FileResponse(path, media_type="text/csv", filename=os.path.basename(path))

# A simple predict endpoint (optional/simple)
@app.get("/predict/{match_id}")
async def predict(match_id: int, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM matches WHERE match_id=?", (match_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Match not found")
    # Simple pick-by-lowest-odds (you can replace with your model later)
    odds = {"HOME": row["odds_h"], "DRAW": row["odds_x"], "AWAY": row["odds_a"]}
    pick = min(odds, key=odds.get)
    confidence = round(max(0.5, min(0.95, 1.0 / odds[pick])), 2)
    return {
        "match_id": row["match_id"],
        "home": row["home"],
        "away": row["away"],
        "pick": pick,
        "confidence": confidence,
        "odds_used": odds[pick]
    }

# Health check root (render shows 404 for /, better to return a simple message)
@app.get("/")
async def root():
    return {"service": "beastbet-cloud-api", "status": "ok", "time": datetime.utcnow().isoformat()}

