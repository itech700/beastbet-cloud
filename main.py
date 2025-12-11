
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import joblib
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Allow all origins (for testing, can restrict later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CSV_FILE = "master_matches.csv"
MODEL_FILE = "model_state.json"

# ---------- Models ----------
class Match(BaseModel):
    match_id: str
    home: str
    away: str
    odds_1: float
    odds_X: float
    odds_2: float
    result: str | None = None

# ---------- Helper Functions ----------
def load_master_csv():
    try:
        return pd.read_csv(CSV_FILE)
    except:
        return pd.DataFrame(columns=["match_id","home","away","odds_1","odds_X","odds_2","result"])

def save_master_csv(df):
    df.to_csv(CSV_FILE, index=False)

def train_model():
    df = load_master_csv()
    if len(df) < 10:  # minimal data to train
        return None
    from sklearn.linear_model import LogisticRegression
    X = df[["odds_1","odds_X","odds_2"]]
    y = df["result"].fillna("X")  # default to X if missing
    model = LogisticRegression()
    model.fit(X, y)
    joblib.dump(model, MODEL_FILE)
    return model

def load_model():
    try:
        return joblib.load(MODEL_FILE)
    except:
        return None

# ---------- API Endpoints ----------
@app.post("/sync_match")
def sync_match(match: Match):
    df = load_master_csv()
    df = pd.concat([df,pd.DataFrame([match.dict()])], ignore_index=True)
    save_master_csv(df)
    train_model()
    return {"status":"ok", "message":"Match synced to cloud."}

@app.get("/get_latest_intelligence")
def get_latest_intelligence():
    model = load_model()
    if model:
        # Simplified: send back serialized model
        return {"status":"ok", "model":MODEL_FILE}
    return {"status":"fail", "message":"No model available yet."}

@app.get("/ping")
def ping():
    return {"status":"ok", "message":"BeastBet cloud alive!"}
