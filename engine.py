import os
import time
import psycopg2
from typing import List
from datetime import datetime
from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage, Feed, Market

# --- Config from environment variables ---
API_KEY  = os.environ["2GxgoDGhSukwYFlP9T9BOLfvi6YpNJK5"]
DB_URL   = os.environ["postgresql://neondb_owner:npg_sQJqnHhkor92@ep-lively-bread-aqixv2yq.c-8.us-east-1.aws.neon.tech/neondb?sslmode=require"]

# --- DB Setup ---
def get_conn():
    return psycopg2.connect(DB_URL)

def setup_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS options_flow (
                id          SERIAL PRIMARY KEY,
                sym         TEXT,
                ev          TEXT,
                av          BIGINT,
                vw          REAL,
                s           BIGINT,
                e           BIGINT,
                option_type TEXT,
                ts          TIMESTAMPTZ DEFAULT NOW()
            )
        """)
    conn.commit()

conn = get_conn()
setup_db(conn)
print("✅ Database ready.")

# --- Message Handler ---
def handle_msg(msgs: List[WebSocketMessage]):
    rows = []
    for m in msgs:
        ticker     = getattr(m, 'symbol', '')
        event_type = getattr(m, 'event_type', '')

        if event_type != "AM":
            continue
        if "SPX" not in ticker and "NDX" not in ticker and "DJX" not in ticker:
            continue

        opt_type = 'C' if 'C' in ticker.split(':')[-1] else 'P'
        av = getattr(m, 'accumulated_volume', 0)
        vw = getattr(m, 'vwap', 0.0)
        s  = getattr(m, 'start_timestamp', 0)
        e  = getattr(m, 'end_timestamp', 0)

        rows.append((ticker, event_type, av, vw, s, e, opt_type))
        print(f"🌊 {datetime.now().strftime('%H:%M:%S')} | {ticker} | Vol: {av}")

    if rows:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO options_flow (sym,ev,av,vw,s,e,option_type) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                rows
            )
        conn.commit()

# --- WebSocket Client ---
client = WebSocketClient(
    api_key=API_KEY,
    feed=Feed.RealTime,
    market=Market.Options
)

client.subscribe("AM.*")
print("🚀 Subscribed to AM.* — filtering SPX, NDX, DJX in real-time...")

# Reconnect loop
while True:
    try:
        client.run(handle_msg)
    except Exception as ex:
        print(f"⚠️  Disconnected: {ex} — reconnecting in 5s...")
        time.sleep(5)
