import re
import sqlite3
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "picks.db"

SAY_POINTS = {
    "Trillion": 7,
    "250": 15,
    "Trump": 15,
    "ICE / National Guard": 17,
    "Fentanyl / Cocaine": 18,
    "Fraud": 20,
    "Hottest": 25,
    "DEI / Woke": 26,
    "Radical Left": 29,
    "Nuclear": 25,
    "Olympics / World Cup": 30,
    "The State of the / our Union is Strong": 30,
    "Affordability": 32,
    "Eight War": 32,
    "MAHA / Make America Healthy Again": 35,
    "Transgender": 34,
    "Mental Institution": 37,
    "Drill Baby Drill": 39,
    "Ballroom": 49,
    "Vaccine / Autism": 48,
    "Fake News": 51,
    "Highest Inflation": 56,
    "Somali / Somalia / Somalian": 55,
    "Hoax": 60,
    "Windmill": 62,
    "Sleepy Joe": 71,
    "Crypto / Bitcoin": 74,
    "DOGE / Department of Government Efficiency": 74,
    "UFC": 75,
    "TDS / Trump Derangement Syndrome": 82,
    "Discombobulator": 88,
    "Ethereum": 97,
}

MENTION_POINTS = {
    "Biden": 8,
    "Marco / Rubio": 31,
    "Charlie Kirk": 39,
    "President Xi": 43,
    "Putin": 45,
    "Thune": 50,
    "Witkoff": 52,
    "Hegseth": 58,
    "Bessent": 56,
    "Homan": 60,
    "Kristi / Noem": 61,
    "Lincoln": 61,
    "Kash / Patel": 69,
    "Obama": 66,
    "Pam / Bondi": 66,
    "Zelensky": 67,
    "Bibi / Netanyahu": 72,
    "Jared / Kushner": 69,
    "Reagan": 71,
    "Kamala": 72,
    "Clinton": 76,
    "Elon / Musk": 76,
    "Newsom / Newscum": 81,
    "Modi": 79,
    "Warsh": 85,
    "Karoline / Leavitt": 86,
    "Usha": 84,
    "Howard / Lutnick": 82,
    "Walz": 86,
    "Schumer": 86,
    "Epstein": 91,
    "Pelosi": 91,
    "Prince Mohammed": 89,
    "Keir / Starmer": 93,
    "Tulsi / Gabbard": 95,
    "Zohran / Mamdani": 92,
    "Pocahontas": 93,
    "Judy Shelton": 99,
    "Satoshi": 99,
}

# Alias map: user-friendly label -> canonical API title
ALIASES = {
    "Marco Rubio": "Marco / Rubio",
    "Kristi Noem": "Kristi / Noem",
    "Kash Patel": "Kash / Patel",
    "Pam Bondi": "Pam / Bondi",
    "Bibi Netanyahu": "Bibi / Netanyahu",
    "Jared Kushner": "Jared / Kushner",
    "Elon Musk": "Elon / Musk",
    "Newsom": "Newsom / Newscum",
    "Karoline Leavitt": "Karoline / Leavitt",
    "Howard Lutnick": "Howard / Lutnick",
    "Keir Starmer": "Keir / Starmer",
    "Tulsi Gabbard": "Tulsi / Gabbard",
    "Zohran Mamdani": "Zohran / Mamdani",
    "Woke / DEI": "DEI / Woke",
}

ALL_POINTS = {**SAY_POINTS, **MENTION_POINTS}


_POINTS_SUFFIX = re.compile(r"\s*[—–-]\s*\d+\s*[Pp]oints?\s*$")


def strip_points_label(raw: str) -> str:
    return _POINTS_SUFFIX.sub("", raw).strip()


def resolve_pick(pick: str) -> str:
    pick = strip_points_label(pick)
    return ALIASES.get(pick, pick)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = _conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS picks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            name TEXT NOT NULL,
            pick TEXT NOT NULL,
            points INTEGER NOT NULL,
            market_ticker TEXT DEFAULT '',
            event_ticker TEXT DEFAULT '',
            locked_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS market_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            event_ticker TEXT NOT NULL,
            title TEXT NOT NULL,
            yes_price REAL NOT NULL,
            status TEXT NOT NULL,
            result TEXT DEFAULT '',
            snapshot_time TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            total_points INTEGER DEFAULT 0,
            correct_picks INTEGER DEFAULT 0,
            updated_at TEXT NOT NULL
        );
    """)
    conn.close()


def validate_pick(pick: str) -> tuple[str | None, int, str]:
    canonical = resolve_pick(pick)
    if canonical in SAY_POINTS:
        return "say", SAY_POINTS[canonical], canonical
    if canonical in MENTION_POINTS:
        return "mention", MENTION_POINTS[canonical], canonical
    return None, 0, pick


def save_picks(df: pd.DataFrame, title_to_ticker: dict[str, dict[str, str]]):
    from kalshi import EVENT_TICKERS

    conn = _conn()
    now = datetime.now(timezone.utc).isoformat()
    pick_cols = sorted([c for c in df.columns if "pick" in c.lower()])
    name_col = next((c for c in df.columns if "name" in c.lower()), "name")
    ts_col = next((c for c in df.columns if "time" in c.lower()), "timestamp")

    for _, row in df.iterrows():
        name = str(row.get(name_col, "")).strip()
        ts = str(row.get(ts_col, ""))
        if not name:
            continue

        for col in pick_cols:
            raw_val = str(row[col]).strip()
            if not raw_val or raw_val.lower() == "nan":
                continue
            category, pts, canonical = validate_pick(raw_val)
            if category is None:
                continue

            event_ticker = EVENT_TICKERS[category]
            ticker_map = title_to_ticker.get(category, {})
            market_ticker = ticker_map.get(canonical, "")

            conn.execute(
                "INSERT INTO picks (timestamp, name, pick, points, market_ticker, event_ticker, locked_at) VALUES (?,?,?,?,?,?,?)",
                (ts, name, canonical, pts, market_ticker, event_ticker, now),
            )

    conn.commit()
    conn.close()


def get_picks() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query("SELECT * FROM picks ORDER BY name, id", conn)
    conn.close()
    return df


def clear_picks():
    conn = _conn()
    conn.execute("DELETE FROM picks")
    conn.commit()
    conn.close()


def save_snapshot(markets: list[dict]):
    conn = _conn()
    now = datetime.now(timezone.utc).isoformat()
    for m in markets:
        conn.execute(
            "INSERT INTO market_snapshots (ticker, event_ticker, title, yes_price, status, result, snapshot_time) VALUES (?,?,?,?,?,?,?)",
            (
                m.get("ticker", ""),
                m.get("event_ticker", ""),
                m.get("title", m.get("yes_sub_title", "")),
                float(m.get("last_price_dollars", m.get("yes_price", 0))),
                m.get("status", ""),
                m.get("result", ""),
                now,
            ),
        )
    conn.commit()
    conn.close()


def get_snapshots(event_ticker: str = "") -> pd.DataFrame:
    conn = _conn()
    if event_ticker:
        df = pd.read_sql_query(
            "SELECT * FROM market_snapshots WHERE event_ticker = ? ORDER BY snapshot_time",
            conn,
            params=(event_ticker,),
        )
    else:
        df = pd.read_sql_query(
            "SELECT * FROM market_snapshots ORDER BY snapshot_time", conn
        )
    conn.close()
    return df


def backfill_tickers(title_to_ticker: dict[str, dict[str, str]]):
    from kalshi import EVENT_TICKERS
    conn = _conn()
    rows = conn.execute("SELECT id, pick, event_ticker FROM picks WHERE market_ticker = '' OR market_ticker IS NULL").fetchall()
    for r in rows:
        pick = r["pick"]
        for label, et in EVENT_TICKERS.items():
            if et == r["event_ticker"] or not r["event_ticker"]:
                ticker = title_to_ticker.get(label, {}).get(pick, "")
                if ticker:
                    conn.execute("UPDATE picks SET market_ticker = ?, event_ticker = ? WHERE id = ?", (ticker, et, r["id"]))
                    break
    conn.commit()
    conn.close()


def _resolved_yes_expr():
    return "(COALESCE(ms.result, '') = 'yes' OR (COALESCE(ms.result, '') = '' AND ms.yes_price >= 0.99))"

def _resolved_no_expr():
    return "(COALESCE(ms.result, '') = 'no' OR (COALESCE(ms.result, '') = '' AND ms.yes_price <= 0.01))"


def calculate_scores() -> pd.DataFrame:
    yes = _resolved_yes_expr()
    conn = _conn()
    df = pd.read_sql_query(f"""
        SELECT
            p.name,
            SUM(CASE WHEN {yes} THEN p.points ELSE 0 END) as total_points,
            SUM(CASE WHEN {yes} THEN 1 ELSE 0 END) as correct_picks,
            COUNT(*) as total_picks
        FROM picks p
        LEFT JOIN (
            SELECT ticker, title, result, status, yes_price
            FROM market_snapshots
            WHERE id IN (SELECT MAX(id) FROM market_snapshots GROUP BY ticker)
        ) ms ON (p.market_ticker != '' AND p.market_ticker = ms.ticker)
             OR (p.market_ticker = '' AND p.pick = ms.title)
        GROUP BY p.name
        ORDER BY total_points DESC
    """, conn)
    conn.close()

    now = datetime.now(timezone.utc).isoformat()
    conn2 = _conn()
    for _, row in df.iterrows():
        conn2.execute("""
            INSERT INTO scores (name, total_points, correct_picks, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                total_points = excluded.total_points,
                correct_picks = excluded.correct_picks,
                updated_at = excluded.updated_at
        """, (row["name"], int(row["total_points"]), int(row["correct_picks"]), now))
    conn2.commit()
    conn2.close()
    return df


def get_leaderboard() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        "SELECT name, total_points, correct_picks, updated_at FROM scores ORDER BY total_points DESC",
        conn,
    )
    conn.close()
    return df


def get_pick_details() -> pd.DataFrame:
    yes = _resolved_yes_expr()
    no = _resolved_no_expr()
    conn = _conn()
    df = pd.read_sql_query(f"""
        SELECT
            p.name, p.pick, p.points, p.market_ticker, p.event_ticker,
            CASE
                WHEN {yes} THEN 'yes'
                WHEN {no} THEN 'no'
                ELSE COALESCE(ms.result, '')
            END as result,
            COALESCE(ms.status, '') as status,
            COALESCE(ms.yes_price, 0) as yes_price
        FROM picks p
        LEFT JOIN (
            SELECT ticker, title, result, status, yes_price
            FROM market_snapshots
            WHERE id IN (SELECT MAX(id) FROM market_snapshots GROUP BY ticker)
        ) ms ON (p.market_ticker != '' AND p.market_ticker = ms.ticker)
             OR (p.market_ticker = '' AND p.pick = ms.title)
        ORDER BY p.name, p.id
    """, conn)
    conn.close()
    return df


init_db()
