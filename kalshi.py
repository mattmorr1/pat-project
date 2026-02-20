import requests
from functools import lru_cache

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

EVENT_TICKERS = {
    "say": "KXTRUMPMENTION-26FEB28",
    "mention": "KXTRUMPMENTION-26MAR02",
}


def get_event_markets(event_ticker: str) -> list[dict]:
    url = f"{BASE_URL}/markets"
    markets = []
    cursor = None

    while True:
        params = {"event_ticker": event_ticker, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        markets.extend(data.get("markets", []))
        cursor = data.get("cursor")
        if not cursor:
            break

    return markets


def get_market(ticker: str) -> dict:
    url = f"{BASE_URL}/markets/{ticker}"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json().get("market", {})


def fetch_all_markets() -> dict[str, list[dict]]:
    return {
        label: get_event_markets(ticker)
        for label, ticker in EVENT_TICKERS.items()
    }


def build_title_to_ticker_map(markets: list[dict]) -> dict[str, str]:
    mapping = {}
    for m in markets:
        title = m.get("yes_sub_title", "").strip()
        if title:
            mapping[title] = m["ticker"]
    return mapping


def parse_market_row(m: dict) -> dict:
    return {
        "ticker": m.get("ticker", ""),
        "event_ticker": m.get("event_ticker", ""),
        "title": m.get("yes_sub_title", ""),
        "yes_price": m.get("last_price_dollars", "0"),
        "yes_bid": m.get("yes_bid_dollars", "0"),
        "yes_ask": m.get("yes_ask_dollars", "0"),
        "status": m.get("status", ""),
        "result": m.get("result", ""),
    }
