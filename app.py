import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timezone
from streamlit_autorefresh import st_autorefresh

import kalshi
import db

st.set_page_config(page_title="SOTU Leaderboard", layout="wide")

# ---------------------------------------------------------------------------
# Session state: cache ticker maps so we don't re-fetch every rerun
# ---------------------------------------------------------------------------
if "title_to_ticker" not in st.session_state:
    st.session_state.title_to_ticker = {}


def _ensure_ticker_map():
    if st.session_state.title_to_ticker:
        return
    try:
        all_markets = kalshi.fetch_all_markets()
        for label, markets in all_markets.items():
            st.session_state.title_to_ticker[label] = kalshi.build_title_to_ticker_map(markets)
    except Exception as e:
        st.warning(f"Could not fetch Kalshi ticker map: {e}")


# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------
page = st.sidebar.radio("Navigation", ["Upload Picks", "Market Tracker", "Leaderboard"])

# ============================= PAGE 1 =====================================
if page == "Upload Picks":
    st.title("Upload Picks")
    st.markdown("Upload an `.xlsx` file with columns: **timestamp, name, pick_1, pick_2, pick_3, pick_4, pick_5**")

    uploaded = st.file_uploader("Choose .xlsx file", type=["xlsx"])

    if uploaded is not None:
        raw = pd.read_excel(uploaded, engine="openpyxl")
        raw.columns = [c.strip().lower().replace(" ", "_").rstrip(":_") for c in raw.columns]

        name_col = next((c for c in raw.columns if "name" in c), None)
        if name_col and name_col != "name":
            raw = raw.rename(columns={name_col: "name"})

        st.subheader("Preview")
        st.dataframe(raw, use_container_width=True)

        pick_cols = [c for c in raw.columns if c.startswith("pick")]
        errors = []
        for idx, row in raw.iterrows():
            for col in pick_cols:
                pick = str(row[col]).strip()
                cat, _, _ = db.validate_pick(pick)
                if cat is None:
                    errors.append(f"Row {idx + 1}, {col}: '{pick}' not a valid option")

        if errors:
            st.error("Validation errors:")
            for e in errors:
                st.write(f"- {e}")
        else:
            st.success("All picks validated.")
            if st.button("Lock In Picks"):
                _ensure_ticker_map()
                db.save_picks(raw, st.session_state.title_to_ticker)
                st.toast("Picks locked in.")
                st.rerun()

    st.divider()
    st.subheader("Locked Picks")
    picks_df = db.get_picks()
    if picks_df.empty:
        st.info("No picks locked yet.")
    else:
        display = picks_df[["name", "pick", "points", "event_ticker", "locked_at"]].copy()
        st.dataframe(display, use_container_width=True, hide_index=True)

    if not picks_df.empty:
        if st.button("Clear All Picks", type="secondary"):
            db.clear_picks()
            st.rerun()

# ============================= PAGE 2 =====================================
elif page == "Market Tracker":
    st.title("Market Tracker")

    tick = st_autorefresh(interval=60_000, key="market_autorefresh")

    def _refresh_markets():
        try:
            all_markets = kalshi.fetch_all_markets()
            for label, markets in all_markets.items():
                parsed = [kalshi.parse_market_row(m) for m in markets]
                db.save_snapshot(parsed)
                st.session_state.title_to_ticker[label] = kalshi.build_title_to_ticker_map(markets)
            return True
        except Exception as e:
            st.error(f"API error: {e}")
            return False

    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("Refresh Now"):
            with st.spinner("Fetching from Kalshi..."):
                if _refresh_markets():
                    st.success("Snapshot saved.")
    with col2:
        st.caption("Auto-refreshes every 60 seconds")

    if tick > 0:
        _refresh_markets()

    for label, event_ticker in kalshi.EVENT_TICKERS.items():
        heading = "What Will Trump Say" if label == "say" else "Who Will Trump Mention"
        st.subheader(heading)

        snaps = db.get_snapshots(event_ticker)
        if snaps.empty:
            st.info("No snapshots yet. Click Refresh Markets.")
            continue

        latest_time = snaps["snapshot_time"].max()
        latest = snaps[snaps["snapshot_time"] == latest_time].copy()
        latest = latest.sort_values("title")

        st.markdown(f"**Latest snapshot:** {latest_time}")
        st.dataframe(
            latest[["title", "yes_price", "status", "result"]].rename(
                columns={"title": "Option", "yes_price": "YES Price", "status": "Status", "result": "Result"}
            ),
            use_container_width=True,
            hide_index=True,
        )

        picked_options = set()
        picks_df = db.get_picks()
        if not picks_df.empty:
            point_map = db.SAY_POINTS if label == "say" else db.MENTION_POINTS
            picked_options = set(p for p in picks_df["pick"] if p in point_map)

        filter_picked = st.checkbox(f"Show only picked options ({heading})", key=f"filter_{label}")
        chart_data = snaps.copy()
        if filter_picked and picked_options:
            chart_data = chart_data[chart_data["title"].isin(picked_options)]

        if not chart_data.empty:
            fig = px.line(
                chart_data,
                x="snapshot_time",
                y="yes_price",
                color="title",
                title=f"{heading} -- YES Price Over Time",
                labels={"snapshot_time": "Time", "yes_price": "YES Price ($)", "title": "Option"},
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

# ============================= PAGE 3 =====================================
elif page == "Leaderboard":
    st.title("Leaderboard")

    if st.button("Finalize Scores"):
        with st.spinner("Fetching latest results and calculating..."):
            try:
                all_markets = kalshi.fetch_all_markets()
                for label, markets in all_markets.items():
                    parsed = [kalshi.parse_market_row(m) for m in markets]
                    db.save_snapshot(parsed)
                    st.session_state.title_to_ticker[label] = kalshi.build_title_to_ticker_map(markets)
                db.backfill_tickers(st.session_state.title_to_ticker)
                scores = db.calculate_scores()
                st.success("Scores updated from latest market data.")
            except Exception as e:
                st.error(f"Error: {e}")

    st.subheader("Scoreboard")
    board = db.get_leaderboard()
    picks_exist = not db.get_picks().empty

    if board.empty and not picks_exist:
        st.info("No picks uploaded yet.")
    elif board.empty and picks_exist:
        st.info("Click 'Finalize Scores' to pull latest results and calculate points.")
    else:
        board.insert(0, "Rank", range(1, len(board) + 1))
        st.dataframe(
            board.rename(columns={
                "name": "Name",
                "total_points": "Total Points",
                "correct_picks": "Correct Picks",
                "updated_at": "Last Updated",
            }),
            use_container_width=True,
            hide_index=True,
        )

        if board["total_points"].sum() > 0:
            fig = px.bar(
                board,
                x="name",
                y="total_points",
                title="Points by Player",
                labels={"name": "Player", "total_points": "Points"},
            )
            fig.update_layout(xaxis_categoryorder="total descending")
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Pick Breakdown")
    st.caption("Points = YES price at lock-in x100. If market resolves YES you earn those points; NO = 0.")
    details = db.get_pick_details()
    if details.empty:
        st.info("No picks to show.")
    else:
        def _status_label(row):
            r = row.get("result", "")
            if r == "yes":
                return "YES -- Earned"
            elif r == "no":
                return "NO -- 0 pts"
            return "Pending"

        def _earned(row):
            if row.get("result", "") == "yes":
                return row["points"]
            if row.get("result", "") == "no":
                return 0
            return ""

        details["outcome"] = details.apply(_status_label, axis=1)
        details["earned"] = details.apply(_earned, axis=1)

        for name in details["name"].unique():
            person = details[details["name"] == name].copy()
            resolved = person[person["result"].isin(["yes", "no"])]
            earned_total = resolved.apply(lambda r: r["points"] if r["result"] == "yes" else 0, axis=1).sum() if not resolved.empty else 0
            pending = (person["result"] == "").sum() + person["result"].isna().sum()

            header = f"{name} -- {int(earned_total)} pts earned"
            if pending > 0:
                header += f" ({pending} pending)"

            with st.expander(header):
                show = person[["pick", "points", "outcome", "earned"]].copy()
                show.columns = ["Pick", "Potential Pts", "Status", "Earned"]
                st.dataframe(show, use_container_width=True, hide_index=True)
