"""
Bootstrap a Superset dashboard with charts for leadership self-serve.
Run after superset-init completes and the warehouse has data.

Usage (from inside the superset container or locally): python bootstrap-dashboard.py
"""

import requests
import json
import sys

BASE = "http://localhost:8088"  # Superset API base URL (adjust if running outside container or with different host/port)


# Authenticate and return a session with CSRF token for subsequent write requests
# In production, we use environment variables or a secure vault for credentials instead of hardcoding
def get_session():
    s = requests.Session()

    # Login
    login_resp = s.post(
        f"{BASE}/api/v1/security/login",
        json={
            "username": "admin",
            "password": "admin",
            "provider": "db",
        },
    )
    if login_resp.status_code != 200:
        print(f"Login failed: {login_resp.status_code} {login_resp.text}")
        sys.exit(1)

    token = login_resp.json()["access_token"]
    s.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
    )

    # Get CSRF token for state-changing requests (POST/PUT/DELETE)
    csrf_resp = s.get(f"{BASE}/api/v1/security/csrf_token/")
    if csrf_resp.status_code == 200:
        csrf = csrf_resp.json().get("result")
        if csrf:
            s.headers["X-CSRFToken"] = csrf

    return s


# Find the Crypto Warehouse database ID
def get_database_id(s):
    resp = s.get(f"{BASE}/api/v1/database/")
    for db in resp.json().get("result", []):
        if "crypto" in db["database_name"].lower():
            return db["id"]
    print("Crypto Warehouse database not found!")
    sys.exit(1)


# Create a dataset (or return existing one), then refresh columns
def get_or_create_dataset(s, db_id, table_name, schema="analytics"):
    resp = s.get(
        f"{BASE}/api/v1/dataset/",
        params={
            "q": json.dumps(
                {"filters": [{"col": "table_name", "opr": "eq", "value": table_name}]}
            )
        },
    )
    results = resp.json().get("result", [])
    if results:
        ds_id = results[0]["id"]
        print(f"  Dataset '{table_name}' already exists (id={ds_id})")
    else:
        resp = s.post(
            f"{BASE}/api/v1/dataset/",
            json={
                "database": db_id,
                "schema": schema,
                "table_name": table_name,
            },
        )
        if resp.status_code == 201:
            ds_id = resp.json()["id"]
            print(f"  Created dataset '{table_name}' (id={ds_id})")
        else:
            print(
                f"  Failed to create dataset '{table_name}': {resp.status_code} {resp.text}"
            )
            return None

    # Refresh to sync columns
    s.put(f"{BASE}/api/v1/dataset/{ds_id}/refresh", json={})
    return ds_id


# Create a chart with query_context so it renders on dashboards 
def create_chart(s, name, ds_id, viz_type, params, query_context):
    resp = s.get(
        f"{BASE}/api/v1/chart/",
        params={
            "q": json.dumps(
                {"filters": [{"col": "slice_name", "opr": "eq", "value": name}]}
            )
        },
    )
    results = resp.json().get("result", [])

    payload = {
        "slice_name": name,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "viz_type": viz_type,
        "params": json.dumps(params),
        "query_context": json.dumps(query_context),
    }

    if results:
        # Chart exists? update it in-place via PUT so config changes take effect on re-runs
        chart_id = results[0]["id"]
        resp = s.put(f"{BASE}/api/v1/chart/{chart_id}", json=payload)
        if resp.status_code == 200:
            print(f"  Updated chart '{name}' (id={chart_id})")
        else:
            print(f"  Failed to update chart '{name}': {resp.status_code} {resp.text}")
        return chart_id

    resp = s.post(f"{BASE}/api/v1/chart/", json=payload)
    if resp.status_code == 201:
        chart_id = resp.json()["id"]
        print(f"  Created chart '{name}' (id={chart_id})")
        return chart_id
    else:
        print(f"  Failed to create chart '{name}': {resp.status_code} {resp.text}")
        return None

# Create a dashboard with the given charts in a 2-column grid layout
def create_dashboard(s, title, chart_ids):
    resp = s.get(
        f"{BASE}/api/v1/dashboard/",
        params={
            "q": json.dumps(
                {"filters": [{"col": "dashboard_title", "opr": "eq", "value": title}]}
            )
        },
    )
    results = resp.json().get("result", [])
    if results:
        print(f"  Dashboard '{title}' already exists (id={results[0]['id']})")
        return results[0]["id"]

    resp = s.post(
        f"{BASE}/api/v1/dashboard/",
        json={
            "dashboard_title": title,
            "published": True,
        },
    )
    if resp.status_code != 201:
        print(f"  Failed to create dashboard: {resp.status_code} {resp.text}")
        return None

    dash_id = resp.json()["id"]
    print(f"  Created dashboard '{title}' (id={dash_id})")

    valid_ids = [cid for cid in chart_ids if cid is not None]
    if valid_ids:
        # Build Superset v2 position layout: ROOT -> GRID -> ROWs -> CHARTs
        position = {"DASHBOARD_VERSION_KEY": "v2"}
        row_ids = []

        # Place charts 2 per row
        for i in range(0, len(valid_ids), 2):
            row_key = f"ROW-{i // 2}"
            row_children = []

            for j, cid in enumerate(valid_ids[i : i + 2]):
                chart_key = f"CHART-{cid}"
                position[chart_key] = {
                    "type": "CHART",
                    "id": chart_key,
                    "children": [],
                    "meta": {
                        "width": 6,
                        "height": 50,
                        "chartId": cid,
                    },
                }
                row_children.append(chart_key)

            position[row_key] = {
                "type": "ROW",
                "id": row_key,
                "children": row_children,
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            row_ids.append(row_key)

        # Header
        header_key = "HEADER_ID"
        position[header_key] = {
            "type": "HEADER",
            "id": header_key,
            "meta": {"text": title},
        }

        position["ROOT_ID"] = {
            "type": "ROOT",
            "id": "ROOT_ID",
            "children": ["GRID_ID"],
        }
        position["GRID_ID"] = {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [header_key] + row_ids,
        }

        resp = s.put(
            f"{BASE}/api/v1/dashboard/{dash_id}",
            json={
                "position_json": json.dumps(position),
            },
        )
        print(f"  Layout update: {resp.status_code}")

        # Associate each chart with the dashboard
        for cid in valid_ids:
            s.put(f"{BASE}/api/v1/chart/{cid}", json={"dashboards": [dash_id]})
        print(f"  Linked {len(valid_ids)} charts to dashboard")

    return dash_id


def main():
    print("Connecting to Superset API...")
    s = get_session()

    print("\nFinding Crypto Warehouse database...")
    db_id = get_database_id(s)
    print(f"  Database ID: {db_id}")

    print("\nCreating datasets...")
    ds_regime = get_or_create_dataset(s, db_id, "mart_market_regime")
    ds_summary = get_or_create_dataset(s, db_id, "mart_daily_summary")
    ds_correlation = get_or_create_dataset(s, db_id, "mart_cross_asset_correlation")
    ds_exchange = get_or_create_dataset(s, db_id, "mart_exchange_market_share")

    print("\nCreating charts...")
    chart_ids = []

    # Helper for common metric format
    def metric(agg, col, label):
        return {
            "expressionType": "SIMPLE",
            "aggregate": agg,
            "column": {"column_name": col},
            "label": label,
        }

    # Helper to build query_context for charts - abstracts common structure and allows flexible filters, groupby, orderby, etc.
    def qc(ds_id, columns, metrics, filters=None, row_limit=100, orderby=None):
        """Build query_context for Superset 6 chart rendering."""
        q = {"columns": columns, "metrics": metrics, "row_limit": row_limit}
        if filters:
            q["filters"] = filters
        if orderby:
            q["orderby"] = orderby
        return {
            "datasource": {"id": ds_id, "type": "table"},
            "force": False,
            "queries": [q],
            "result_format": "json",
            "result_type": "full",
        }

    # Chart 1: Market Regime Distribution (pie chart)
    m1 = metric("COUNT", "price_date", "Count of Days")
    chart_ids.append(
        create_chart(
            s,
            "Market Regime Distribution",
            ds_regime,
            "pie",
            {
                "metric": m1,
                "groupby": ["market_regime"],
                "color_scheme": "supersetColors",
                "show_labels": True,
                "show_legend": True,
                "label_type": "key_percent",
                "row_limit": 100,
                "viz_type": "pie",
                "datasource": f"{ds_regime}__table",
            },
            qc(ds_regime, ["market_regime"], [m1]),
        )
    )

    # Chart 2: BTC Price Over Time (line chart)
    m2 = metric("AVG", "price_usd", "Avg Price USD")
    chart_ids.append(
        create_chart(
            s,
            "Bitcoin Price (90 Days)",
            ds_summary,
            "echarts_timeseries_line",
            {
                "x_axis": "price_date",
                "time_grain_sqla": "P1D",
                "metrics": [m2],
                "adhoc_filters": [
                    {
                        "clause": "WHERE",
                        "comparator": "bitcoin",
                        "expressionType": "SIMPLE",
                        "operator": "==",
                        "subject": "coin_id",
                    }
                ],
                "row_limit": 10000,
                "rich_tooltip": True,
                "show_legend": True,
                "viz_type": "echarts_timeseries_line",
                "datasource": f"{ds_summary}__table",
            },
            qc(
                ds_summary,
                [
                    {
                        "sqlExpression": "price_date",
                        "label": "price_date",
                        "expressionType": "SQL",
                    }
                ],
                [m2],
                filters=[{"col": "coin_id", "op": "==", "val": "bitcoin"}],
                row_limit=10000,
            ),
        )
    )

    # Chart 3: Volume Anomaly Count by Asset (stacked bar: high vs low anomaly)
    # Splits by volume_flag so viewers see immediately whether anomalies are
    # high-volume spikes (whale/news) vs low-volume crashes (liquidity dry-up).
    m3 = metric("COUNT", "price_date", "Anomaly Days")
    chart_ids.append(
        create_chart(
            s,
            "Volume Anomaly Days by Asset",
            ds_summary,
            "echarts_timeseries_bar",
            {
                "metrics": [m3],
                "x_axis": "coin_id",
                "groupby": ["volume_flag"],
                "stack": True,
                "adhoc_filters": [
                    {
                        "clause": "WHERE",
                        "comparator": "normal",
                        "expressionType": "SIMPLE",
                        "operator": "!=",
                        "subject": "volume_flag",
                    }
                ],
                "row_limit": 100,
                "show_legend": True,
                "color_scheme": "supersetColors",
                "viz_type": "echarts_timeseries_bar",
                "datasource": f"{ds_summary}__table",
            },
            qc(
                ds_summary,
                [
                    {
                        "sqlExpression": "coin_id",
                        "label": "coin_id",
                        "expressionType": "SQL",
                    },
                    {
                        "sqlExpression": "volume_flag",
                        "label": "volume_flag",
                        "expressionType": "SQL",
                    },
                ],
                [m3],
                filters=[{"col": "volume_flag", "op": "!=", "val": "normal"}],
            ),
        )
    )

    # Chart 4: Exchange Market Share (pie)
    # Shows all 30 exchanges so market_share_pct (calculated against all 30) is honest.
    # Filtering to a subset causes Superset to renormalize slices, making tooltip values
    # disagree with the visual percentages and misrepresenting any exchange's true market share.
    m4 = metric("AVG", "market_share_pct", "Avg Market Share %")
    chart_ids.append(
        create_chart(
            s,
            "Exchange Market Share (Top 10)",
            ds_exchange,
            "pie",
            {
                "metric": m4,
                "groupby": ["exchange_name"],
                "adhoc_filters": [],
                "row_limit": 10,
                "show_labels": True,
                "show_legend": True,
                "label_type": "key_percent",
                "color_scheme": "supersetColors",
                "viz_type": "pie",
                "datasource": f"{ds_exchange}__table",
            },
            qc(
                ds_exchange,
                ["exchange_name"],
                [m4],
                filters=[],
                row_limit=10,
            ),
        )
    )

    # Chart 5: BTC Correlation Over Time (multi-line)
    # Shows rolling 30-day correlation with BTC for 6 key altcoins over the 90-day window.
    # A line chart reveals whether correlation is stable or oscillating — if ETH's line
    # hugs 0.9 while TRON bounces between 0.3 and 0.8, that's a diversification signal
    # a single avg bar can never convey.
    m5 = metric("AVG", "correlation_30d", "Correlation")
    chart_ids.append(
        create_chart(
            s,
            "BTC Correlation by Altcoin",
            ds_correlation,
            "echarts_timeseries_line",
            {
                "x_axis": "price_date",
                "time_grain_sqla": "P1D",
                "metrics": [m5],
                "groupby": ["coin_id"],
                "adhoc_filters": [
                    {
                        "clause": "WHERE",
                        "comparator": [
                            "ethereum",
                            "solana",
                            "cardano",
                            "ripple",
                            "tron",
                            "cosmos",
                        ],
                        "expressionType": "SIMPLE",
                        "operator": "IN",
                        "subject": "coin_id",
                    }
                ],
                "row_limit": 10000,
                "rich_tooltip": True,
                "show_legend": True,
                "color_scheme": "supersetColors",
                "viz_type": "echarts_timeseries_line",
                "datasource": f"{ds_correlation}__table",
            },
            qc(
                ds_correlation,
                [
                    {
                        "sqlExpression": "price_date",
                        "label": "price_date",
                        "expressionType": "SQL",
                    },
                    {
                        "sqlExpression": "coin_id",
                        "label": "coin_id",
                        "expressionType": "SQL",
                    },
                ],
                [m5],
                filters=[
                    {
                        "col": "coin_id",
                        "op": "IN",
                        "val": [
                            "ethereum",
                            "solana",
                            "cardano",
                            "ripple",
                            "tron",
                            "cosmos",
                        ],
                    }
                ],
                row_limit=10000,
            ),
        )
    )

    # Chart 6: Drawdown Severity Distribution (stacked bar by drawdown_severity)
    # Shows how many of the 90 days each asset spent in critical/moderate/mild drawdown.
    # "Solana spent 45 days in critical drawdown" is a clearer risk signal than a
    # single -38% bar, and the three-color stack gives immediate severity triage.
    m6 = metric("COUNT", "price_date", "Days")
    chart_ids.append(
        create_chart(
            s,
            "Current Drawdown from Peak",
            ds_summary,
            "echarts_timeseries_bar",
            {
                "metrics": [m6],
                "x_axis": "coin_id",
                "groupby": ["drawdown_severity"],
                "stack": True,
                "row_limit": 200,
                "show_legend": True,
                "color_scheme": "supersetColors",
                "viz_type": "echarts_timeseries_bar",
                "datasource": f"{ds_summary}__table",
            },
            qc(
                ds_summary,
                [
                    {
                        "sqlExpression": "coin_id",
                        "label": "coin_id",
                        "expressionType": "SQL",
                    },
                    {
                        "sqlExpression": "drawdown_severity",
                        "label": "drawdown_severity",
                        "expressionType": "SQL",
                    },
                ],
                [m6],
                row_limit=200,
            ),
        )
    )

    print("\nCreating dashboard...")
    dash_id = create_dashboard(s, "Crypto Market Intelligence", chart_ids)

    # Save useful ad-hoc queries in SQL Lab for quick business reviews
    print("\nSaving SQL queries...")
    saved_queries = [
        {
            "label": "Current Market Snapshot",
            "description": "Latest price, regime, drawdown, and anomaly status for every coin. Use in weekly reviews.",
            "sql": (
                "SELECT coin_id, price_usd, market_regime, volatility_regime,\n"
                "       drawdown_pct, drawdown_severity, volume_flag\n"
                "FROM analytics.mart_daily_summary\n"
                "WHERE price_date = (SELECT MAX(price_date) FROM analytics.mart_daily_summary)\n"
                "ORDER BY price_usd DESC;"
            ),
        },
        {
            "label": "Regime Transitions (Last 7 Days)",
            "description": "Shows which coins changed market regime recently - catches inflection points for metric reviews.",
            "sql": (
                "SELECT coin_id, price_date, market_regime,\n"
                "       LAG(market_regime) OVER (PARTITION BY coin_id ORDER BY price_date) AS prev_regime\n"
                "FROM analytics.mart_market_regime\n"
                "WHERE price_date >= CURRENT_DATE - INTERVAL '7 days'\n"
                "ORDER BY price_date DESC, coin_id;"
            ),
        },
        {
            "label": "Recent Volume Anomalies",
            "description": "Details on all volume anomaly events. Use when a PM asks 'what caused that spike?'",
            "sql": (
                "SELECT coin_id, price_date, volume_usd, volume_z_score, volume_flag,\n"
                "       price_usd, daily_return\n"
                "FROM analytics.mart_daily_summary\n"
                "WHERE volume_flag != 'normal'\n"
                "ORDER BY price_date DESC, ABS(volume_z_score) DESC;"
            ),
        },
        {
            "label": "Exchange Market Share Trend",
            "description": "Competitive positioning over time - volume, rank, and share for all exchanges.",
            "sql": (
                "SELECT snapshot_date, exchange_name, volume_btc_24h,\n"
                "       market_share_pct, volume_rank\n"
                "FROM analytics.mart_exchange_market_share\n"
                "ORDER BY snapshot_date DESC, volume_rank;"
            ),
        },
        {
            "label": "Assets in Critical Drawdown",
            "description": "Risk triage - shows coins currently in critical or moderate drawdown with volatility context.",
            "sql": (
                "SELECT coin_id, price_usd, drawdown_pct, drawdown_severity,\n"
                "       volatility_30d, volatility_regime\n"
                "FROM analytics.mart_daily_summary\n"
                "WHERE price_date = (SELECT MAX(price_date) FROM analytics.mart_daily_summary)\n"
                "  AND drawdown_severity IN ('critical', 'moderate')\n"
                "ORDER BY drawdown_pct ASC;"
            ),
        },
    ]

    for q in saved_queries:
        # Check if query already exists by label
        resp = s.get(
            f"{BASE}/api/v1/saved_query/",
            params={
                "q": json.dumps(
                    {"filters": [{"col": "label", "opr": "eq", "value": q["label"]}]}
                )
            },
        )
        results = resp.json().get("result", [])
        payload = {
            "label": q["label"],
            "description": q["description"],
            "sql": q["sql"],
            "db_id": db_id,
            "schema": "analytics",
        }
        if results:
            query_id = results[0]["id"]
            resp = s.put(f"{BASE}/api/v1/saved_query/{query_id}", json=payload)
            if resp.status_code == 200:
                print(f"  Updated query '{q['label']}' (id={query_id})")
            else:
                print(f"  Failed to update query '{q['label']}': {resp.status_code}")
        else:
            resp = s.post(f"{BASE}/api/v1/saved_query/", json=payload)
            if resp.status_code == 201:
                print(f"  Saved query '{q['label']}' (id={resp.json()['id']})")
            else:
                print(f"  Failed to save query '{q['label']}': {resp.status_code}")

    print(f"\nDone! Open http://localhost:8088/superset/dashboard/{dash_id}/ to view.")
    print(
        "Tip: Saved queries are in SQL Lab > Saved Queries. Click the 'All' tab to see all queries."
    )


if __name__ == "__main__":
    main()
