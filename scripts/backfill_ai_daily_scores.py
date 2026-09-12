#!/usr/bin/env python3
"""Backfill the latest N daily rebound scores for AI-compute stock groups."""

import argparse
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import market_server as market  # noqa: E402


SUPPORTED_GROUPS = ("cpo", "pcb", "fiber", "chip", "computeChip")


def load_stocks(groups):
    source = (ROOT / "trading-agents-stock.html").read_text(encoding="utf-8")
    stocks = {}
    memberships = {}
    for group in groups:
        match = re.search(
            rf"const\s+{group}Layers\s*=\s*\[(.*?)\n\s*\];",
            source,
            flags=re.S,
        )
        if not match:
            raise RuntimeError(f"未找到 {group}Layers")
        for name, code in re.findall(r"([\u4e00-\u9fffA-Za-z]+)\s+(\d{6})", match.group(1)):
            stocks[code] = name
            memberships.setdefault(code, set()).add(group)
    return stocks, memberships


def calculate_stock(code, name, days):
    try:
        stock = market.get_stock_data(code, True)
        history = stock.get("history", [])
    except market.MarketDataError:
        payload = market.request_json(market.EASTMONEY_DAILY_KLINE_URL, {
            "secid": market.eastmoney_secid(code),
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101", "fqt": "1", "lmt": "400",
            "end": "20500101", "ut": "fa5fd1943c7b386f172d6893dbfba10",
        })
        history = []
        for raw in (payload.get("data") or {}).get("klines") or []:
            parts = str(raw).split(",")
            if len(parts) < 6 or market.number(parts[2]) is None:
                continue
            history.append({
                "date": parts[0], "open": market.number(parts[1]),
                "close": market.number(parts[2]), "high": market.number(parts[3]),
                "low": market.number(parts[4]), "volume": market.number(parts[5]) or 0,
            })
        if not history:
            raise market.MarketDataError(f"{code} 东方财富日K备用接口无数据")
    history = [row for row in history if market.number(row.get("close")) is not None]
    rows = []
    for index in range(max(24, len(history) - days), len(history)):
        result = market.rebound_daily_score({"history": history[: index + 1]})
        if result.get("score") is None:
            continue
        rows.append(
            (
                str(history[index].get("date")),
                code,
                name,
                int(result["score"]),
                json.dumps(result.get("factors") or {}, ensure_ascii=False),
                datetime.now(market.CHINA_TZ).isoformat(),
            )
        )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--groups", default="cpo,pcb,fiber", help="Comma-separated layer groups")
    args = parser.parse_args()
    days = min(100, max(1, args.days))
    groups = tuple(dict.fromkeys(item.strip() for item in args.groups.split(",") if item.strip()))
    unknown = [item for item in groups if item not in SUPPORTED_GROUPS]
    if not groups or unknown:
        parser.error(f"不支持的分组: {','.join(unknown) if unknown else '空'}")
    stocks, memberships = load_stocks(groups)
    completed = {}
    failed = {}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(calculate_stock, code, name, days): (code, name)
            for code, name in stocks.items()
        }
        for future in as_completed(futures):
            code, name = futures[future]
            try:
                completed[code] = future.result()
            except Exception as exc:  # Keep the batch useful when one provider call fails.
                failed[code] = {"name": name, "error": str(exc)}

    records = [row for rows in completed.values() for row in rows]
    with market.open_backtest_db() as connection:
        connection.executemany(
            """INSERT OR REPLACE INTO rebound_daily_scores
               (trade_date,code,name,daily_score,factors_json,calculated_at)
               VALUES(?,?,?,?,?,?)""",
            records,
        )
        connection.commit()

    counts = {group: 0 for group in groups}
    for code in completed:
        for group in memberships[code]:
            counts[group] += 1
    print(json.dumps({
        "days": days,
        "uniqueStocks": len(stocks),
        "completedStocks": len(completed),
        "savedRecords": len(records),
        "completedByGroup": counts,
        "failed": failed,
    }, ensure_ascii=False, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
