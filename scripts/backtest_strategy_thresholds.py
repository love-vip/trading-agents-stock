#!/usr/bin/env python3
"""Compare intraday multi-strategy 70-point entry rules from saved minute traces."""

from __future__ import annotations

import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATABASE = BASE_DIR / "data" / "backtest.sqlite"

RULES = {
    "至少2项≥70": lambda values: sum(score >= 70 for score in values) >= 2,
    "至少2项≥72": lambda values: sum(score >= 72 for score in values) >= 2,
    "至少2项≥73": lambda values: sum(score >= 73 for score in values) >= 2,
    "至少2项≥74": lambda values: sum(score >= 74 for score in values) >= 2,
    "至少2项≥70，且最高≥75": lambda values: sum(score >= 70 for score in values) >= 2 and max(values, default=0) >= 75,
}


def classify(trigger_price: float, rows: list[tuple]) -> str:
    high = max(row[2] for row in rows)
    low = min(row[2] for row in rows)
    high_gain = (high / trigger_price - 1) * 100
    low_drawdown = (low / trigger_price - 1) * 100
    if high_gain >= 5:
        return "走强"
    if low_drawdown <= -3:
        return "失败"
    return "中性"


def main() -> None:
    trade_date = sys.argv[1] if len(sys.argv) > 1 else "2026-09-09"
    connection = sqlite3.connect(DATABASE)
    rows_by_code = defaultdict(list)
    names = {}
    for code, name, minute, price, base_json in connection.execute(
        """
        SELECT code, name, minute, price, base_json
        FROM score_traces
        WHERE trade_date = ? AND minute <= '15:00'
          AND (code LIKE '300%' OR code LIKE '301%' OR code LIKE '688%' OR code LIKE '689%')
        ORDER BY code, minute
        """,
        (trade_date,),
    ):
        base = json.loads(base_json or "{}")
        scores = base.get("_strategyScores") or {}
        if len(scores) == 4 and price is not None:
            rows_by_code[code].append((minute, scores, float(price)))
            names[code] = name or code

    if not rows_by_code:
        print(f"{trade_date}: no per-minute strategy score trajectories are available")
        return

    print(f"{trade_date} | usable stocks: {len(rows_by_code)}")
    print("rule\tsamples\tstrong\tfailed\tneutral\tstrong/all\tstrong/(strong+failed)")
    for label, rule in RULES.items():
        buckets = defaultdict(int)
        for code, rows in rows_by_code.items():
            trigger_index = next((index for index, (_, scores, _) in enumerate(rows) if rule(list(map(float, scores.values())))), None)
            if trigger_index is None:
                continue
            trigger_price = rows[trigger_index][2]
            future = rows[trigger_index:]
            buckets[classify(trigger_price, future)] += 1
        total = sum(buckets.values())
        decisive = buckets["走强"] + buckets["失败"]
        all_rate = buckets["走强"] / total if total else 0
        decisive_rate = buckets["走强"] / decisive if decisive else 0
        print(f"{label}\t{total}\t{buckets['走强']}\t{buckets['失败']}\t{buckets['中性']}\t{all_rate:.2%}\t{decisive_rate:.2%}")


if __name__ == "__main__":
    main()
