#!/usr/bin/env python3
"""Create an auditable intraday outcome report for composite-score >=75 entries."""

from __future__ import annotations

import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATABASE = BASE_DIR / "data" / "backtest.sqlite"
REPORT_DIR = BASE_DIR / "reports"


def pct(value: float) -> str:
    return f"{value:+.2f}%"


def main() -> None:
    trade_date = sys.argv[1] if len(sys.argv) > 1 else "2026-09-09"
    connection = sqlite3.connect(DATABASE)
    traces = defaultdict(list)
    for code, name, minute, price, score in connection.execute(
        """
        SELECT code, name, minute, price, score
        FROM score_traces
        WHERE trade_date = ? AND minute <= '15:00'
          AND score IS NOT NULL AND price IS NOT NULL
          AND (code LIKE '300%' OR code LIKE '301%' OR code LIKE '688%' OR code LIKE '689%')
        ORDER BY code, minute
        """,
        (trade_date,),
    ):
        traces[code].append((name or code, minute, float(price), float(score)))

    categories = {"走强": [], "失败": [], "中性": []}
    for code, rows in traces.items():
        trigger_index = next((index for index, row in enumerate(rows) if row[3] >= 75), None)
        if trigger_index is None:
            continue
        name, trigger_time, trigger_price, trigger_score = rows[trigger_index]
        future = rows[trigger_index:]
        high = max(future, key=lambda row: row[2])
        low = min(future, key=lambda row: row[2])
        latest = future[-1]
        high_gain = (high[2] / trigger_price - 1) * 100
        low_drawdown = (low[2] / trigger_price - 1) * 100
        item = (code, name, trigger_time, trigger_price, trigger_score, high, low, latest, high_gain, low_drawdown)
        if high_gain >= 5:
            categories["走强"].append(item)
        elif low_drawdown <= -3:
            categories["失败"].append(item)
        else:
            categories["中性"].append(item)

    total = sum(len(rows) for rows in categories.values())
    lines = [
        f"# {trade_date} 创业板 / 科创板 综合评分 ≥75 分池盘后复盘",
        "",
        f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 判定口径",
        "",
        "- 样本：分钟级综合评分首次达到或超过 75 分的创业板、科创板股票。",
        "- 后续走强：入池后至 15:00 的最高价相对入池价上涨 ≥5%。",
        "- 后续失败：未达到后续走强，且最低价相对入池价回撤 ≥3%。",
        "- 中性：其余股票。",
        "",
        "## 汇总",
        "",
        "| 分类 | 数量 | 占全部入池样本比例 |",
        "|---|---:|---:|",
    ]
    for category in ("走强", "失败", "中性"):
        count = len(categories[category])
        lines.append(f"| {category} | {count} | {count / total:.2%} |")
    lines += [f"| 合计 | {total} | 100.00% |", ""]

    for category in ("走强", "失败", "中性"):
        items = categories[category]
        items.sort(key=(lambda row: row[8]), reverse=category == "走强")
        lines += [f"## 后续{category}（{len(items)} 只）", "", "| 股票 | ≥75入池 | 入池价 / 综合分 | 后续最高（涨幅） | 后续最低（回撤） | 15:00价格 / 综合分 |", "|---|---|---:|---|---|---|"]
        for code, name, time, price, score, high, low, latest, high_gain, low_drawdown in items:
            lines.append(
                f"| {name}（{code}） | {time} | {price:.2f} / {score:.0f} | "
                f"{high[1]} {high[2]:.2f}（{pct(high_gain)}） | "
                f"{low[1]} {low[2]:.2f}（{pct(low_drawdown)}） | "
                f"{latest[1]} {latest[2]:.2f} / {latest[3]:.0f} |"
            )
        lines.append("")

    REPORT_DIR.mkdir(exist_ok=True)
    destination = REPORT_DIR / f"{trade_date}-chinext-star-composite-75-review.md"
    destination.write_text("\n".join(lines), encoding="utf-8")
    decisive = len(categories["走强"]) + len(categories["失败"])
    print(f"report={destination}")
    print(f"total={total} strong={len(categories['走强'])} failed={len(categories['失败'])} neutral={len(categories['中性'])} decisive_success={len(categories['走强']) / decisive:.2%}" if decisive else "no decisive samples")


if __name__ == "__main__":
    main()
