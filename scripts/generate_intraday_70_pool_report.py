#!/usr/bin/env python3
"""Export an auditable same-day outcome report for ChiNext and STAR 70-point alerts."""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATABASE = BASE_DIR / "data" / "backtest.sqlite"
REPORT_DIR = BASE_DIR / "reports"
TRADE_DATE = "2026-09-09"


def pct(value: float) -> str:
    return f"{value:+.2f}%"


def main() -> None:
    connection = sqlite3.connect(DATABASE)
    alerts = connection.execute(
        """
        SELECT code, name, alert_time, price, score
        FROM alert_events
        WHERE trade_date = ? AND alert_kind = 'score' AND threshold = 70
          AND (code LIKE '300%' OR code LIKE '301%' OR code LIKE '688%' OR code LIKE '689%')
        ORDER BY alert_time, code
        """,
        (TRADE_DATE,),
    ).fetchall()

    first_alerts = {}
    for code, name, alert_time, price, score in alerts:
        first_alerts.setdefault(code, (code, name, alert_time, price, score))

    traces = defaultdict(list)
    for row in connection.execute(
        """
        SELECT code, minute, price, score
        FROM score_traces
        WHERE trade_date = ? AND minute <= '15:00'
          AND (code LIKE '300%' OR code LIKE '301%' OR code LIKE '688%' OR code LIKE '689%')
        ORDER BY code, minute
        """,
        (TRADE_DATE,),
    ):
        if row[0] in first_alerts and row[2] is not None:
            traces[row[0]].append(row)

    categories = {"走强": [], "失败": [], "中性": []}
    for code, (code, name, alert_time, trigger_price, trigger_score) in first_alerts.items():
        rows = [row for row in traces[code] if row[1] >= alert_time[:5]]
        if not rows or not trigger_price:
            continue
        high = max(rows, key=lambda row: row[2])
        low = min(rows, key=lambda row: row[2])
        latest = rows[-1]
        high_gain = (high[2] / trigger_price - 1) * 100
        low_drawdown = (low[2] / trigger_price - 1) * 100
        item = {
            "code": code,
            "name": name,
            "alert_time": alert_time,
            "trigger_price": trigger_price,
            "trigger_score": trigger_score,
            "high": high,
            "low": low,
            "latest": latest,
            "high_gain": high_gain,
            "low_drawdown": low_drawdown,
        }
        if high_gain >= 5:
            categories["走强"].append(item)
        elif low_drawdown <= -3:
            categories["失败"].append(item)
        else:
            categories["中性"].append(item)

    total = sum(len(items) for items in categories.values())
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"# {TRADE_DATE} 创业板 / 科创板 70 分确认池盘后复盘",
        "",
        f"生成时间：{generated_at}",
        "",
        "## 判定口径",
        "",
        "- 样本：当日首次触发“至少两种策略评分不低于 70 分”的创业板、科创板股票。",
        "- 后续走强：触发后至 15:00 的最高价相对触发价上涨 ≥5%。",
        "- 后续失败：未达到后续走强，且触发后至 15:00 的最低价相对触发价回撤 ≥3%。",
        "- 中性：其余股票，保留为观察样本，不强行归类为成功或失败。",
        "",
        "## 汇总",
        "",
        "| 分类 | 数量 | 占全部确认样本比例 |",
        "|---|---:|---:|",
    ]
    for name in ("走强", "失败", "中性"):
        count = len(categories[name])
        lines.append(f"| {name} | {count} | {count / total:.2%} |")
    lines += [f"| 合计 | {total} | 100.00% |", ""]

    for category in ("走强", "失败", "中性"):
        items = categories[category]
        if category == "走强":
            items.sort(key=lambda item: item["high_gain"], reverse=True)
        elif category == "失败":
            items.sort(key=lambda item: item["low_drawdown"])
        else:
            items.sort(key=lambda item: item["code"])
        lines += [f"## 后续{category}（{len(items)} 只）", "", "| 股票 | 70分触发 | 触发价 / 触发分 | 后续最高（涨幅） | 后续最低（回撤） | 15:00价格 / 综合分 |", "|---|---|---:|---|---|---|"]
        for item in items:
            high_time, high_price, _ = item["high"][1:]
            low_time, low_price, _ = item["low"][1:]
            last_time, last_price, last_score = item["latest"][1:]
            lines.append(
                f"| {item['name']}（{item['code']}） | {item['alert_time']} | "
                f"{item['trigger_price']:.2f} / {item['trigger_score']:.0f} | "
                f"{high_time} {high_price:.2f}（{pct(item['high_gain'])}） | "
                f"{low_time} {low_price:.2f}（{pct(item['low_drawdown'])}） | "
                f"{last_time} {last_price:.2f} / {last_score:.0f} |"
            )
        lines.append("")

    REPORT_DIR.mkdir(exist_ok=True)
    destination = REPORT_DIR / f"{TRADE_DATE}-chinext-star-70-pool-review.md"
    destination.write_text("\n".join(lines), encoding="utf-8")
    print(f"report={destination}")
    print(f"total={total} strong={len(categories['走强'])} failed={len(categories['失败'])} neutral={len(categories['中性'])}")


if __name__ == "__main__":
    main()
