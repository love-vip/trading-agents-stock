#!/usr/bin/env python3
"""Serve a sector-based TradingAgents stock selector with live A-share quotes."""

import argparse
import json
import math
import statistics
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse


BASE_DIR = Path(__file__).resolve().parent
HTML_FILE = BASE_DIR / "trading-agents-stock.html"
QUOTE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
EASTMONEY_LIST_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
EASTMONEY_MINUTE_KLINE_URL = "https://push2delay.eastmoney.com/api/qt/stock/kline/get"
EASTMONEY_HOT_CONCEPT_URL = "https://emappdata.eastmoney.com/stockrank/getHotStockRankList"
CHINA_TZ = timezone(timedelta(hours=8))
HISTORY_DAYS = 400
CACHE_TTL_SECONDS = 20
INTRADAY_CACHE_TTL_SECONDS = 300
DYNAMIC_CACHE_TTL_SECONDS = 30
STOCK_CACHE_TTL_SECONDS = 300
DOWNTREND_HISTORY_DAYS = 30
DOWNTREND_MIN_HISTORY_DAYS = 25
DOWNTREND_CACHE_TTL_SECONDS = 300
DOWNTREND_CANDIDATE_BUFFER = 20
DOWNTREND_MAX_WORKERS = 8
CONCEPT_CACHE_TTL_SECONDS = 600
REQUEST_TIMEOUT_SECONDS = 10
MARKET_PAGE_SIZE = 100

SECTOR_ORDER = ("semiconductor", "new-energy", "consumer", "medicine", "ai-computing")
SECTORS: Dict[str, Dict[str, Any]] = {
    "overview": {
        "label": "市场总览",
        "shortLabel": "总览",
        "description": "跨板块观察核心代表股，快速比较行情强弱与风险结构。",
        "stocks": (),
    },
    "semiconductor": {
        "label": "半导体",
        "shortLabel": "芯片",
        "description": "覆盖材料、设备、设计与制造环节，观察国产替代和周期弹性。",
        "stocks": (
            ("300346", "光刻胶 / 电子特气"),
            ("688981", "晶圆代工"),
            ("002371", "半导体设备"),
            ("603501", "CMOS 图像传感器"),
            ("688012", "刻蚀与薄膜设备"),
            ("002049", "特种集成电路"),
        ),
    },
    "new-energy": {
        "label": "新能源",
        "shortLabel": "新能源",
        "description": "覆盖动力电池、整车、光伏、储能和锂资源产业链。",
        "stocks": (
            ("300750", "动力电池 / 储能"),
            ("002594", "新能源汽车"),
            ("601012", "光伏硅片 / 组件"),
            ("300274", "光伏逆变器 / 储能"),
            ("002466", "锂资源"),
            ("300014", "锂电池"),
        ),
    },
    "consumer": {
        "label": "消费",
        "shortLabel": "消费",
        "description": "覆盖白酒、食品饮料、家电和免税消费，偏重现金流与防御属性。",
        "stocks": (
            ("600519", "高端白酒"),
            ("000858", "高端白酒"),
            ("600887", "乳制品"),
            ("603288", "调味品"),
            ("000333", "家电"),
            ("601888", "免税消费"),
        ),
    },
    "medicine": {
        "label": "医药",
        "shortLabel": "医药",
        "description": "覆盖医疗器械、创新药、医疗服务、疫苗与研发服务。",
        "stocks": (
            ("300760", "医疗器械"),
            ("600276", "创新药"),
            ("000538", "中药 / 健康消费"),
            ("300015", "眼科医疗"),
            ("300122", "生物疫苗"),
            ("603259", "医药研发服务"),
        ),
    },
    "ai-computing": {
        "label": "AI 算力",
        "shortLabel": "AI 算力",
        "description": "覆盖服务器、算力基础设施、光模块、光通信与 AI 芯片。",
        "stocks": (
            ("601138", "AI 服务器"),
            ("000977", "服务器 / 算力基础设施"),
            ("603019", "高性能计算"),
            ("300308", "高速光模块"),
            ("002281", "光通信器件"),
            ("688256", "AI 芯片"),
        ),
    },
}
OVERVIEW_CODES = (
    "300346",
    "688981",
    "300750",
    "002594",
    "600519",
    "000333",
    "300760",
    "600276",
    "601138",
    "300308",
)

STOCK_META: Dict[str, Dict[str, str]] = {}
for sector_key in SECTOR_ORDER:
    for stock_code, stock_theme in SECTORS[sector_key]["stocks"]:
        STOCK_META[stock_code] = {
            "sectorKey": sector_key,
            "industry": SECTORS[sector_key]["label"],
            "theme": stock_theme,
        }

MARKET_SCOPES: Dict[str, Dict[str, Any]] = {
    "all": {
        "label": "全部类型",
        "description": "汇总主板、创业板、科创板与北交所，覆盖 A 股主要交易市场。",
        "prefixes": (),
    },
    "sh-main": {
        "label": "主板上海",
        "description": "扫描上海证券交易所主板，排除科创板代码。",
        "fs": "m:1+t:2",
        "prefixes": ("600", "601", "603", "605"),
    },
    "sz-main": {
        "label": "主板深圳",
        "description": "扫描深圳证券交易所主板，排除创业板代码。",
        "fs": "m:0+t:6",
        "prefixes": ("000", "001", "002", "003"),
    },
    "chinext": {
        "label": "创业板",
        "description": "扫描创业板上市公司，覆盖 300 与 301 代码段。",
        "fs": "m:0+t:80",
        "prefixes": ("300", "301"),
    },
    "star": {
        "label": "科创板",
        "description": "扫描科创板上市公司，覆盖 688 与 689 代码段。",
        "fs": "m:1+t:23",
        "prefixes": ("688", "689"),
    },
    "bse": {
        "label": "北交所",
        "description": "扫描北交所上市公司，排除新三板混合行情。",
        "fs": "m:0+t:81+s:2048",
        "prefixes": ("4", "8", "92"),
    },
}

ALL_MARKET_SCOPE_KEYS = ("sh-main", "sz-main", "chinext", "star", "bse")

_cache_lock = threading.Lock()
_cache: Dict[str, Dict[str, Any]] = {}
_intraday_cache: Dict[str, Dict[str, Any]] = {}
_dynamic_cache: Dict[str, Dict[str, Any]] = {}
_sector_strength_cache: Dict[str, Dict[str, Any]] = {}
_stock_cache: Dict[str, Dict[str, Any]] = {}
_downtrend_history_cache: Dict[str, Dict[str, Any]] = {}
_concept_cache: Dict[str, Dict[str, Any]] = {}


class MarketDataError(RuntimeError):
    pass


def stock_symbol(code: str) -> str:
    exchange = "sh" if code.startswith(("5", "6", "9")) else "sz"
    return f"{exchange}{code}"


def stock_rank_symbol(code: str) -> str:
    exchange = "SH" if code.startswith(("5", "6", "9")) else "SZ"
    return f"{exchange}{code}"


def eastmoney_secid(code: str) -> str:
    market = "1" if code.startswith(("5", "6")) else "0"
    return f"{market}.{code}"


def request_json(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    full_url = f"{url}?{urlencode(params, safe=',')}"
    try:
        result = subprocess.run(
            [
                "curl",
                "-fsSL",
                "--max-time",
                str(REQUEST_TIMEOUT_SECONDS),
                "--retry",
                "2",
                "--retry-delay",
                "0",
                full_url,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=REQUEST_TIMEOUT_SECONDS + 2,
        )
        return json.loads(result.stdout)
    except (
        FileNotFoundError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        json.JSONDecodeError,
    ) as exc:
        raise MarketDataError(f"行情服务请求失败: {exc}") from exc


def request_json_post(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        result = subprocess.run(
            [
                "curl",
                "-fsSL",
                "--max-time",
                str(REQUEST_TIMEOUT_SECONDS),
                "--retry",
                "2",
                "--retry-delay",
                "0",
                "-X",
                "POST",
                "-H",
                "Content-Type: application/json",
                "--data",
                json.dumps(payload, ensure_ascii=True),
                url,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=REQUEST_TIMEOUT_SECONDS + 2,
        )
        return json.loads(result.stdout)
    except (
        FileNotFoundError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        json.JSONDecodeError,
    ) as exc:
        raise MarketDataError(f"概念数据请求失败: {exc}") from exc


def number(value: Any) -> Optional[float]:
    if value in (None, "", "-"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def clamp(value: float, minimum: float = 0, maximum: float = 100) -> int:
    return round(max(minimum, min(maximum, value)))


def return_pct(start: Optional[float], end: Optional[float]) -> float:
    if start in (None, 0) or end is None:
        return 0.0
    return (end / start - 1) * 100


def add_moving_averages(rows: List[Dict[str, Any]]) -> None:
    """Attach trailing close-price moving averages to the daily K-line rows."""
    for index, row in enumerate(rows):
        for period in (10, 20, 30):
            window = [item.get("close") for item in rows[index - period + 1 : index + 1]]
            valid_closes = [close for close in window if close is not None]
            row[f"ma{period}"] = (
                sum(valid_closes) / period if len(valid_closes) == period else None
            )


def normalized_trade_date(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y%m%d")
    except (TypeError, ValueError) as exc:
        raise MarketDataError("交易日格式应为 YYYY-MM-DD") from exc


def factor_band(value: int, strong: str, neutral: str, weak: str) -> str:
    if value >= 65:
        return strong
    if value <= 42:
        return weak
    return neutral


def calculate_factors(quote: Dict[str, Any]) -> Dict[str, int]:
    recent_history = quote["history"][-12:]
    closes = [row["close"] for row in recent_history if row.get("close") is not None]
    short_return = return_pct(closes[-6], closes[-1]) if len(closes) >= 6 else 0.0
    long_return = return_pct(closes[0], closes[-1]) if len(closes) >= 2 else 0.0
    daily_returns = [
        return_pct(closes[index - 1], closes[index])
        for index in range(1, len(closes))
        if closes[index - 1] not in (None, 0)
    ]
    volatility = statistics.pstdev(daily_returns) if len(daily_returns) >= 2 else 0.0
    change_pct = quote.get("changePct") or 0.0
    amount = max(quote.get("amount") or 0.0, 100_000_000)

    stability = clamp(96 - volatility * 7.5)
    risk = clamp(100 - stability + max(0, abs(change_pct) - 5) * 3)
    return {
        "trend": clamp(50 + long_return * 1.6 + short_return * 1.4),
        "momentum": clamp(50 + short_return * 2.8 + change_pct * 2.0),
        "liquidity": clamp(35 + math.log10(amount / 100_000_000) * 30),
        "strength": clamp(50 + change_pct * 5),
        "stability": stability,
        "risk": risk,
    }


def enrich_stock(quote: Dict[str, Any]) -> Dict[str, Any]:
    meta = STOCK_META.get(
        quote["code"],
        {
            "sectorKey": "dynamic",
            "industry": "全 A 股",
            "theme": "全市场动态扫描",
        },
    )
    factors = calculate_factors(quote)
    trend_text = factor_band(factors["trend"], "趋势偏强", "趋势震荡", "趋势偏弱")
    momentum_text = factor_band(factors["momentum"], "动量活跃", "动量中性", "动量承压")
    liquidity_text = factor_band(factors["liquidity"], "流动性充足", "流动性适中", "流动性偏低")
    risk_text = factor_band(factors["risk"], "波动风险偏高", "风险水平适中", "波动风险较低")
    change_text = f"{quote['changePct']:+.2f}%" if quote.get("changePct") is not None else "--"

    quote.update(
        {
            **meta,
            "base": factors,
            "tags": [meta["industry"], trend_text, liquidity_text],
            "reason": (
                f"{meta['theme']}方向，近 12 日{trend_text}，{momentum_text}，"
                f"当日涨跌 {change_text}。"
            ),
            "riskText": f"{risk_text}，风险分 {factors['risk']}，需结合板块整体强弱控制仓位。",
            "agents": {
                "trend": f"近 12 个交易日价格结构评估为{trend_text}，趋势分 {factors['trend']}。",
                "momentum": f"短周期价格变化与当日强弱综合为{momentum_text}，动量分 {factors['momentum']}。",
                "liquidity": f"基于当日成交额评估为{liquidity_text}，流动性分 {factors['liquidity']}。",
                "risk": f"近期收益波动对应风险分 {factors['risk']}；该分数只反映行情波动，不代表基本面风险。",
            },
        }
    )
    return quote


def fetch_stock(code: str) -> Dict[str, Any]:
    symbol = stock_symbol(code)
    payload = request_json(QUOTE_URL, {"param": f"{symbol},day,,,{HISTORY_DAYS},qfq"})
    data = (payload.get("data") or {}).get(symbol) or {}
    quote = ((data.get("qt") or {}).get(symbol) or [])
    if payload.get("code") != 0 or len(quote) < 38:
        raise MarketDataError(f"{code} 行情数据不完整")

    rows = []
    previous_close = None
    for fields in data.get("qfqday") or data.get("day") or []:
        if len(fields) < 6:
            continue
        close = number(fields[2])
        change_amount = close - previous_close if close is not None and previous_close is not None else None
        rows.append(
            {
                "date": fields[0],
                "open": number(fields[1]),
                "close": close,
                "high": number(fields[3]),
                "low": number(fields[4]),
                "volume": number(fields[5]),
                "changePct": return_pct(previous_close, close) if previous_close else None,
                "changeAmount": change_amount,
            }
        )
        previous_close = close

    add_moving_averages(rows)

    trade_parts = quote[35].split("/") if len(quote) > 35 else []
    raw_volume = number(trade_parts[1] if len(trade_parts) > 1 else quote[6])
    volume = raw_volume / 100 if raw_volume is not None and code.startswith("688") else raw_volume
    amount = number(trade_parts[2]) if len(trade_parts) > 2 else None
    try:
        updated_at = datetime.strptime(quote[30], "%Y%m%d%H%M%S").replace(tzinfo=CHINA_TZ)
    except (TypeError, ValueError):
        updated_at = None

    return enrich_stock(
        {
            "code": code,
            "name": quote[1] or code,
            "price": number(quote[3]),
            "changePct": number(quote[32]),
            "changeAmount": number(quote[31]),
            "volume": volume,
            "amount": amount,
            "high": number(quote[33]),
            "low": number(quote[34]),
            "open": number(quote[5]),
            "previousClose": number(quote[4]),
            "updatedAt": updated_at.isoformat() if updated_at else None,
            "history": rows,
        }
    )


def sector_codes(sector_key: str) -> Tuple[str, ...]:
    if sector_key == "overview":
        return OVERVIEW_CODES
    return tuple(code for code, _theme in SECTORS[sector_key]["stocks"])


def public_sector(sector_key: str) -> Dict[str, Any]:
    sector = SECTORS[sector_key]
    return {
        "key": sector_key,
        "label": sector["label"],
        "shortLabel": sector["shortLabel"],
        "description": sector["description"],
        "count": len(sector_codes(sector_key)),
    }


def fetch_market_data(sector_key: str) -> Dict[str, Any]:
    codes = sector_codes(sector_key)
    quotes: List[Dict[str, Any]] = []
    unavailable_codes: List[str] = []
    with ThreadPoolExecutor(max_workers=min(6, len(codes))) as executor:
        futures = {executor.submit(fetch_stock, code): code for code in codes}
        for future in as_completed(futures):
            code = futures[future]
            try:
                quotes.append(future.result())
            except MarketDataError:
                unavailable_codes.append(code)

    if not quotes:
        raise MarketDataError("行情服务未返回任何股票报价")

    quotes.sort(key=lambda item: codes.index(item["code"]))
    market_dates = [item["history"][-1]["date"] for item in quotes if item.get("history")]
    quote_timestamps = [item["updatedAt"] for item in quotes if item.get("updatedAt")]
    return {
        "source": "腾讯证券公开行情",
        "sector": public_sector(sector_key),
        "marketDate": max(market_dates) if market_dates else None,
        "updatedAt": max(quote_timestamps) if quote_timestamps else None,
        "fetchedAt": datetime.now(CHINA_TZ).isoformat(),
        "unavailableCodes": unavailable_codes,
        "quotes": quotes,
    }


def get_market_data(sector_key: str, force_refresh: bool = False) -> Dict[str, Any]:
    now = time.monotonic()
    with _cache_lock:
        cached = _cache.get(sector_key)
        if cached and now - cached["created_at"] < CACHE_TTL_SECONDS and not force_refresh:
            return cached["payload"]

    payload = fetch_market_data(sector_key)
    with _cache_lock:
        _cache[sector_key] = {"created_at": time.monotonic(), "payload": payload}
    return payload


def dynamic_factors(
    change_pct: float, amount: float, turnover: float, amplitude: float
) -> Dict[str, int]:
    """Score a full-market snapshot without inventing unavailable daily history."""
    liquidity = clamp(35 + math.log10(max(amount, 1) / 100_000_000) * 28)
    strength = clamp(50 + change_pct * 4.2)
    momentum = clamp(50 + change_pct * 3.1 + min(turnover, 15) * 1.1)
    trend = clamp(50 + change_pct * 2.3 + min(turnover, 12) * 0.8)
    stability = clamp(92 - amplitude * 1.9 - abs(change_pct) * 0.8)
    risk = clamp(100 - stability + max(0, abs(change_pct) - 5) * 2.4)
    return {
        "trend": trend,
        "momentum": momentum,
        "liquidity": liquidity,
        "strength": strength,
        "stability": stability,
        "risk": risk,
    }


def fetch_downtrend_history(code: str) -> List[Dict[str, Any]]:
    """Fetch only the daily bars needed by the dynamic downtrend filter."""
    symbol = stock_symbol(code)
    payload = request_json(
        QUOTE_URL,
        {"param": f"{symbol},day,,,{DOWNTREND_HISTORY_DAYS},qfq"},
    )
    data = (payload.get("data") or {}).get(symbol) or {}
    rows: List[Dict[str, Any]] = []
    for fields in data.get("qfqday") or data.get("day") or []:
        if len(fields) < 5:
            continue
        close = number(fields[2])
        if close is None:
            continue
        rows.append({"date": fields[0], "close": close})
    if payload.get("code") != 0 or not rows:
        raise MarketDataError(f"{code} 日线数据不足")
    return rows


def get_downtrend_history(code: str) -> List[Dict[str, Any]]:
    now = time.monotonic()
    with _cache_lock:
        cached = _downtrend_history_cache.get(code)
        if cached and now - cached["created_at"] < DOWNTREND_CACHE_TTL_SECONDS:
            return cached["payload"]

    history = fetch_downtrend_history(code)
    with _cache_lock:
        _downtrend_history_cache[code] = {
            "created_at": time.monotonic(),
            "payload": history,
        }
    return history


def analyze_steady_decline(history: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Identify sustained weak price structures without excluding normal pullbacks."""
    closes = [row["close"] for row in history if row.get("close") not in (None, 0)]
    if len(closes) < DOWNTREND_MIN_HISTORY_DAYS:
        return {
            "available": False,
            "excluded": False,
            "score": 0,
            "signals": [],
            "historyDays": len(closes),
        }

    latest = closes[-1]
    ma10 = sum(closes[-10:]) / 10
    ma20 = sum(closes[-20:]) / 20
    ma10_previous = sum(closes[-15:-5]) / 10
    ma20_previous = sum(closes[-25:-5]) / 20
    return_10 = return_pct(closes[-11], latest)
    return_20 = return_pct(closes[-21], latest)
    down_days = sum(
        closes[index] < closes[index - 1]
        for index in range(len(closes) - 10, len(closes))
    )
    high_20 = max(closes[-21:])
    drawdown_20 = return_pct(high_20, latest) * -1
    rebound_from_low = return_pct(min(closes[-5:]), latest)

    bearish_structure = (
        latest < ma10
        and latest < ma20
        and ma10 < ma20
        and ma10 < ma10_previous
        and ma20 < ma20_previous
    )
    cumulative_decline = return_10 <= -4 and return_20 <= -7
    persistent_down_days = down_days >= 6
    deep_drawdown = drawdown_20 >= 8
    failed_rebound = latest < ma10 and rebound_from_low <= 2.5

    score = 0
    signals: List[str] = []
    if bearish_structure:
        score += 35
        signals.append("均线空头")
    if cumulative_decline:
        score += 25
        signals.append("10/20 日累计走弱")
    if persistent_down_days:
        score += 20
        signals.append("近 10 日下跌天数偏多")
    if deep_drawdown:
        score += 15
        signals.append("距 20 日高点回撤较深")
    if failed_rebound:
        score += 10
        signals.append("短线反弹未收复 MA10")

    excluded = bearish_structure and cumulative_decline and (
        persistent_down_days or deep_drawdown or failed_rebound
    )
    return {
        "available": True,
        "excluded": excluded,
        "score": score,
        "signals": signals,
        "historyDays": len(closes),
        "return10": round(return_10, 2),
        "return20": round(return_20, 2),
        "drawdown20": round(drawdown_20, 2),
    }


def filter_steady_decline_quotes(quotes: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    if not quotes:
        return [], 0

    analyses: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=min(DOWNTREND_MAX_WORKERS, len(quotes))) as executor:
        futures = {
            executor.submit(get_downtrend_history, quote["code"]): quote["code"]
            for quote in quotes
        }
        for future in as_completed(futures):
            code = futures[future]
            try:
                analyses[code] = analyze_steady_decline(future.result())
            except MarketDataError:
                analyses[code] = {
                    "available": False,
                    "excluded": False,
                    "score": 0,
                    "signals": [],
                    "historyDays": 0,
                }

    filtered_quotes: List[Dict[str, Any]] = []
    excluded_count = 0
    for quote in quotes:
        analysis = analyses[quote["code"]]
        quote["screener"]["downtrend"] = analysis
        if analysis["excluded"]:
            excluded_count += 1
            continue
        filtered_quotes.append(quote)
    return filtered_quotes, excluded_count


def fetch_stock_concepts(code: str) -> List[Dict[str, Any]]:
    payload = request_json_post(
        EASTMONEY_HOT_CONCEPT_URL,
        {
            "appId": "appId01",
            "globalId": "786e4c21-70dc-435a-93bb-38",
            "srcSecurityCode": stock_rank_symbol(code),
        },
    )
    if payload.get("code") not in (0, "0"):
        raise MarketDataError(f"{code} 概念数据不可用")

    concepts: List[Dict[str, Any]] = []
    for row in payload.get("data") or []:
        name = str(row.get("conceptName") or "").strip()
        if not name:
            continue
        concepts.append(
            {
                "name": name,
                "heat": int(number(row.get("hitCount")) or 0),
            }
        )
    concepts.sort(key=lambda item: item["heat"], reverse=True)
    return concepts[:5]


def get_stock_concepts(code: str) -> List[Dict[str, Any]]:
    now = time.monotonic()
    with _cache_lock:
        cached = _concept_cache.get(code)
        if cached and now - cached["created_at"] < CONCEPT_CACHE_TTL_SECONDS:
            return cached["payload"]

    concepts = fetch_stock_concepts(code)
    with _cache_lock:
        _concept_cache[code] = {
            "created_at": time.monotonic(),
            "payload": concepts,
        }
    return concepts


def dynamic_snapshot(
    row: Dict[str, Any], updated_at: datetime, market_key: str
) -> Optional[Dict[str, Any]]:
    market = MARKET_SCOPES[market_key]
    code = str(row.get("f12") or "")
    name = str(row.get("f14") or "")
    price = number(row.get("f2"))
    change_pct = number(row.get("f3"))
    amount = number(row.get("f6"))
    turnover = number(row.get("f8"))
    amplitude = number(row.get("f7"))
    previous_close = number(row.get("f18"))
    if (
        len(code) != 6
        or not code.isdigit()
        or price in (None, 0)
        or change_pct is None
        or amount is None
        or turnover is None
        or amplitude is None
        or "ST" in name.upper()
    ):
        return None

    factors = dynamic_factors(change_pct, amount, turnover, amplitude)
    score = round(
        factors["trend"] * 0.22
        + factors["momentum"] * 0.28
        + factors["liquidity"] * 0.2
        + factors["strength"] * 0.2
        + factors["stability"] * 0.1
        - factors["risk"] * 0.1
    )
    movement = f"{change_pct:+.2f}%"
    return {
        "code": code,
        "name": name or code,
        "price": price,
        "changePct": change_pct,
        "changeAmount": price - previous_close if previous_close is not None else None,
        "volume": number(row.get("f5")),
        "amount": amount,
        "high": number(row.get("f15")),
        "low": number(row.get("f16")),
        "open": number(row.get("f17")),
        "previousClose": previous_close,
        "updatedAt": updated_at.isoformat(),
        "history": [],
        "sectorKey": market_key,
        "industry": market["label"],
        "actualIndustry": str(row.get("f100") or ""),
        "theme": "市场动态扫描",
        "marketCap": number(row.get("f20")),
        "floatMarketCap": number(row.get("f21")),
        "peTtm": number(row.get("f9")),
        "volumeRatio": number(row.get("f10")),
        "pb": number(row.get("f23")),
        "base": factors,
        "screener": {
            "turnover": turnover,
            "amplitude": amplitude,
            "snapshotScore": score,
        },
        "tags": [f"{market['label']}动态", f"换手 {turnover:.2f}%", f"振幅 {amplitude:.2f}%"],
        "reason": f"{market['label']}实时扫描：当日 {movement}，换手 {turnover:.2f}%，成交额 {amount / 100_000_000:.2f} 亿。",
        "riskText": f"快照波动风险分 {factors['risk']}；点击后可继续查看日 K 与上一交易日分时。",
        "agents": {
            "trend": f"基于当日涨跌与换手的快照趋势分 {factors['trend']}，不替代中期趋势判断。",
            "momentum": f"当日动量分 {factors['momentum']}，涨跌 {movement}，需结合盘中持续性复核。",
            "liquidity": f"当日成交额 {amount / 100_000_000:.2f} 亿，流动性分 {factors['liquidity']}。",
            "risk": f"振幅 {amplitude:.2f}% 与当日波动对应风险分 {factors['risk']}。",
        },
    }


def aggregate_sector_strength(quotes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aggregate today's industry breadth from the unfiltered market snapshot."""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for quote in quotes:
        industry = str(quote.get("actualIndustry") or "").strip()
        name = str(quote.get("name") or "").strip().upper()
        change_pct = quote.get("changePct")
        is_unbounded_new_listing = name.startswith(("N", "C")) or (
            change_pct is not None and abs(float(change_pct)) > 30.5
        )
        if industry and not is_unbounded_new_listing:
            grouped.setdefault(industry, []).append(quote)

    total_amount = sum(
        max(0.0, float(quote.get("amount") or 0))
        for members in grouped.values()
        for quote in members
    )
    sectors: List[Dict[str, Any]] = []
    for industry, members in grouped.items():
        if len(members) < 3:
            continue
        changes = [float(item["changePct"]) for item in members if item.get("changePct") is not None]
        if not changes:
            continue
        rising = sum(1 for value in changes if value > 0)
        falling = sum(1 for value in changes if value < 0)
        flat = len(changes) - rising - falling
        limit_up = sum(1 for value in changes if value >= 9.8)
        amount = sum(max(0.0, float(item.get("amount") or 0)) for item in members)
        average_change = statistics.fmean(changes)
        advance_ratio = rising / len(changes) * 100
        limit_up_ratio = limit_up / len(changes) * 100
        strength_score = clamp(
            50
            + average_change * 8
            + (advance_ratio - 50) * 0.35
            + min(limit_up_ratio, 20) * 0.8
        )
        leaders = sorted(
            members,
            key=lambda item: (
                item.get("changePct") or -100,
                item.get("screener", {}).get("snapshotScore") or 0,
                item.get("amount") or 0,
            ),
            reverse=True,
        )[:5]
        sectors.append(
            {
                "name": industry,
                "strengthScore": strength_score,
                "averageChange": round(average_change, 2),
                "advanceRatio": round(advance_ratio, 1),
                "risingCount": rising,
                "fallingCount": falling,
                "flatCount": flat,
                "limitUpCount": limit_up,
                "memberCount": len(changes),
                "amount": round(amount, 2),
                "amountShare": round(amount / total_amount * 100, 2) if total_amount else None,
                "leaders": [
                    {
                        "code": item["code"],
                        "name": item["name"],
                        "price": item.get("price"),
                        "changePct": item.get("changePct"),
                        "amount": item.get("amount"),
                        "score": item.get("screener", {}).get("snapshotScore"),
                    }
                    for item in leaders
                ],
            }
        )
    sectors.sort(
        key=lambda item: (item["strengthScore"], item["averageChange"], item["amount"]),
        reverse=True,
    )
    for index, sector in enumerate(sectors, 1):
        sector["rank"] = index
    return sectors


def fetch_market_rows(market_key: str) -> Tuple[List[Dict[str, Any]], int]:
    if market_key == "all":
        rows_by_code: Dict[str, Dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=len(ALL_MARKET_SCOPE_KEYS)) as executor:
            futures = [executor.submit(fetch_market_rows, key) for key in ALL_MARKET_SCOPE_KEYS]
            for future in as_completed(futures):
                rows, _ = future.result()
                for row in rows:
                    code = str(row.get("f12") or "")
                    if code:
                        rows_by_code[code] = row
        return list(rows_by_code.values()), len(rows_by_code)

    market = MARKET_SCOPES[market_key]
    params = {
        "pn": 1,
        "pz": MARKET_PAGE_SIZE,
        "po": 1,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": market["fs"],
        "fields": "f2,f3,f5,f6,f7,f8,f9,f10,f12,f14,f15,f16,f17,f18,f20,f21,f23,f100,f124",
    }
    first_page = request_json(EASTMONEY_LIST_URL, params)
    data = first_page.get("data") or {}
    rows = list(data.get("diff") or [])
    total = int(data.get("total") or len(rows))
    page_count = math.ceil(total / MARKET_PAGE_SIZE)
    if page_count > 1:
        def fetch_page(page_number: int) -> List[Dict[str, Any]]:
            page_params = {**params, "pn": page_number}
            page = request_json(EASTMONEY_LIST_URL, page_params)
            return list(((page.get("data") or {}).get("diff") or []))

        with ThreadPoolExecutor(max_workers=min(4, page_count - 1)) as executor:
            pages = executor.map(fetch_page, range(2, page_count + 1))
            for page_rows in pages:
                rows.extend(page_rows)
    return rows, total


def fetch_dynamic_market_data(
    market_key: str, min_amount: float, min_change: float, min_turnover: float, limit: int
) -> Dict[str, Any]:
    market = MARKET_SCOPES[market_key]
    rows, total = fetch_market_rows(market_key)
    updated_at = datetime.now(CHINA_TZ)
    quotes: List[Dict[str, Any]] = []
    market_quotes: List[Dict[str, Any]] = []
    for row in rows:
        if market_key != "all" and not str(row.get("f12") or "").startswith(market["prefixes"]):
            continue
        timestamp = number(row.get("f124"))
        if timestamp is not None:
            if timestamp > 10_000_000_000:
                timestamp /= 1000
            try:
                updated_at = datetime.fromtimestamp(timestamp, CHINA_TZ)
            except (OverflowError, OSError, ValueError):
                pass
        quote = dynamic_snapshot(row, updated_at, market_key)
        if quote is None:
            continue
        market_quotes.append(quote)
        if (
            quote["amount"] < min_amount
            or quote["changePct"] < min_change
            or quote["screener"]["turnover"] < min_turnover
        ):
            continue
        quotes.append(quote)

    quotes.sort(
        key=lambda item: (item["changePct"], item["screener"]["snapshotScore"]),
        reverse=True,
    )
    screening_pool_size = min(
        len(quotes),
        min(150, limit + DOWNTREND_CANDIDATE_BUFFER),
    )
    screened_quotes, downtrend_excluded_count = filter_steady_decline_quotes(
        quotes[:screening_pool_size]
    )
    return {
        "source": f"东方财富{market['label']}行情快照",
        "sector": {
            "key": market_key,
            "label": f"{market['label']}动态选股",
            "shortLabel": market["label"],
            "description": market["description"],
            "count": len(screened_quotes[:limit]),
        },
        "marketDate": updated_at.strftime("%Y-%m-%d"),
        "updatedAt": updated_at.isoformat(),
        "fetchedAt": datetime.now(CHINA_TZ).isoformat(),
        "scannedCount": total,
        "matchedCount": len(quotes),
        "downtrendExcludedCount": downtrend_excluded_count,
        "sectorStrength": aggregate_sector_strength(market_quotes),
        "unavailableCodes": [],
        "quotes": screened_quotes[:limit],
    }


def get_dynamic_market_data(
    market_key: str,
    min_amount: float,
    min_change: float,
    min_turnover: float,
    limit: int,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    cache_key = f"{market_key}:{min_amount:.0f}:{min_change:.2f}:{min_turnover:.2f}:{limit}"
    now = time.monotonic()
    with _cache_lock:
        cached = _dynamic_cache.get(cache_key)
        if cached and now - cached["created_at"] < DYNAMIC_CACHE_TTL_SECONDS and not force_refresh:
            return cached["payload"]

    payload = fetch_dynamic_market_data(market_key, min_amount, min_change, min_turnover, limit)
    with _cache_lock:
        _dynamic_cache[cache_key] = {"created_at": time.monotonic(), "payload": payload}
    return payload


def fetch_sector_strength_data(market_key: str) -> Dict[str, Any]:
    market = MARKET_SCOPES[market_key]
    rows, total = fetch_market_rows(market_key)
    updated_at: Optional[datetime] = None
    quotes: List[Dict[str, Any]] = []
    for row in rows:
        if market_key != "all" and not str(row.get("f12") or "").startswith(market["prefixes"]):
            continue
        row_updated_at: Optional[datetime] = None
        timestamp = number(row.get("f124"))
        if timestamp is not None:
            if timestamp > 10_000_000_000:
                timestamp /= 1000
            try:
                row_updated_at = datetime.fromtimestamp(timestamp, CHINA_TZ)
                updated_at = row_updated_at if updated_at is None else max(updated_at, row_updated_at)
            except (OverflowError, OSError, ValueError):
                pass
        quote = dynamic_snapshot(row, row_updated_at or datetime.now(CHINA_TZ), market_key)
        if quote is not None:
            quotes.append(quote)
    return {
        "source": f"东方财富{market['label']}行情快照",
        "marketDate": updated_at.strftime("%Y-%m-%d") if updated_at else None,
        "updatedAt": updated_at.isoformat() if updated_at else None,
        "fetchedAt": datetime.now(CHINA_TZ).isoformat(),
        "scannedCount": total,
        "sectorStrength": aggregate_sector_strength(quotes),
    }


def get_sector_strength_data(market_key: str, force_refresh: bool = False) -> Dict[str, Any]:
    now = time.monotonic()
    with _cache_lock:
        cached = _sector_strength_cache.get(market_key)
        if cached and now - cached["created_at"] < DYNAMIC_CACHE_TTL_SECONDS and not force_refresh:
            return cached["payload"]
    payload = fetch_sector_strength_data(market_key)
    with _cache_lock:
        _sector_strength_cache[market_key] = {
            "created_at": time.monotonic(),
            "payload": payload,
        }
    return payload


def get_stock_data(code: str, force_refresh: bool = False) -> Dict[str, Any]:
    now = time.monotonic()
    with _cache_lock:
        cached = _stock_cache.get(code)
        if cached and now - cached["created_at"] < STOCK_CACHE_TTL_SECONDS and not force_refresh:
            return cached["payload"]

    payload = fetch_stock(code)
    with _cache_lock:
        _stock_cache[code] = {"created_at": time.monotonic(), "payload": payload}
    return payload


def fetch_intraday_data(code: str, requested_date: str) -> Dict[str, Any]:
    """Return actual one-minute data for the selected daily K-line date."""
    expected_date = normalized_trade_date(requested_date)
    payload = request_json(
        EASTMONEY_MINUTE_KLINE_URL,
        {
            "secid": eastmoney_secid(code),
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "1",
            "fqt": "1",
            "beg": expected_date,
            "end": expected_date,
            "lmt": "1000",
            "ut": "fa5fd1943c7b386f172d6893dbfba10",
            "rtntype": "6",
        },
    )
    data = payload.get("data") or {}
    raw_klines = data.get("klines") or []
    points: List[Dict[str, Any]] = []
    for raw_kline in raw_klines:
        parts = str(raw_kline).split(",")
        if len(parts) < 7:
            continue
        date_time = parts[0].split()
        if len(date_time) != 2 or date_time[0] != requested_date:
            continue
        price = number(parts[2])
        volume = number(parts[5])
        amount = number(parts[6])
        if price is None or volume is None or amount is None:
            continue
        points.append(
            {
                "time": date_time[1],
                "price": price,
                "volume": max(0.0, volume),
                "amount": max(0.0, amount),
            }
        )
    if not points:
        raise MarketDataError(f"{code} 在 {requested_date} 未返回分钟行情")
    return {
        "source": "东方财富公开历史分钟行情",
        "code": code,
        "name": str(data.get("name") or code),
        "date": requested_date,
        "previousClose": number(data.get("preKPrice")),
        "points": points,
    }


def get_intraday_data(code: str, requested_date: str, force_refresh: bool = False) -> Dict[str, Any]:
    cache_key = f"{code}:{requested_date}"
    now = time.monotonic()
    with _cache_lock:
        cached = _intraday_cache.get(cache_key)
        if cached and now - cached["created_at"] < INTRADAY_CACHE_TTL_SECONDS and not force_refresh:
            return cached["payload"]

    payload = fetch_intraday_data(code, requested_date)
    with _cache_lock:
        _intraday_cache[cache_key] = {"created_at": time.monotonic(), "payload": payload}
    return payload


class AppHandler(BaseHTTPRequestHandler):
    server_version = "TradingAgentsMarket/2.0"

    def send_json(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_html(self) -> None:
        try:
            body = HTML_FILE.read_bytes()
        except OSError as exc:
            self.send_json(500, {"error": f"无法读取页面: {exc}"})
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if parsed.path in ("/", "/tradingagents-stock-selector.html"):
            self.send_html()
            return
        if parsed.path == "/api/health":
            self.send_json(200, {"status": "ok", "version": 10})
            return
        if parsed.path == "/api/sectors":
            self.send_json(
                200,
                {
                    "overview": public_sector("overview"),
                    "sectors": [public_sector(key) for key in SECTOR_ORDER],
                },
            )
            return
        if parsed.path == "/api/quotes":
            sector_key = query.get("sector", ["overview"])[0]
            if sector_key not in SECTORS:
                self.send_json(400, {"error": "未知板块"})
                return
            try:
                self.send_json(
                    200,
                    get_market_data(sector_key, query.get("refresh") == ["1"]),
                )
            except MarketDataError as exc:
                self.send_json(502, {"error": str(exc)})
            return
        if parsed.path == "/api/screener":
            market_key = query.get("market", [""])[0]
            if market_key not in MARKET_SCOPES:
                self.send_json(400, {"error": "未知市场范围"})
                return
            try:
                min_amount = max(0.0, min(float(query.get("minAmount", ["0"])[0]), 10_000_000_000))
                min_change = max(-20.0, min(float(query.get("minChange", ["0"])[0]), 20.0))
                min_turnover = max(0.0, min(float(query.get("minTurnover", ["0"])[0]), 100.0))
                limit = max(20, min(int(query.get("limit", ["80"])[0]), 150))
            except (TypeError, ValueError):
                self.send_json(400, {"error": "动态筛选参数格式不正确"})
                return
            try:
                self.send_json(
                    200,
                    get_dynamic_market_data(
                        market_key,
                        min_amount,
                        min_change,
                        min_turnover,
                        limit,
                        query.get("refresh") == ["1"],
                    ),
                )
            except MarketDataError as exc:
                self.send_json(502, {"error": str(exc)})
            return
        if parsed.path == "/api/sector-strength":
            market_key = query.get("market", ["all"])[0]
            if market_key not in MARKET_SCOPES:
                self.send_json(400, {"error": "未知市场范围"})
                return
            try:
                self.send_json(
                    200,
                    get_sector_strength_data(
                        market_key,
                        query.get("refresh") == ["1"],
                    ),
                )
            except MarketDataError as exc:
                self.send_json(502, {"error": str(exc)})
            return
        if parsed.path == "/api/stock":
            code = query.get("code", [""])[0]
            if len(code) != 6 or not code.isdigit():
                self.send_json(400, {"error": "股票代码应为 6 位数字"})
                return
            try:
                self.send_json(
                    200,
                    {"source": "腾讯证券公开行情", "quote": get_stock_data(code, query.get("refresh") == ["1"])},
                )
            except MarketDataError as exc:
                self.send_json(502, {"error": str(exc)})
            return
        if parsed.path == "/api/concepts":
            code = query.get("code", [""])[0]
            if len(code) != 6 or not code.isdigit():
                self.send_json(400, {"error": "股票代码应为 6 位数字"})
                return
            try:
                self.send_json(
                    200,
                    {
                        "source": "东方财富热门概念",
                        "concepts": get_stock_concepts(code),
                    },
                )
            except MarketDataError as exc:
                self.send_json(502, {"error": str(exc)})
            return
        if parsed.path == "/api/intraday":
            code = query.get("code", [""])[0]
            requested_date = query.get("date", [""])[0]
            if len(code) != 6 or not code.isdigit():
                self.send_json(400, {"error": "股票代码应为 6 位数字"})
                return
            try:
                self.send_json(
                    200,
                    get_intraday_data(code, requested_date, query.get("refresh") == ["1"]),
                )
            except MarketDataError as exc:
                self.send_json(502, {"error": str(exc)})
            return
        self.send_json(404, {"error": "未找到该页面"})

    def log_message(self, message_format: str, *args: Any) -> None:
        print(f"[{self.log_date_time_string()}] {message_format % args}")


def main() -> None:
    parser = argparse.ArgumentParser(description="TradingAgents A股板块选股服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8502)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), AppHandler)
    print(f"TradingAgents 板块选股工作台已启动: http://{args.host}:{args.port}/")
    print("按 Ctrl+C 停止服务")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
