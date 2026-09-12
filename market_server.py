#!/usr/bin/env python3
"""Serve a sector-based TradingAgents stock selector with live A-share quotes."""

import argparse
import collections
import difflib
import html
import json
import math
import os
import re
import shlex
import sqlite3
import statistics
import subprocess
import threading
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse


BASE_DIR = Path(__file__).resolve().parent
HTML_FILE = BASE_DIR / "trading-agents-stock.html"
QUOTE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
TENCENT_BATCH_QUOTE_URL = "https://qt.gtimg.cn/q="
EASTMONEY_LIST_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
EASTMONEY_LIST_FALLBACK_URL = "https://push2.eastmoney.com/api/qt/clist/get"
EASTMONEY_MINUTE_KLINE_URL = "https://push2delay.eastmoney.com/api/qt/stock/kline/get"
EASTMONEY_DAILY_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
EASTMONEY_HOT_CONCEPT_URL = "https://emappdata.eastmoney.com/stockrank/getHotStockRankList"
EASTMONEY_SEARCH_URL = "https://searchapi.eastmoney.com/api/suggest/get"
SOHU_HISTORY_URL = "https://q.stock.sohu.com/hisHq"
SINA_DAILY_KLINE_URL = "https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
THS_HOT_RANK_URL = "https://eq.10jqka.com.cn/earlyInterpret/index.php"
THS_BASIC_STOCK_URL = "https://basic.10jqka.com.cn/{code}/"
CHINA_TZ = timezone(timedelta(hours=8))
HISTORY_DAYS = 400
CACHE_TTL_SECONDS = 20
INTRADAY_CACHE_TTL_SECONDS = 300
DYNAMIC_CACHE_TTL_SECONDS = 30
STOCK_CACHE_TTL_SECONDS = 300
DOWNTREND_HISTORY_DAYS = 90
DOWNTREND_MIN_HISTORY_DAYS = 25
DOWNTREND_CACHE_TTL_SECONDS = 300
DOWNTREND_CANDIDATE_BUFFER = 20
DOWNTREND_MAX_WORKERS = 8
CONCEPT_CACHE_TTL_SECONDS = 600
CONCEPT_DB_REFRESH_SECONDS = 7 * 24 * 60 * 60
LIMIT_UP_CACHE_TTL_SECONDS = 25
LIMIT_UP_HISTORY_CACHE_TTL_SECONDS = 12 * 60 * 60
MIDTERM_ACTIVITY_CACHE_TTL_SECONDS = 12 * 60 * 60
# Keep roughly three years of trading sessions for the follow-up model's
# historical calibration. Limit-up counts themselves still use a one-year cutoff.
LIMIT_UP_HISTORY_DAYS = 750
LIMIT_UP_CANDIDATE_LIMIT = 36
THS_POPULARITY_CACHE_TTL_SECONDS = 10 * 60
REQUEST_TIMEOUT_SECONDS = 10
SUPPLEMENT_REQUEST_TIMEOUT_SECONDS = 5
MARKET_PAGE_SIZE = 100
AUCTION_DATA_DIR = BASE_DIR / "data" / "auction"
BACKTEST_DB_PATH = BASE_DIR / "data" / "backtest.sqlite"
THRESHOLD_TIMES_PATH = BASE_DIR / "data" / "threshold_times.json"
BACKTEST_LOOKBACK_DAYS = 30
BACKTEST_HORIZON_DAYS = 10
BACKTEST_TARGET_RETURN = 0.05
HOT_CONCEPT_ALIASES: Dict[str, Tuple[str, ...]] = {
    "液冷": ("液冷服务器", "液冷服务器概念"),
    "CPO": ("CPO概念", "光模块"),
    "PCB": ("PCB概念", "印制电路板"),
    "光纤": ("光纤概念",),
    "存储芯片": ("存储芯片概念", "存储器", "半导体存储器"),
    "半导体": ("半导体概念", "存储芯片"),
    "AI视频": ("AI视频概念", "文生视频", "Sora概念"),
    "AI语料": ("AI语料概念", "语料库", "数据要素"),
    "AI应用": ("AI应用概念", "人工智能应用"),
    "AI智能体": ("AI智能体概念", "智能体", "AI Agent"),
    "稀土": ("稀土永磁", "稀土概念"),
    "稀土永磁": ("稀土永磁概念", "稀土"),
    "黄金": ("黄金概念", "贵金属"),
    "小金属": ("小金属概念", "小金属行业"),
    "酒店": ("酒店餐饮",),
    "景点运营": ("旅游及景区", "旅游景点"),
    "免税店": ("免税概念",),
    "白酒": ("白酒概念",),
    "大消费": ("消费风格", "新消费"),
    "啤酒": ("啤酒概念",),
    "预制菜": ("预制菜概念",),
    "乳业": ("乳业概念",),
    "饮料制造": ("饮料乳品", "软饮料"),
    "零售": ("商业百货", "零售业"),
    "商业百货": ("零售业", "商业零售"),
    "创新药": ("创新药概念", "化学制药"),
    "医药商业": ("医药商业行业", "医药流通"),
    "农业种植": ("种植业", "农业种植概念"),
    "化肥": ("化肥行业",),
    "海工装备": ("海工装备概念", "海洋工程装备"),
    "兵装重组": ("兵装重组概念", "中国兵器装备集团"),
    "数字货币": ("数字货币概念",),
    "互联网金融": ("互联网金融概念", "金融科技"),
    "猪肉": ("猪肉概念", "生猪养殖"),
    "养鸡": ("鸡肉概念", "家禽养殖"),
    "渔业": ("水产养殖", "渔业概念"),
    "鸡肉概念": ("养鸡", "家禽养殖"),
    "水产养殖": ("渔业", "渔业概念"),
    "石油": ("石油行业", "油气开采"),
    "天然气": ("天然气概念",),
}
HOT_CONCEPT_FIXED_BOARDS: Dict[str, Tuple[str, str]] = {
    # Eastmoney's suggestion API currently returns index 980138 first, while
    # the constituent-bearing concept board uses BK1137.
    "存储芯片": ("BK1137", "存储芯片"),
    # 光纤概念在东方财富的成分板块代码为 BK1660，避免名称搜索误命中指数或空结果。
    "光纤": ("BK1660", "光纤概念"),
    # 页面展示名为“AI芯片”，内部兼容“算力芯片”请求。
    "算力芯片": ("BK1127", "AI芯片"),
}
# This command is deliberately supplied through the local environment, never code.
# It must print an EMT get_open_call_auction JSON result for the whole A-share universe.
# Example: EMT_AUCTION_COMMAND='python3 /path/to/export_emt_auction.py --date {date}'
EMT_AUCTION_COMMAND = os.environ.get("EMT_AUCTION_COMMAND", "").strip()
AUCTION_CAPTURE_WINDOW_START = (9, 25, 0)
AUCTION_CAPTURE_WINDOW_END = (9, 30, 0)

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
_stock_name_cache: Dict[str, str] = {}
_downtrend_history_cache: Dict[str, Dict[str, Any]] = {}
_concept_cache: Dict[str, Dict[str, Any]] = {}
_hot_concept_cache: Dict[str, Dict[str, Any]] = {}
_hot_concept_summary_cache: Dict[str, Dict[str, Any]] = {}
_concept_scoring_context_cache: Dict[str, Any] = {}
_concept_scoring_refresh_state: Dict[str, bool] = {"running": False}
_limit_up_cache: Dict[str, Any] = {}
_sector_money_flow_cache: Dict[str, Any] = {}
_limit_up_history_cache: Dict[str, Dict[str, Any]] = {}
_midterm_activity_cache: Dict[str, Dict[str, Any]] = {}
_ths_popularity_cache: Dict[str, Dict[str, Any]] = {}
_auction_capture_attempts: Dict[str, float] = {}
_backtest_lock = threading.Lock()
_midterm_lock = threading.Lock()
_all_market_backtest_status: Dict[str, Any] = {
    "status": "idle", "total": 0, "processed": 0, "records": 0, "failed": 0,
}
_midterm_scan_status: Dict[str, Any] = {
    "status": "idle", "total": 0, "processed": 0, "added": 0, "failed": 0,
}
_midterm_quote_cache: Dict[str, Any] = {"created_at": 0.0, "rows": {}}
_score_threshold_times: Dict[Tuple[str, str, int], str] = {}
_score_threshold_names: Dict[Tuple[str, str, int], str] = {}
_score_threshold_quotes: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
_prediction_threshold_times: Dict[Tuple[str, str, int], str] = {}
_prediction_threshold_names: Dict[Tuple[str, str, int], str] = {}
_prediction_threshold_quotes: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
_change_threshold_times: Dict[Tuple[str, str, int], str] = {}
_nonmain_repeat_alert_times: Dict[Tuple[str, str], str] = {}
_nonmain_repeat_alert_names: Dict[Tuple[str, str], str] = {}
_nonmain_repeat_alert_quotes: Dict[Tuple[str, str], Dict[str, Any]] = {}
_nonmain_confirmation_history: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
_nonmain_candidate_entries: Dict[Tuple[str, str], Dict[str, Any]] = {}
_score_observations: Dict[Tuple[str, str], Tuple[float, float]] = {}


def is_intraday_alert_session(value: datetime) -> bool:
    """Only trading-session snapshots may create live alerts."""
    local = value.astimezone(CHINA_TZ)
    if local.weekday() >= 5:
        return False
    minutes = local.hour * 60 + local.minute
    return 9 * 60 + 25 <= minutes < 11 * 60 + 30 or 13 * 60 <= minutes < 15 * 60


def is_main_board_code(code: str) -> bool:
    code = str(code)
    return code.startswith(("000", "001", "002", "003", "600", "601", "603", "605"))


def load_threshold_times() -> None:
    """Restore first-hit timestamps so service restarts never overwrite them."""
    try:
        payload = json.loads(THRESHOLD_TIMES_PATH.read_text(encoding="utf-8"))
        for item in payload.get("score", []):
            key = (str(item["date"]), str(item["code"]), int(item["threshold"]))
            _score_threshold_times[key] = str(item["time"])
            _score_threshold_names[key] = str(item.get("name") or item["code"])
            _score_threshold_quotes[key] = {
                "price": item.get("price"), "changePct": item.get("changePct"),
                "strategyScore": item.get("strategyScore"), "industry": item.get("industry"),
            }
        for item in payload.get("prediction", []):
            key = (str(item["date"]), str(item["code"]), int(item["threshold"]))
            _prediction_threshold_times[key] = str(item["time"])
            _prediction_threshold_names[key] = str(item.get("name") or item["code"])
            _prediction_threshold_quotes[key] = {"price": item.get("price"), "changePct": item.get("changePct")}
        for item in payload.get("change", []):
            key = (str(item["date"]), str(item["code"]), int(item["threshold"]))
            _change_threshold_times[key] = str(item["time"])
        for item in payload.get("nonMainRepeat", []):
            key = (str(item["date"]), str(item["code"]))
            _nonmain_repeat_alert_times[key] = str(item["time"])
            _nonmain_repeat_alert_names[key] = str(item.get("name") or item["code"])
            _nonmain_repeat_alert_quotes[key] = {
                "price": item.get("price"), "changePct": item.get("changePct"),
                "strategyScore": item.get("strategyScore"), "industry": item.get("industry"),
            }
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
        pass


def persist_threshold_times() -> None:
    payload = {
        "score": [
            {"date": date, "code": code, "threshold": threshold, "time": value, "name": _score_threshold_names.get((date, code, threshold), code), **(_score_threshold_quotes.get((date, code, threshold)) or {})}
            for (date, code, threshold), value in _score_threshold_times.items()
        ],
        "prediction": [
            {"date": date, "code": code, "threshold": threshold, "time": value, "name": _prediction_threshold_names.get((date, code, threshold), code), **(_prediction_threshold_quotes.get((date, code, threshold)) or {})}
            for (date, code, threshold), value in _prediction_threshold_times.items()
        ],
        "change": [
            {"date": date, "code": code, "threshold": threshold, "time": value}
            for (date, code, threshold), value in _change_threshold_times.items()
        ],
        "nonMainRepeat": [
            {"date": date, "code": code, "time": value, "name": _nonmain_repeat_alert_names.get((date, code), code), **(_nonmain_repeat_alert_quotes.get((date, code)) or {})}
            for (date, code), value in _nonmain_repeat_alert_times.items()
        ],
    }
    THRESHOLD_TIMES_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = THRESHOLD_TIMES_PATH.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    temporary.replace(THRESHOLD_TIMES_PATH)


load_threshold_times()


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


def main_board_code(code: str) -> bool:
    return code.startswith(MARKET_SCOPES["sh-main"]["prefixes"] + MARKET_SCOPES["sz-main"]["prefixes"])


def bse_code(code: str) -> bool:
    return code.startswith(MARKET_SCOPES["bse"]["prefixes"])


def midterm_required_ma_count(code: str) -> int:
    """All markets require at least three rising moving averages."""
    return 3


def midterm_simplified_market(code: str) -> bool:
    """创业板、科创板使用7月新低+8月反弹的简化规则。"""
    return str(code or "").startswith(("300", "301", "688", "689"))


def normal_limit_price(previous_close: float) -> float:
    return float((Decimal(str(previous_close)) * Decimal("1.10")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


INDUSTRY_BLACKLIST = frozenset({"环境治理", "环保设备", "轨交设备", "银行", "个护用品", "种植业", "油服工程"})


def blacklisted_industry(row: Dict[str, Any]) -> bool:
    """行业级硬排除。

    东方财富部分行业名称会带“Ⅰ/Ⅱ”层级后缀，因此取前缀匹配，保证同一
    行业的子类别也无法绕过黑名单。
    """
    industry = str(row.get("f100") or "").strip()
    return any(industry == item or industry.startswith(item) for item in INDUSTRY_BLACKLIST)


def high_open_fade(row: Dict[str, Any], limit_pct: float = 7.0) -> bool:
    """排除高开后走弱的股票。

    开盘涨幅以 f17 相对前收 f18 计算；当开盘涨幅达 7% 及以上，且
    盘中价 f2 已跌回开盘价之下，当日不再参与候选、评分或告警。
    这使高开后仍继续走强的股票不会被误杀。
    """
    opening = number(row.get("f17"))
    previous_close = number(row.get("f18"))
    current_price = number(row.get("f2"))
    if opening in (None, 0) or previous_close in (None, 0) or current_price is None:
        return False
    return (opening / previous_close - 1) * 100 >= limit_pct and current_price < opening


def intraday_limit_up_fade(row: Dict[str, Any]) -> bool:
    """排除盘中曾涨停、后续已跳水打开的股票。

    f15 是当日最高价，若已触及按市场类型计算的涨停价，但 f2 已低于涨停价，
    则该股当日永久不再进入候选池或产生告警。
    """
    code = str(row.get("f12") or "")
    name = str(row.get("f14") or "")
    previous_close = number(row.get("f18"))
    high = number(row.get("f15"))
    current_price = number(row.get("f2"))
    if len(code) != 6 or previous_close in (None, 0) or high is None or current_price is None:
        return False
    limit_price = float(
        (Decimal(str(previous_close)) * Decimal(str(1 + market_limit_ratio(code, name))))
        .quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    )
    return high >= limit_price - 0.001 and current_price < limit_price - 0.001


def auction_data_path(trade_date: str) -> Path:
    return AUCTION_DATA_DIR / f"{trade_date}.json"


def auction_code(record: Dict[str, Any]) -> str:
    raw = str(record.get("code") or record.get("symbol") or record.get("sec_id") or "")
    return raw.split(".")[-1].strip()


def number_or_none(value: Any) -> Optional[float]:
    return number(value)


def normalize_auction_records(payload: Any, trade_date: str) -> Dict[str, Dict[str, Any]]:
    """Normalize EMT's open-call-auction rows without retaining any account credentials."""
    records = payload.get("data", payload) if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        raise MarketDataError("竞价导出结果应为数组，或包含 data 数组的 JSON 对象")
    normalized: Dict[str, Dict[str, Any]] = {}
    for row in records:
        if not isinstance(row, dict):
            continue
        code = auction_code(row)
        if len(code) != 6 or not code.isdigit():
            continue
        bid_amount = 0.0
        for level in range(1, 6):
            price = number_or_none(row.get(f"bid_p{level}"))
            volume = number_or_none(row.get(f"bid_v{level}"))
            if price is not None and volume is not None and price > 0 and volume > 0:
                bid_amount += price * volume
        normalized[code] = {
            "code": code,
            "auctionAmount": number_or_none(row.get("open_amount") or row.get("auctionAmount")),
            "auctionVolume": number_or_none(row.get("open_volume") or row.get("auctionVolume")),
            "auctionPrice": number_or_none(row.get("current_price") or row.get("auctionPrice")),
            "unmatchedBuyAmount": round(bid_amount, 2) if bid_amount else None,
            "capturedAt": str(row.get("time") or f"{trade_date} 09:25:00"),
        }
    if not normalized:
        raise MarketDataError("竞价导出中没有可识别的股票记录")
    return normalized


def load_auction_snapshot(trade_date: str) -> Dict[str, Any]:
    path = auction_data_path(trade_date)
    if not path.exists():
        return {"available": False, "status": "missing", "records": {}, "message": "尚未保存 09:25 竞价快照"}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        records = payload.get("records") if isinstance(payload, dict) else None
        if not isinstance(records, dict):
            raise ValueError("records 缺失")
        return {
            "available": True,
            "status": "ready",
            "records": records,
            "capturedAt": payload.get("capturedAt"),
            "message": "已保存 09:25 竞价快照",
        }
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {"available": False, "status": "error", "records": {}, "message": f"竞价快照读取失败: {exc}"}


def capture_emt_auction_snapshot(trade_date: str) -> Dict[str, Any]:
    """Run an opt-in local EMT exporter and persist only derived market fields."""
    if not EMT_AUCTION_COMMAND:
        return {"available": False, "status": "unconfigured", "records": {}, "message": "未配置 EMT_AUCTION_COMMAND，未接入竞价数据"}
    command = shlex.split(EMT_AUCTION_COMMAND.replace("{date}", trade_date))
    if not command:
        return {"available": False, "status": "unconfigured", "records": {}, "message": "EMT_AUCTION_COMMAND 为空"}
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=45)
        records = normalize_auction_records(json.loads(result.stdout), trade_date)
        AUCTION_DATA_DIR.mkdir(parents=True, exist_ok=True)
        snapshot = {
            "marketDate": trade_date,
            "capturedAt": datetime.now(CHINA_TZ).isoformat(),
            "provider": "东方财富 EMT get_open_call_auction",
            "records": records,
        }
        path = auction_data_path(trade_date)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
        temporary.replace(path)
        return {"available": True, "status": "ready", "records": records, "capturedAt": snapshot["capturedAt"], "message": "已采集并保存 09:25 竞价快照"}
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError, MarketDataError) as exc:
        return {"available": False, "status": "error", "records": {}, "message": f"EMT 竞价采集失败: {exc}"}


def get_auction_snapshot(trade_date: str, capture_if_due: bool = False) -> Dict[str, Any]:
    snapshot = load_auction_snapshot(trade_date)
    if snapshot["available"] or not capture_if_due:
        return snapshot
    if not EMT_AUCTION_COMMAND:
        return {"available": False, "status": "unconfigured", "records": {}, "message": "未配置 EMT_AUCTION_COMMAND，未接入竞价数据"}
    now = datetime.now(CHINA_TZ)
    current_time = (now.hour, now.minute, now.second)
    if now.strftime("%Y-%m-%d") != trade_date or now.weekday() >= 5 or not (AUCTION_CAPTURE_WINDOW_START <= current_time < AUCTION_CAPTURE_WINDOW_END):
        return snapshot
    last_attempt = _auction_capture_attempts.get(trade_date, 0)
    if time.monotonic() - last_attempt < 25:
        return snapshot
    _auction_capture_attempts[trade_date] = time.monotonic()
    return capture_emt_auction_snapshot(trade_date)


def auction_capture_loop(stop_event: threading.Event) -> None:
    """Capture once after 09:25 when the local service remains running during the auction."""
    while not stop_event.wait(1):
        now = datetime.now(CHINA_TZ)
        if now.weekday() >= 5:
            continue
        current_time = (now.hour, now.minute, now.second)
        if not (AUCTION_CAPTURE_WINDOW_START <= current_time < AUCTION_CAPTURE_WINDOW_END):
            continue
        trade_date = now.strftime("%Y-%m-%d")
        snapshot = load_auction_snapshot(trade_date)
        if not snapshot["available"]:
            get_auction_snapshot(trade_date, capture_if_due=True)


def follow_up_snapshot_loop(stop_event: threading.Event) -> None:
    """交易时段后台扫描全市场，收盘后保存一次候选快照。"""
    captured_dates: set[str] = set()
    last_intraday_scan = 0.0
    while not stop_event.wait(30):
        now = datetime.now(CHINA_TZ)
        if now.weekday() >= 5:
            continue
        trade_date = now.strftime("%Y-%m-%d")
        current_minutes = now.hour * 60 + now.minute
        in_session = 555 <= current_minutes < 690 or 780 <= current_minutes < 900
        try:
            if in_session and time.monotonic() - last_intraday_scan >= 25:
                # 轻量扫描全市场快照；dynamic_snapshot 会记录首次达到阈值的时间，
                # 不触发历史 K 线、概念和人气等页面级补充请求。
                scan_all_market_thresholds()
                last_intraday_scan = time.monotonic()
                print(f"后台全市场评分扫描完成：{trade_date} {now.strftime('%H:%M:%S')}")
            elif now.hour == 15 and 5 <= now.minute < 30 and trade_date not in captured_dates:
                scan_all_market_thresholds()
                captured_dates.add(trade_date)
                print(f"后续上涨模型已保存 {trade_date} 收盘候选快照")
        except (MarketDataError, OSError, ValueError) as exc:
            print(f"后台全市场评分扫描失败: {exc}")


def scan_all_market_thresholds() -> None:
    """Scan every live A-share row only for score/change threshold timestamps."""
    rows, _ = fetch_market_rows("all")
    updated_at = datetime.now(CHINA_TZ)
    snapshots: List[Dict[str, Any]] = []
    for row in rows:
        quote = dynamic_snapshot(row, updated_at, "all")
        if quote:
            snapshots.append(quote)
    save_score_traces(snapshots, updated_at)
    process_nonmain_confirmation_alerts(snapshots, updated_at)


def save_score_traces(quotes: List[Dict[str, Any]], updated_at: datetime) -> None:
    """Persist at most one score snapshot per stock per minute."""
    if not quotes:
        return
    trade_date = updated_at.astimezone(CHINA_TZ).strftime("%Y-%m-%d")
    minute = updated_at.astimezone(CHINA_TZ).strftime("%H:%M")
    connection = open_backtest_db()
    try:
        connection.executemany(
            """INSERT OR IGNORE INTO score_traces
            (trade_date, minute, code, name, score, predictive_score, predictive_probability,
             price, change_pct, volume_ratio, net_inflow, macd_signal, base_json,
             order_flow_score, macd_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    trade_date, minute, quote["code"], quote.get("name"),
                    quote.get("screener", {}).get("snapshotScore"),
                    quote.get("screener", {}).get("predictiveScore"),
                    quote.get("screener", {}).get("predictiveProbability"),
                    quote.get("price"), quote.get("changePct"), quote.get("volumeRatio"),
                    quote.get("netInflow"), None,
                    json.dumps({
                        **(quote.get("base") or {}),
                        "_strategyScores": (quote.get("screener") or {}).get("strategyScores") or {},
                        "_strategyPredictiveScores": (quote.get("screener") or {}).get("strategyPredictiveScores") or {},
                        "_strategyPredictiveProbabilities": (quote.get("screener") or {}).get("strategyPredictiveProbabilities") or {},
                        "_amount": quote.get("amount"),
                        "_amountGatePassed": (quote.get("screener") or {}).get("amountGatePassed"),
                    }, ensure_ascii=False),
                    (quote.get("probability") or {}).get("orderFlowScore"),
                    (quote.get("probability") or {}).get("macdScore"),
                )
                for quote in quotes
            ],
        )
        connection.commit()
    finally:
        connection.close()


def _clock_seconds(value: str) -> Optional[int]:
    try:
        hour, minute, second = (int(part) for part in str(value).split(":"))
        return hour * 3600 + minute * 60 + second
    except (TypeError, ValueError):
        return None


def process_nonmain_confirmation_alerts(quotes: List[Dict[str, Any]], updated_at: datetime) -> None:
    """Maintain ten-minute repeat alerts for qualified main and non-main stocks."""
    if not is_intraday_alert_session(updated_at):
        return
    trade_date = updated_at.astimezone(CHINA_TZ).strftime("%Y-%m-%d")
    score_time = updated_at.astimezone(CHINA_TZ).strftime("%H:%M:%S")
    minute = score_time[:5]
    with _cache_lock:
        changed = False
        for quote in quotes:
            code = str(quote.get("code") or "")
            if not code:
                continue
            screener = quote.get("screener") or {}
            strategy_scores = screener.get("strategyScores") or {}
            if len(strategy_scores) != 4:
                continue
            key = (trade_date, code)
            if is_main_board_code(code):
                if not screener.get("mainBoardElite"):
                    changed = changed or key in _nonmain_repeat_alert_times
                    _nonmain_repeat_alert_times.pop(key, None)
                    _nonmain_repeat_alert_names.pop(key, None)
                    _nonmain_repeat_alert_quotes.pop(key, None)
                    continue
                first_time = _score_threshold_times.get((trade_date, code, 70))
                previous_time = _nonmain_repeat_alert_times.get(key) or first_time
                previous_seconds, current_seconds = _clock_seconds(previous_time), _clock_seconds(score_time)
                if previous_seconds is None or current_seconds is None or current_seconds - previous_seconds < 600:
                    continue
                highest_strategy_score = max(float(value) for value in strategy_scores.values())
                _nonmain_repeat_alert_times[key] = score_time
                _nonmain_repeat_alert_names[key] = str(quote.get("name") or code)
                _nonmain_repeat_alert_quotes[key] = {
                    "price": quote.get("price"), "changePct": quote.get("changePct"),
                    "strategyScore": highest_strategy_score, "industry": quote.get("actualIndustry"),
                }
                changed = True
                save_alert_event(
                    trade_date, code, str(quote.get("name") or code), f"score-repeat-{score_time}", 70, score_time,
                    {"price": quote.get("price"), "changePct": quote.get("changePct"),
                     "volumeRatio": quote.get("volumeRatio"), "turnover": screener.get("turnover"),
                     "amplitude": screener.get("amplitude"), "netInflow": quote.get("netInflow")},
                    highest_strategy_score, float(screener.get("predictiveScore") or 0),
                    float(screener.get("predictiveProbability") or 0), quote.get("base") or {}, None,
                )
                continue
            snapshot = {
                "minute": minute,
                "score": float(screener.get("snapshotScore") or 0),
                "strategyScores": {name: float(value) for name, value in strategy_scores.items()},
                "price": float(quote.get("price") or 0),
                "amount": float(quote.get("amount") or 0),
                "turnover": float(screener.get("turnover") or 0),
                "netInflow": quote.get("netInflow"),
                "volumeRatio": quote.get("volumeRatio"),
            }
            history = _nonmain_confirmation_history.setdefault(key, [])
            if history and history[-1]["minute"] == minute:
                history[-1] = snapshot
            else:
                history.append(snapshot)
            del history[:-10]

            strategy_confirmed = sum(value >= 70 for value in snapshot["strategyScores"].values()) >= 2
            if not strategy_confirmed:
                changed = changed or key in _nonmain_repeat_alert_times
                _nonmain_candidate_entries.pop(key, None)
                _nonmain_repeat_alert_times.pop(key, None)
                _nonmain_repeat_alert_names.pop(key, None)
                _nonmain_repeat_alert_quotes.pop(key, None)
                continue

            entry = _nonmain_candidate_entries.setdefault(key, {
                "minute": minute,
                "price": snapshot["price"],
                "amount": snapshot["amount"],
                "turnover": snapshot["turnover"],
                "firstFivePrices": [],
            })
            if not entry["firstFivePrices"] or entry["firstFivePrices"][-1][0] != minute:
                if len(entry["firstFivePrices"]) < 5:
                    entry["firstFivePrices"].append((minute, snapshot["price"]))

            if len(history) < 10:
                continue
            confirmed_count = sum(sum(value >= 70 for value in item["strategyScores"].values()) >= 2 for item in history)
            high_score_count = sum(max(item["strategyScores"].values()) >= 75 for item in history)
            scores = [item["score"] for item in history]
            flow_start, flow_now = history[0]["netInflow"], snapshot["netInflow"]
            flow_stable = not (flow_start is not None and float(flow_start) > 0 and flow_now is not None and float(flow_now) < 0)
            ratios = [float(item["volumeRatio"]) for item in history[-4:] if item["volumeRatio"] is not None]
            volume_stable = not (len(ratios) == 4 and all(ratios[index] - ratios[index + 1] >= 0.05 for index in range(3)))
            entry_floor = min((price for _, price in entry["firstFivePrices"]), default=entry["price"])
            amount_turnover_stable = (
                all(item["amount"] >= 100_000_000 for item in history[-3:])
                and snapshot["amount"] > entry["amount"]
                and snapshot["turnover"] > entry["turnover"]
            )
            confirmation_passed = (
                confirmed_count >= 8
                and high_score_count >= 3
                and snapshot["score"] >= max(scores) - 5
                and flow_stable
                and volume_stable
                and snapshot["price"] > entry["price"]
                and snapshot["price"] >= entry_floor
                and amount_turnover_stable
            )
            if not confirmation_passed:
                changed = changed or key in _nonmain_repeat_alert_times
                _nonmain_repeat_alert_times.pop(key, None)
                _nonmain_repeat_alert_names.pop(key, None)
                _nonmain_repeat_alert_quotes.pop(key, None)
                continue

            previous_time = _nonmain_repeat_alert_times.get(key)
            previous_seconds, current_seconds = _clock_seconds(previous_time), _clock_seconds(score_time)
            if previous_seconds is not None and current_seconds is not None and current_seconds - previous_seconds < 600:
                continue
            highest_strategy_score = max(snapshot["strategyScores"].values())
            _nonmain_repeat_alert_times[key] = score_time
            _nonmain_repeat_alert_names[key] = str(quote.get("name") or code)
            _nonmain_repeat_alert_quotes[key] = {
                "price": snapshot["price"], "changePct": quote.get("changePct"),
                "strategyScore": highest_strategy_score, "industry": quote.get("actualIndustry"),
            }
            changed = True
            save_alert_event(
                trade_date, code, str(quote.get("name") or code), f"score-repeat-{score_time}", 70, score_time,
                {"price": snapshot["price"], "changePct": quote.get("changePct"),
                 "volumeRatio": snapshot["volumeRatio"], "turnover": snapshot["turnover"],
                 "amplitude": screener.get("amplitude"), "netInflow": flow_now},
                highest_strategy_score, float(screener.get("predictiveScore") or 0),
                float(screener.get("predictiveProbability") or 0), quote.get("base") or {}, None,
            )
        if changed:
            persist_threshold_times()


def save_alert_event(
    trade_date: str, code: str, name: str, alert_kind: str, threshold: int,
    alert_time: str, quote: Dict[str, Any], score: float, predictive_score: float,
    predictive_probability: float, factors: Dict[str, Any], macd_score: Optional[float] = None,
) -> None:
    """Persist a one-time alert snapshot for later after-close evaluation."""
    connection = open_backtest_db()
    try:
        connection.execute(
            """INSERT OR IGNORE INTO alert_events
            (trade_date, code, name, alert_kind, threshold, alert_time, price, change_pct,
             score, predictive_score, predictive_probability, volume_ratio, turnover,
             amplitude, net_inflow, factors_json, macd_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                trade_date, code, name or code, alert_kind, threshold, alert_time,
                quote.get("price"), quote.get("changePct"), score, predictive_score,
                predictive_probability, quote.get("volumeRatio"), quote.get("turnover"),
                quote.get("amplitude"), quote.get("netInflow"),
                json.dumps(factors or {}, ensure_ascii=False), macd_score,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def alert_consecutive_days(code: str, alert_kind: str, trade_date: str) -> int:
    """Count consecutive weekdays with the same alert kind up to trade_date."""
    with open_backtest_db() as connection:
        rows = connection.execute(
            "SELECT DISTINCT trade_date FROM alert_events WHERE code=? AND alert_kind=? AND trade_date<=? ORDER BY trade_date DESC",
            (code, alert_kind, trade_date),
        ).fetchall()
    dates = {str(row[0]) for row in rows}
    if trade_date not in dates:
        return 0
    current = datetime.strptime(trade_date, "%Y-%m-%d").date()
    count = 0
    while current.strftime("%Y-%m-%d") in dates:
        count += 1
        current -= timedelta(days=1)
        while current.weekday() >= 5:
            current -= timedelta(days=1)
    return count


def get_today_alert_pool(market_key: str = "all", requested_codes: Optional[set] = None) -> Dict[str, Any]:
    """Return one row per stock, using the previous alert day before 09:15."""
    local_now = datetime.now(CHINA_TZ)
    today = local_now.strftime("%Y-%m-%d")
    with open_backtest_db() as connection:
        trade_date = today
        if local_now.weekday() >= 5 or local_now.hour * 60 + local_now.minute < 9 * 60 + 15:
            previous = connection.execute(
                """SELECT MAX(trade_date) FROM alert_events
                   WHERE trade_date < ? AND (alert_kind='score' OR alert_kind LIKE 'score-repeat-%')""",
                (today,),
            ).fetchone()
            if previous and previous[0]:
                trade_date = str(previous[0])
        rows = connection.execute(
            """SELECT code, name, alert_time, score, price, change_pct
               FROM alert_events
               WHERE trade_date=? AND (alert_kind='score' OR alert_kind LIKE 'score-repeat-%')
               ORDER BY alert_time, id""",
            (trade_date,),
        ).fetchall()
    first_by_code: Dict[str, Any] = {}
    for row in rows:
        code = str(row[0])
        if requested_codes is not None and code not in requested_codes:
            continue
        if requested_codes is None and market_key != "all" and not code.startswith(MARKET_SCOPES[market_key]["prefixes"]):
            continue
        first_by_code.setdefault(code, {
            "code": code, "name": row[1] or code, "firstAlertTime": row[2],
            "entryScore": row[3], "entryPrice": row[4], "entryChangePct": row[5],
        })
    if not first_by_code:
        return {"tradeDate": trade_date, "isPreviousTradingDay": trade_date != today, "updatedAt": local_now.isoformat(), "stocks": []}
    try:
        market_rows, _ = fetch_market_rows("all")
        live_by_code = {str(row.get("f12") or ""): row for row in market_rows}
    except (MarketDataError, OSError, ValueError):
        live_by_code = {}
    stocks = []
    for code, item in first_by_code.items():
        live = live_by_code.get(code) or {}
        # 服务重启前写入的当日告警也不应再出现在告警池。
        # 以实时开盘价、前收价和现价复核后统一过滤。
        if live and (high_open_fade(live) or intraday_limit_up_fade(live) or blacklisted_industry(live)):
            continue
        item.update({
            "industry": str(live.get("f100") or "--"),
            "currentPrice": number(live.get("f2")),
            "currentChangePct": number(live.get("f3")),
        })
        stocks.append(item)
    concept_by_code: Dict[str, str] = {}
    if stocks:
        with ThreadPoolExecutor(max_workers=min(12, len(stocks))) as executor:
            futures = {executor.submit(get_stock_concepts, item["code"]): item for item in stocks}
            for future in as_completed(futures):
                item = futures[future]
                try:
                    concepts = future.result()
                    names = [str(concept.get("name") or "") for concept in concepts]
                except (MarketDataError, OSError, ValueError):
                    names = []
                concept_by_code[item["code"]] = select_primary_business_concept(item["code"], item.get("industry"), names)
    for item in stocks:
        concept = concept_by_code.get(item["code"], "")
        if concept in ("共封装光学(CPO)", "共封装光学（CPO）"):
            concept = "CPO"
        industry = str(item.get("industry") or "--")
        item["topicLabel"] = f"{industry} / {concept}" if concept else industry
        item["industry"] = item["topicLabel"]
    stocks.sort(key=lambda item: (item["firstAlertTime"] or "", item["code"]))
    return {"tradeDate": trade_date, "isPreviousTradingDay": trade_date != today, "updatedAt": local_now.isoformat(), "stocks": stocks}


def auction_strength_score(auction: Optional[Dict[str, Any]], float_cap: Optional[float]) -> Dict[str, Any]:
    """Score the 09:25 unmatched five-level buy amount; unavailable data is neutral, never zero."""
    if not auction or auction.get("unmatchedBuyAmount") in (None, 0):
        return {"available": False, "score": 50, "absolute": None, "relative": None, "vsAuction": None}
    buy_amount = float(auction["unmatchedBuyAmount"])
    auction_amount = float(auction.get("auctionAmount") or 0)
    absolute = clamp(20 + math.log10(max(buy_amount, 1) / 1_000_000) * 30)
    relative = clamp((buy_amount / max(float(float_cap or 1), 1)) * 500_000)
    vs_auction = clamp((buy_amount / max(auction_amount, 1)) * 100)
    return {
        "available": True,
        "score": round(absolute * 0.5 + relative * 0.3 + vs_auction * 0.2),
        "absolute": round(absolute),
        "relative": round(relative),
        "vsAuction": round(vs_auction),
    }


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


def resolve_stock_name(code: str) -> str:
    """Resolve a missing quote name without downloading the full market list."""
    with _cache_lock:
        cached = _stock_name_cache.get(code)
    if cached:
        return cached
    try:
        payload = request_json(EASTMONEY_SEARCH_URL, {
            "input": code, "type": 14,
            "token": "D43BF722C8E33BDC906FB84D85E326E8",
        })
        candidates = ((payload.get("QuotationCodeTable") or {}).get("Data") or [])
        match = next((item for item in candidates if str(item.get("Code") or "") == code), None)
        name = str((match or {}).get("Name") or "").strip()
        if name:
            with _cache_lock:
                _stock_name_cache[code] = name
            return name
    except (MarketDataError, OSError, ValueError):
        pass
    return code


def fetch_stock(code: str) -> Dict[str, Any]:
    symbol = stock_symbol(code)
    try:
        payload = request_json(QUOTE_URL, {"param": f"{symbol},day,,,{HISTORY_DAYS},qfq"})
        data = (payload.get("data") or {}).get(symbol) or {}
        quote = ((data.get("qt") or {}).get(symbol) or [])
        if payload.get("code") != 0 or len(quote) < 38:
            raise MarketDataError(f"{code} 行情数据不完整")
    except MarketDataError as primary_error:
        # 腾讯复权接口偶发返回 502/空数据时，使用新浪日线保证个股详情和 K 线仍可打开。
        fallback = request_json(
            SINA_DAILY_KLINE_URL,
            {"symbol": symbol, "scale": 240, "ma": "no", "datalen": HISTORY_DAYS},
        )
        rows = []
        for item in fallback if isinstance(fallback, list) else []:
            if not isinstance(item, dict):
                continue
            close = number(item.get("close"))
            if close is None:
                continue
            rows.append({
                "date": str(item.get("day") or ""),
                "open": number(item.get("open")),
                "close": close,
                "high": number(item.get("high")),
                "low": number(item.get("low")),
                "volume": number(item.get("volume")) or 0.0,
                "changePct": None,
                "changeAmount": None,
            })
        if not rows:
            raise MarketDataError(f"{code} 行情接口均不可用：{primary_error}") from primary_error
        previous = None
        for row in rows:
            row["changePct"] = return_pct(previous, row["close"]) if previous else None
            row["changeAmount"] = row["close"] - previous if previous is not None else None
            previous = row["close"]
        add_moving_averages(rows)
        latest = rows[-1]
        return enrich_stock({
            "code": code,
            "name": resolve_stock_name(code),
            "price": latest["close"],
            "changePct": latest.get("changePct"),
            "changeAmount": latest.get("changeAmount"),
            "volume": latest.get("volume") or 0.0,
            "amount": (latest.get("volume") or 0.0) * latest["close"],
            "high": latest.get("high"),
            "low": latest.get("low"),
            "open": latest.get("open"),
            "previousClose": rows[-2]["close"] if len(rows) > 1 else None,
            "updatedAt": None,
            "history": rows,
        })

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

    raw_name = str(quote[1] or "").strip()
    resolved_name = raw_name if raw_name and raw_name != code else resolve_stock_name(code)
    return enrich_stock(
        {
            "code": code,
            "name": resolved_name,
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
    change_pct: float, amount: float, turnover: float, amplitude: float, net_inflow: Optional[float] = None
) -> Dict[str, int]:
    """Score a full-market snapshot without inventing unavailable daily history."""
    liquidity_base = clamp(35 + math.log10(max(amount, 1) / 100_000_000) * 28)
    flow_ratio = (net_inflow / amount * 100) if net_inflow is not None and amount > 0 else 0
    capital_flow = clamp(50 + flow_ratio * 8)
    liquidity = clamp(liquidity_base * .7 + capital_flow * .3)
    strength = clamp(50 + change_pct * 4.2)
    momentum = clamp(50 + change_pct * 3.1 + min(turnover, 15) * 1.1)
    trend = clamp(50 + change_pct * 2.3 + min(turnover, 12) * 0.8)
    stability = clamp(92 - amplitude * 1.9 - abs(change_pct) * 0.8)
    risk = clamp(100 - stability + max(0, abs(change_pct) - 5) * 2.4)
    return {
        "trend": trend,
        "momentum": momentum,
        "liquidity": liquidity,
        "capitalFlow": capital_flow,
        "strength": strength,
        "stability": stability,
        "risk": risk,
    }


def fetch_ths_popularity(code: str, trade_date: str) -> Dict[str, Any]:
    payload = request_json(
        THS_HOT_RANK_URL,
        {"con": "index", "act": "getIndexData", "stockCode": code, "date": trade_date.replace("-", "")},
    )
    stock_info = ((payload.get("data") or {}).get("stockInfo") or {}) if isinstance(payload, dict) else {}
    rank = number(stock_info.get("hotRank"))
    total = number(stock_info.get("stockCnt"))
    if rank is None or rank <= 0:
        raise MarketDataError(f"{code} 同花顺人气排名不可用")
    return {"available": True, "rank": int(rank), "total": int(total) if total else None, "source": "同花顺人气"}


def get_ths_popularity(code: str, trade_date: str) -> Dict[str, Any]:
    cache_key = f"{trade_date}:{code}"
    now = time.monotonic()
    with _cache_lock:
        cached = _ths_popularity_cache.get(cache_key)
        if cached and now - cached["created_at"] < THS_POPULARITY_CACHE_TTL_SECONDS:
            return cached["payload"]
    payload = fetch_ths_popularity(code, trade_date)
    with _cache_lock:
        _ths_popularity_cache[cache_key] = {"created_at": time.monotonic(), "payload": payload}
    return payload


def add_ths_popularity(quotes: List[Dict[str, Any]], trade_date: str) -> None:
    if not quotes:
        return
    results: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=min(12, len(quotes))) as executor:
        futures = {executor.submit(get_ths_popularity, quote["code"], trade_date): quote["code"] for quote in quotes}
        for future in as_completed(futures):
            code = futures[future]
            try:
                results[code] = future.result()
            except MarketDataError:
                results[code] = {"available": False, "rank": None, "total": None, "source": "同花顺人气"}
    for quote in quotes:
        quote["popularity"] = results[quote["code"]]


def add_related_sectors(quotes: List[Dict[str, Any]]) -> None:
    """Attach the highest-heat Eastmoney concept as a display-only related sector."""
    if not quotes:
        return
    results: Dict[str, List[str]] = {}
    with ThreadPoolExecutor(max_workers=min(12, len(quotes))) as executor:
        futures = {executor.submit(get_stock_concepts, quote["code"]): quote["code"] for quote in quotes}
        for future in as_completed(futures):
            code = futures[future]
            try:
                concepts = future.result()
                results[code] = [str(concept["name"]) for concept in concepts[:5] if concept.get("name")]
            except MarketDataError:
                results[code] = []
    for quote in quotes:
        names = results.get(quote["code"], [])
        primary = select_primary_business_concept(quote["code"], quote.get("actualIndustry"), names)
        quote["relatedSectors"] = ([primary] if primary else []) + [name for name in names if name != primary]


# 已人工确认的主营产品概念优先级。它只影响页面标签，不参与评分。
PRIMARY_BUSINESS_CONCEPT_OVERRIDES = {
    "002201": "玻璃玻纤",
    "002585": "MLCC概念",
    "300285": "MLCC概念",
    "300408": "MLCC概念",
    "300563": "铜缆高速连接",
    "300852": "PCB概念",
    "605606": "玻璃玻纤",
    "688020": "PCB概念",
    "688143": "光纤概念",
    "688260": "MLCC概念",
    "688655": "PCB概念",
}


def select_primary_business_concept(code: str, industry: Any, concepts: List[str]) -> str:
    """Choose a business-relevant concept before falling back to provider order."""
    override = PRIMARY_BUSINESS_CONCEPT_OVERRIDES.get(str(code))
    if override:
        return override
    names = [str(name).strip() for name in concepts if str(name).strip()]
    if not names:
        return ""
    industry_text = str(industry or "").replace("Ⅱ", "").replace("Ⅲ", "").strip()
    if industry_text:
        matched = [name for name in names if industry_text in name or name.replace("概念", "") in industry_text]
        if matched:
            return matched[0]
    return names[0]


def fetch_downtrend_history(code: str) -> List[Dict[str, Any]]:
    """Fetch daily bars for the downtrend filter, with a Sina fallback for moving averages."""
    symbol = stock_symbol(code)
    try:
        payload = request_json(
            QUOTE_URL,
            {"param": f"{symbol},day,,,{DOWNTREND_HISTORY_DAYS},qfq"},
        )
        data = (payload.get("data") or {}).get(symbol) or {}
        rows: List[Dict[str, Any]] = []
        for fields in data.get("qfqday") or data.get("day") or []:
            if len(fields) < 5:
                continue
            open_price = number(fields[1])
            close = number(fields[2])
            if close is not None:
                rows.append({"date": fields[0], "open": open_price, "close": close, "high": number(fields[3]), "low": number(fields[4])})
        if payload.get("code") == 0 and rows:
            return rows[-DOWNTREND_HISTORY_DAYS:]
    except MarketDataError:
        pass

    payload = request_json(
        SINA_DAILY_KLINE_URL,
        {"symbol": symbol, "scale": 240, "ma": "no", "datalen": DOWNTREND_HISTORY_DAYS},
    )
    rows = [
        {"date": str(row.get("day") or ""), "open": number(row.get("open")), "close": close, "high": number(row.get("high")), "low": number(row.get("low"))}
        for row in (payload if isinstance(payload, list) else [])
        if isinstance(row, dict)
        for close in [number(row.get("close"))]
        if close is not None
    ]
    if not rows:
        raise MarketDataError(f"{code} 日线数据不足")
    return rows[-DOWNTREND_HISTORY_DAYS:]


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


def calculate_macd(closes: List[float]) -> Dict[str, Any]:
    if len(closes) < 26:
        return {"available": False, "dif": None, "dea": None, "histogram": None, "signal": "数据不足", "bars": []}
    def ema(values: List[float], period: int) -> List[float]:
        result = [values[0]]
        alpha = 2 / (period + 1)
        for value in values[1:]:
            result.append(value * alpha + result[-1] * (1 - alpha))
        return result
    fast = ema(closes, 12)
    slow = ema(closes, 26)
    diffs = [a - b for a, b in zip(fast, slow)]
    deas = ema(diffs, 9)
    histograms = [(dif - dea) * 2 for dif, dea in zip(diffs, deas)]
    histogram = histograms[-1]
    previous = histograms[-2]
    if diffs[-1] > deas[-1] and diffs[-2] <= deas[-2]:
        signal = "金叉"
    elif diffs[-1] < deas[-1] and diffs[-2] >= deas[-2]:
        signal = "死叉"
    elif histogram > 0 and histogram >= previous:
        signal = "多头增强"
    elif histogram > 0:
        signal = "多头收敛"
    elif histogram <= previous:
        signal = "空头增强"
    else:
        signal = "空头收敛"
    return {"available": True, "dif": round(diffs[-1], 4), "dea": round(deas[-1], 4), "histogram": round(histogram, 4), "signal": signal, "bars": [round(value, 4) for value in histograms[-90:]]}


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
    moving_averages = {
        f"ma{period}": round(sum(closes[-period:]) / period, 2) if len(closes) >= period else None
        for period in (5, 10, 20, 30)
    }
    macd = calculate_macd(closes)
    if macd.get("available"):
        macd["barDates"] = [str(row.get("date") or "")[5:] for row in history[-len(macd.get("bars") or []):]]
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
    recent_rows = [row for row in history if row.get("close") not in (None, 0)][-22:]
    pattern_signals: List[str] = []
    pattern_score = 0
    valid_ohlc = all(
        row.get(key) is not None
        for row in recent_rows
        for key in ("open", "high", "low", "close")
    )
    if valid_ohlc and len(recent_rows) >= 8:
        current = recent_rows[-1]
        body = abs(current["close"] - current["open"])
        candle_range = max(current["high"] - current["low"], 0.01)
        lower_shadow = min(current["open"], current["close"]) - current["low"]
        upper_shadow = current["high"] - max(current["open"], current["close"])
        prior_decline = recent_rows[-7]["close"] > recent_rows[-2]["close"]
        hammer_shape = body <= candle_range * .32 and lower_shadow >= max(body * 2, candle_range * .48) and upper_shadow <= candle_range * .18
        if prior_decline and hammer_shape:
            pattern_signals.append("锤子线")
            pattern_score += 8
        prior_low = min(row["low"] for row in recent_rows[-8:-1])
        if prior_decline and hammer_shape and current["low"] <= prior_low and current["close"] >= current["low"] + candle_range * .62:
            pattern_signals.append("金针探底")
            pattern_score += 9
    if valid_ohlc and len(recent_rows) >= 6:
        first, star, third = recent_rows[-3:]
        first_body = first["open"] - first["close"]
        star_body = abs(star["close"] - star["open"])
        third_body = third["close"] - third["open"]
        if first_body > 0 and third_body > 0 and star_body <= first_body * .55 and third["close"] >= first["close"] + first_body * .55 and recent_rows[-6]["close"] > first["close"]:
            pattern_signals.append("启明星")
            pattern_score += 12
        previous, current = recent_rows[-2:]
        if recent_rows[-6]["close"] > previous["close"] and previous["close"] < previous["open"] and current["close"] > current["open"] and current["open"] <= previous["close"] and current["close"] >= previous["open"]:
            pattern_signals.append("看涨吞没")
            pattern_score += 11
    if len(recent_rows) >= 5 and all(row.get("open") is not None for row in recent_rows[-5:]):
        small_yang = sum(row["close"] > row["open"] and return_pct(row["open"], row["close"]) <= 3.5 for row in recent_rows[-5:])
        if small_yang >= 4 and closes[-1] > closes[-5]:
            pattern_signals.append("碎步小阳")
            pattern_score += 10
    if len(recent_rows) >= 3 and all(row.get("open") is not None for row in recent_rows[-3:]):
        last_three = recent_rows[-3:]
        if all(row["close"] > row["open"] for row in last_three) and last_three[0]["close"] < last_three[1]["close"] < last_three[2]["close"]:
            pattern_signals.append("红三兵")
            pattern_score += 12
        first, middle, last = last_three
        if first["close"] > first["open"] and middle["close"] < middle["open"] and last["close"] > last["open"] and abs(first["close"] - first["open"]) > abs(middle["close"] - middle["open"]) and abs(last["close"] - last["open"]) > abs(middle["close"] - middle["open"]) and middle["low"] >= min(first["open"], first["close"]) * .97 and last["close"] > first["close"]:
            pattern_signals.append("多方炮")
            pattern_score += 10
    if valid_ohlc and len(recent_rows) >= 5:
        five = recent_rows[-5:]
        first, *middle, last = five
        first_body = first["close"] - first["open"]
        if first_body > 0 and last["close"] > last["open"] and last["close"] > first["close"] and all(row["high"] <= first["high"] * 1.01 and row["low"] >= first["low"] * .99 for row in middle):
            pattern_signals.append("上升三部曲")
            pattern_score += 11
    if len(recent_rows) >= 4 and all(row.get("open") is not None for row in recent_rows[-4:]):
        previous = recent_rows[-4:-1]
        current = recent_rows[-1]
        if all(row["close"] < row["open"] for row in previous) and current["close"] > current["open"] and current["open"] <= min(row["close"] for row in previous) and current["close"] >= max(row["open"] for row in previous):
            pattern_signals.append("一阳包三阴")
            pattern_score += 14
    if len(closes) >= 20:
        low_index = closes[-20:].index(min(closes[-20:]))
        if 5 <= low_index <= 14 and closes[-1] > sum(closes[-5:]) / 5 > min(closes[-20:]):
            pattern_signals.append("圆弧底")
            pattern_score += 10
        if valid_ohlc:
            twenty = recent_rows[-20:]
            local_lows = [i for i in range(1, len(twenty) - 1) if twenty[i]["low"] <= twenty[i - 1]["low"] and twenty[i]["low"] <= twenty[i + 1]["low"]]
            double_bottom = False
            for left in local_lows:
                for right in local_lows:
                    if right - left < 4:
                        continue
                    low_a, low_b = twenty[left]["low"], twenty[right]["low"]
                    neckline = max(row["high"] for row in twenty[left:right + 1])
                    if abs(low_a - low_b) / max(low_a, low_b, .01) <= .035 and latest >= neckline:
                        double_bottom = True
                        break
                if double_bottom:
                    break
            if double_bottom:
                pattern_signals.append("双底")
                pattern_score += 13
            impulse = return_pct(twenty[-12]["close"], twenty[-6]["close"])
            consolidation = twenty[-6:-1]
            if impulse >= 8 and consolidation[-1]["high"] <= consolidation[0]["high"] * 1.02 and latest > max(row["high"] for row in consolidation):
                pattern_signals.append("旗形上涨")
                pattern_score += 10
            recent_15 = twenty[-15:]
            resistance = max(row["high"] for row in recent_15[:-1])
            high_band = [row["high"] for row in recent_15[:-1] if row["high"] >= resistance * .97]
            if len(high_band) >= 2 and min(row["low"] for row in recent_15[-5:]) > min(row["low"] for row in recent_15[:5]) * 1.02 and latest >= resistance * .995:
                pattern_signals.append("上升三角形")
                pattern_score += 11
        boll_mid = sum(closes[-20:]) / 20
        boll_std = statistics.pstdev(closes[-20:])
        if latest > boll_mid + 2 * boll_std and moving_averages["ma5"] > moving_averages["ma10"] > moving_averages["ma20"]:
            pattern_signals.append("均线多头-布林突破")
            pattern_score += 14

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
        "movingAverages": moving_averages,
        "macd": macd,
        "patternSignals": pattern_signals,
        "patternScore": min(40, pattern_score),
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
        averages = analysis.get("movingAverages") or quote.get("screener", {}).get("movingAverages") or quote.get("movingAverages") or {}
        quote["screener"]["movingAverages"] = {
            key: {
                "value": value,
                "above": value is not None and quote.get("price") is not None and quote["price"] >= value,
            }
            for key, value in averages.items()
        }
        quote["screener"]["downtrend"] = analysis
        if analysis["excluded"]:
            excluded_count += 1
            continue
        filtered_quotes.append(quote)
    return filtered_quotes, excluded_count


def fetch_ths_stock_concepts(code: str) -> List[Dict[str, Any]]:
    request = urllib.request.Request(
        THS_BASIC_STOCK_URL.format(code=code),
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://stock.10jqka.com.cn/"},
    )
    with urllib.request.urlopen(request, timeout=SUPPLEMENT_REQUEST_TIMEOUT_SECONDS) as response:
        page = response.read().decode("gbk", errors="ignore")
    start = page.find("概念行情贴合度")
    if start < 0:
        raise MarketDataError(f"{code} 同花顺概念数据不可用")
    section = page[start:start + 12000]
    names: List[str] = []
    for match in re.finditer(r"<a[^>]*newtaid[^>]*>(.*?)</a>", section, re.I | re.S):
        name = html.unescape(re.sub(r"<[^>]+>", "", match.group(1))).strip(" \t\r\n，,")
        if name and name not in names and len(name) <= 30:
            names.append(name)
    if not names:
        raise MarketDataError(f"{code} 同花顺概念数据不可用")
    return [{"name": name, "heat": len(names) - index, "source": "同花顺"} for index, name in enumerate(names[:5])]


def fetch_stock_concepts(code: str) -> List[Dict[str, Any]]:
    try:
        ths_concepts = fetch_ths_stock_concepts(code)
        if ths_concepts:
            return ths_concepts
    except (OSError, urllib.error.URLError, TimeoutError, MarketDataError, ValueError):
        pass

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
                "source": "东方财富",
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

    try:
        with open_backtest_db() as connection:
            row = connection.execute(
                "SELECT concepts_json, fetched_at FROM stock_concept_cache WHERE code = ?",
                (code,),
            ).fetchone()
        if row and time.time() - float(row[1]) < CONCEPT_DB_REFRESH_SECONDS:
            concepts = json.loads(row[0])
            if isinstance(concepts, list):
                with _cache_lock:
                    _concept_cache[code] = {"created_at": now, "payload": concepts}
                return concepts
    except (OSError, sqlite3.Error, TypeError, ValueError, json.JSONDecodeError):
        pass

    concepts = fetch_stock_concepts(code)
    try:
        with open_backtest_db() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO stock_concept_cache (code, concepts_json, fetched_at, source) VALUES (?, ?, ?, ?)",
                (code, json.dumps(concepts, ensure_ascii=False), time.time(), "同花顺优先/东方财富备用"),
            )
            connection.commit()
    except sqlite3.Error:
        pass
    with _cache_lock:
        _concept_cache[code] = {
            "created_at": time.monotonic(),
            "payload": concepts,
        }
    return concepts


def build_sector_scoring_context(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Build a lightweight, market-wide industry heat/breadth context for live scoring."""
    grouped: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
    for row in rows:
        industry = str(row.get("f100") or "").strip()
        change = number(row.get("f3"))
        if industry and change is not None and abs(change) <= 30.5:
            grouped[industry].append(row)
    contexts: List[Dict[str, Any]] = []
    for industry, members in grouped.items():
        if len(members) < 3:
            continue
        changes = [float(number(item.get("f3")) or 0) for item in members]
        rising = sum(value > 0 for value in changes)
        strong = sum(value >= 2 for value in changes)
        amount = sum(max(0.0, float(number(item.get("f6")) or 0)) for item in members)
        net_flow = sum(float(number(item.get("f62")) or 0) for item in members)
        average = statistics.fmean(changes)
        breadth = rising / len(changes) * 100
        strong_ratio = strong / len(changes) * 100
        flow_ratio = net_flow / amount * 100 if amount else 0
        heat_score = clamp(50 + average * 9 + (breadth - 50) * .35 + min(strong_ratio, 30) * .35 + max(-10, min(10, flow_ratio * 8)))
        peer_score = clamp(25 + breadth * .55 + min(strong_ratio, 35) * .65)
        contexts.append({"industry": industry, "heatScore": heat_score, "peerScore": peer_score, "averageChange": average, "breadth": breadth, "strongCount": strong, "memberCount": len(changes), "netFlow": net_flow, "flowRatio": flow_ratio})
    contexts.sort(key=lambda item: (item["heatScore"], item["averageChange"]), reverse=True)
    top_count = max(1, math.ceil(len(contexts) * .30))
    result: Dict[str, Dict[str, Any]] = {}
    for rank, item in enumerate(contexts, 1):
        item["rank"] = rank
        item["hot"] = rank <= top_count
        item["confirmed"] = bool(item["hot"] and item["breadth"] >= 55 and item["strongCount"] >= 3 and item["flowRatio"] >= -.5)
        result[item["industry"]] = item
    # 概念板块成分抓取较重，绝不能阻塞页面行情和全市场扫描。
    # 当前请求只读取已有缓存；缓存缺失或过期时在后台刷新，下一轮自动使用。
    with _cache_lock:
        cached = _concept_scoring_context_cache.get("all")
        if cached:
            result.update(cached.get("payload") or {})
        needs_refresh = not cached or time.monotonic() - cached.get("created_at", 0) >= 180
        should_start = needs_refresh and not _concept_scoring_refresh_state["running"]
        if should_start:
            _concept_scoring_refresh_state["running"] = True
    if should_start:
        threading.Thread(target=refresh_concept_scoring_context, name="concept-scoring-refresh", daemon=True).start()
    return result


def dynamic_snapshot(
    row: Dict[str, Any], updated_at: datetime, market_key: str
) -> Optional[Dict[str, Any]]:
    market = MARKET_SCOPES[market_key]
    code = str(row.get("f12") or "")
    name = str(row.get("f14") or "")
    listing_date = str(row.get("f26") or "").strip()
    if name.upper().startswith(("N", "C")):
        return None
    if len(listing_date) == 8 and listing_date.isdigit():
        try:
            listed = datetime.strptime(listing_date, "%Y%m%d").replace(tzinfo=CHINA_TZ)
            if updated_at - listed < timedelta(days=180):
                return None
        except ValueError:
            pass
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
    # 高开 7% 及以上且已跌回开盘价下方，或盘中涨停后跳水，当日不再参与
    # 候选、评分记录和告警判定，防止因涨幅回落而重新入池。
    if high_open_fade(row) or intraday_limit_up_fade(row) or blacklisted_industry(row):
        return None

    net_inflow = number(row.get("f62"))
    volume_ratio = number(row.get("f10"))
    factors = dynamic_factors(change_pct, amount, turnover, amplitude, net_inflow)
    base_score_reference = round(
        factors["trend"] * 0.22
        + factors["momentum"] * 0.28
        + factors["liquidity"] * 0.2
        + factors["strength"] * 0.2
        + factors["stability"] * 0.1
        - factors["risk"] * 0.1
    )
    trade_date = updated_at.astimezone(CHINA_TZ).strftime("%Y-%m-%d")
    score_time = updated_at.astimezone(CHINA_TZ).strftime("%H:%M:%S")
    observation_key = (trade_date, code)
    previous_score, previous_at = _score_observations.get(observation_key, (float(base_score_reference), time.monotonic()))
    elapsed_minutes = max((time.monotonic() - previous_at) / 60, 0.5)
    score_velocity = (base_score_reference - previous_score) / elapsed_minutes
    _score_observations[observation_key] = (float(base_score_reference), time.monotonic())
    flow_ratio = (net_inflow / amount * 100) if net_inflow is not None and amount > 0 else 0.0
    # 告警专用评分：实时量价65% + 资金15% + 分时动能/MACD代理15% + 风险5%。
    # 全市场扫描不逐只请求分钟K线，分时动能代理由评分速度、量比、振幅和涨跌背离构成。
    price_volume_score = (
        factors["trend"] * 0.12
        + factors["momentum"] * 0.20
        + factors["liquidity"] * 0.13
        + factors["strength"] * 0.10
    )
    capital_score = clamp(50 + flow_ratio * 12) if net_inflow is not None else 50
    actual_industry = str(row.get("f100") or "").strip()
    individual_strength_score = clamp(50 + change_pct * 4 + (2 if amplitude >= 3 else 0))
    intraday_macd_score = clamp(
        50
        + score_velocity * 18
        + max(-12, min(12, ((volume_ratio or 1) - 1) * 8))
        + (6 if change_pct > 0 and change_pct >= amplitude * 0.55 else 0)
        - (6 if change_pct > 6 and score_velocity < 0 else 0)
    )
    risk_quality_score = clamp(100 - factors["risk"])
    score = round(
        price_volume_score
        + capital_score * 0.15
        + individual_strength_score * 0.10
        + intraday_macd_score * 0.15
        + risk_quality_score * 0.05
    )
    # 成交额是盘中评分的基础有效性门槛。未达到 1 亿元时，任何评分都
    # 只能停留在观察线（60 分），不能借由趋势或 MACD 抬升到确认/预警区。
    amount_gate_passed = amount >= 100_000_000
    if not amount_gate_passed:
        score = min(score, 60)
    strategy_weights = {
        "balanced": {"trend": 22, "momentum": 20, "liquidity": 18, "strength": 18, "stability": 12, "risk": 10},
        "growth": {"trend": 28, "momentum": 28, "liquidity": 15, "strength": 15, "stability": 6, "risk": 8},
        "value": {"trend": 16, "momentum": 10, "liquidity": 16, "strength": 12, "stability": 34, "risk": 12},
        "event": {"trend": 18, "momentum": 30, "liquidity": 14, "strength": 25, "stability": 5, "risk": 8},
    }
    strategy_scores = {}
    for strategy_name, weights in strategy_weights.items():
        weighted_base = sum(float(factors[key]) * weights[key] for key in ("trend", "momentum", "liquidity", "strength", "stability"))
        weighted_base -= float(factors["risk"]) * weights["risk"]
        base = clamp(weighted_base / 90)
        adjusted = round(base * .85 + intraday_macd_score * .15)
        if not amount_gate_passed:
            adjusted = min(60, adjusted)
        strategy_scores[strategy_name] = adjusted
    strategy_confirmed = sum(value >= 70 for value in strategy_scores.values()) >= 2
    main_strategy_confirmed = sum(value >= 72 for value in strategy_scores.values()) >= 2
    strategy_early = sum(value > 60 for value in strategy_scores.values()) >= 2
    highest_strategy_score = max(strategy_scores.values())
    strategy_average = sum(strategy_scores.values()) / len(strategy_scores)
    strategy_spread = max(strategy_scores.values()) - min(strategy_scores.values())
    main_board_elite = (
        not is_main_board_code(code)
        or (
            main_strategy_confirmed
            and strategy_average >= 70
            and max(strategy_scores.values()) >= 75
            and strategy_spread <= 10
            and (volume_ratio or 0) >= 1.5
            and amount >= 100_000_000
            and (flow_ratio >= 0 or net_inflow is None)
            and change_pct <= 8
        )
    )
    alert_confirmed = (main_strategy_confirmed if is_main_board_code(code) else strategy_confirmed) and main_board_elite
    alert_early = strategy_early and main_board_elite
    momentum_support = 0
    if score_velocity >= .15: momentum_support += 4
    if (volume_ratio or 0) >= 1.2: momentum_support += 3
    if flow_ratio > 0: momentum_support += 3
    if amplitude >= 3: momentum_support += 2
    chase_penalty = max(0, change_pct - 7) * 2.5
    # 预测性入池：允许评分尚未到 70 分、但多个领先指标已经同步改善的股票提前进入观察池。
    # 70 分仍是确认线；预测线只负责提前发现，不直接等同于确认信号。
    # 提前预警不再只采用一个综合概率。四种策略分别按自身评分叠加盘中
    # 动能、量能、资金和追高惩罚计算胜率；取第三高值作为三策略共识概率。
    # 因此只有至少三种策略都站上同一概率阈值，才会产生提前预警。
    strategy_predictive_scores = {}
    strategy_predictive_probabilities = {}
    for strategy_name, strategy_score in strategy_scores.items():
        strategy_predictive_score = min(95, max(0, strategy_score + momentum_support + score_velocity * 5 - chase_penalty))
        if not amount_gate_passed:
            strategy_predictive_score = min(60, strategy_predictive_score)
        strategy_predictive_scores[strategy_name] = round(strategy_predictive_score)
        strategy_predictive_probabilities[strategy_name] = round(min(
            95,
            max(1, 35 + (strategy_predictive_score - 60) * 6 + max(0, score_velocity) * 12 - max(0, change_pct - 7) * 5),
        ))
    consensus_predictive_scores = sorted(strategy_predictive_scores.values(), reverse=True)
    consensus_predictive_probabilities = sorted(strategy_predictive_probabilities.values(), reverse=True)
    predictive_score = consensus_predictive_scores[2]
    predictive_probability = consensus_predictive_probabilities[2]
    strategy_predictive_confirmed = sum(value >= 80 for value in strategy_predictive_probabilities.values()) >= 3
    early_momentum = momentum_support >= 6 and (volume_ratio or 0) >= 1.1
    early_flow = flow_ratio > 0.15
    pre_alert = (
        56 <= score < 70
        and predictive_score >= 66
        and strategy_predictive_confirmed
        and change_pct <= 7.5
        and (early_momentum or early_flow)
        and alert_early
    )
    with _cache_lock:
        if is_intraday_alert_session(updated_at):
            # 预测概率分级告警：每个级别只记录首次达到时间，70 分为最终确认告警。
            for threshold in (80, 85, 90, 95):
                key = (trade_date, code, threshold)
                if pre_alert and sum(value >= threshold for value in strategy_predictive_probabilities.values()) >= 3 and key not in _prediction_threshold_times:
                    _prediction_threshold_times[key] = score_time
                    _prediction_threshold_names[key] = name or code
                    _prediction_threshold_quotes[key] = {"price": price, "changePct": change_pct}
                    save_alert_event(
                        trade_date, code, name, "prediction", threshold, score_time,
                        {"price": price, "changePct": change_pct, "volumeRatio": volume_ratio,
                         "turnover": turnover, "amplitude": amplitude, "netInflow": net_inflow},
                        score, predictive_score, predictive_probability, factors,
                        intraday_macd_score,
                    )
                    persist_threshold_times()
            for threshold in (60, 70):
                key = (trade_date, code, threshold)
                # 70 分确认线以“至少两种策略各自达到 70 分”为准，
                # 不再额外要求综合快照分也必须到 70，避免共识信号被平均分滞后。
                threshold_met = score >= 60 if threshold == 60 else strategy_confirmed
                if alert_confirmed and threshold_met and key not in _score_threshold_times:
                    _score_threshold_times[key] = score_time
                    _score_threshold_names[key] = name or code
                    _score_threshold_quotes[key] = {"price": price, "changePct": change_pct, "strategyScore": highest_strategy_score, "industry": actual_industry}
                    save_alert_event(
                        trade_date, code, name, "score", threshold, score_time,
                        {"price": price, "changePct": change_pct, "volumeRatio": volume_ratio,
                         "turnover": turnover, "amplitude": amplitude, "netInflow": net_inflow},
                        highest_strategy_score, predictive_score, predictive_probability, factors,
                        intraday_macd_score,
                    )
                    persist_threshold_times()
            for threshold in (3, 6):
                key = (trade_date, code, threshold)
                if change_pct >= threshold and key not in _change_threshold_times:
                    _change_threshold_times[key] = score_time
                    persist_threshold_times()
        day_average = statistics.mean(
            value for value in (number(row.get("f17")), number(row.get("f15")), number(row.get("f16")), price)
            if value is not None
        )
        recommendation = None
        limit_rate = .20 if code.startswith(("300", "301", "688", "689")) else (.30 if code.startswith(("4", "8", "92")) else .10)
        if previous_close and price >= previous_close * (1 + limit_rate) - .01:
            recommendation = "已涨停"
        elif change_pct >= 6 and number(row.get("f15")) and price >= number(row.get("f15")) * .985 and (volume_ratio or 0) >= 2:
            recommendation = "等待回落"
        elif change_pct >= 5 and day_average and price < day_average and number(row.get("f15")) and price < number(row.get("f15")) * .97:
            recommendation = "冲高回落风险"
        elif change_pct > 8:
            recommendation = "谨慎追高"
        elif score >= 70 and 3 <= change_pct <= 8 and day_average:
            flow_ratio = (net_inflow / amount * 100) if net_inflow is not None and amount > 0 else None
            near_high = number(row.get("f15")) is not None and price >= number(row.get("f15")) * .975
            stable_intraday = price >= day_average and price >= number(row.get("f17") or price)
            reasonable_volume = volume_ratio is None or 1.2 <= volume_ratio <= 4.5
            healthy_flow = flow_ratio is not None and flow_ratio > 0
            if stable_intraday and not near_high and reasonable_volume and healthy_flow:
                recommendation = "推荐买入"
            else:
                recommendation = "推荐观望"
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
        "actualIndustry": actual_industry,
        "theme": "市场动态扫描",
        "marketCap": number(row.get("f20")),
        "floatMarketCap": number(row.get("f21")),
        "peTtm": number(row.get("f9")),
        "volumeRatio": number(row.get("f10")),
        "netInflow": net_inflow,
        "pb": number(row.get("f23")),
        "base": factors,
        "screener": {
            "turnover": turnover,
            "amplitude": amplitude,
            "snapshotScore": score,
            "strategyScores": strategy_scores,
            "strategyConfirmed": strategy_confirmed,
            "strategyEarly": strategy_early,
            "strategyAverage": round(strategy_average, 1),
            "strategySpread": round(strategy_spread, 1),
            "mainBoardElite": main_board_elite,
            "amountGatePassed": amount_gate_passed,
            "predictiveScore": predictive_score,
            "predictiveProbability": predictive_probability,
            "strategyPredictiveScores": strategy_predictive_scores,
            "strategyPredictiveProbabilities": strategy_predictive_probabilities,
            "strategyPredictiveConfirmed": strategy_predictive_confirmed,
            "preAlert": pre_alert,
            "preAlertReason": "预测评分正在向70分靠近：评分趋势、量价动能或资金流已出现同步改善；这是提前观察信号，不代表已确认达到70分。" if pre_alert else None,
            "predictiveThresholdTimes": {str(level): _prediction_threshold_times.get((trade_date, code, level)) for level in (80, 85, 90, 95)},
            "scoreThresholdTimes": {
                "60": _score_threshold_times.get((trade_date, code, 60)),
                "70": _score_threshold_times.get((trade_date, code, 70)),
            },
            "changeThresholdTimes": {
                "3": _change_threshold_times.get((trade_date, code, 3)),
                "6": _change_threshold_times.get((trade_date, code, 6)),
            },
            "dayAverage": day_average,
            "recommendation": recommendation,
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


def tencent_quote_symbol(code: str) -> str:
    return ("sh" if code.startswith(("5", "6", "9")) else "sz") + code


def add_order_flow_factors(quotes: List[Dict[str, Any]], sectors: List[Dict[str, Any]]) -> None:
    """Attach Tencent inner/outer volume and a context-confirmed, low-weight order-flow score."""
    if not quotes:
        return
    sector_by_name = {item["name"]: item for item in sectors}
    rows_by_code: Dict[str, List[str]] = {}
    for start in range(0, len(quotes), 60):
        symbols = ",".join(tencent_quote_symbol(item["code"]) for item in quotes[start:start + 60])
        try:
            result = subprocess.run(
                ["curl", "-fsSL", "--max-time", str(REQUEST_TIMEOUT_SECONDS), f"{TENCENT_BATCH_QUOTE_URL}{symbols}"],
                check=True, capture_output=True, timeout=REQUEST_TIMEOUT_SECONDS + 2,
            )
            content = result.stdout.decode("gb18030", errors="replace")
        except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
            continue
        for line in content.splitlines():
            if '="' not in line:
                continue
            fields = line.split('="', 1)[1].rstrip('";').split("~")
            if len(fields) > 51 and len(fields[2]) == 6:
                rows_by_code[fields[2]] = fields

    for quote in quotes:
        fields = rows_by_code.get(quote["code"])
        if not fields:
            quote["orderFlow"] = {"available": False, "source": "腾讯盘口暂不可用"}
            continue
        outer = number(fields[7])
        inner = number(fields[8])
        bid_volumes = [number(fields[index]) or 0 for index in (10, 12, 14, 16, 18)]
        ask_volumes = [number(fields[index]) or 0 for index in (20, 22, 24, 26, 28)]
        total_flow = float(outer or 0) + float(inner or 0)
        total_orders = sum(bid_volumes) + sum(ask_volumes)
        if total_flow <= 0:
            quote["orderFlow"] = {"available": False, "source": "腾讯盘口无有效内外盘"}
            continue
        commission_ratio = ((sum(bid_volumes) - sum(ask_volumes)) / total_orders * 100) if total_orders else None
        flow_score = clamp(50 + (float(outer or 0) - float(inner or 0)) / total_flow * 50)
        book_score = clamp(50 + float(commission_ratio or 0) / 2) if commission_ratio is not None else 50
        high, low, price = quote.get("high"), quote.get("low"), quote.get("price")
        position_score = clamp((float(price) - float(low)) / (float(high) - float(low)) * 100) if None not in (high, low, price) and high != low else 50
        volume_score = clamp(float(quote.get("volumeRatio") or 0) / 3 * 100)
        analysis = quote.get("screener", {}).get("downtrend") or {}
        pattern_score = clamp(float(analysis.get("patternScore") or 0) * 2)
        averages = quote.get("screener", {}).get("movingAverages") or {}
        ma_bull = sum(1 for key in ("ma5", "ma10", "ma20", "ma30") if (averages.get(key) or {}).get("above"))
        ma_score = ma_bull / 4 * 100
        sector_score = float(sector_by_name.get(quote.get("actualIndustry"), {}).get("strengthScore") or 50)
        divergence = abs(flow_score - book_score)
        confirmed_score = clamp(
            flow_score * .25 + book_score * .15 + volume_score * .15 + position_score * .10
            + pattern_score * .10 + ma_score * .10 + sector_score * .15
            - max(0, divergence - 35) * .35
        )
        quote["orderFlow"] = {
            "available": True,
            "source": "腾讯实时盘口",
            "innerVolume": int(inner or 0),
            "outerVolume": int(outer or 0),
            "commissionRatio": round(commission_ratio, 2) if commission_ratio is not None else None,
            "score": confirmed_score,
            "divergence": round(divergence),
            "note": "内外盘仅作低权重确认，并结合委比、价格位置、量比、K线形态、均线和板块强度；盘口背离时已降权。",
        }
        quote["base"]["orderFlow"] = confirmed_score


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
        "fields": "f2,f3,f5,f6,f7,f8,f9,f10,f12,f14,f15,f16,f17,f18,f20,f21,f23,f26,f62,f100,f124",
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


def normalize_concept_name(name: str) -> str:
    normalized = str(name or "").strip().upper()
    for suffix in ("概念板块", "概念", "板块", "行业"):
        normalized = normalized.replace(suffix, "")
    return normalized.replace(" ", "")


def resolve_hot_concept(concept_name: str) -> Tuple[Dict[str, Any], str]:
    requested = normalize_concept_name(concept_name)
    fixed = HOT_CONCEPT_FIXED_BOARDS.get(concept_name)
    if fixed:
        return {"Code": fixed[0], "Name": fixed[1]}, "fixed"
    best: Optional[Tuple[float, Dict[str, Any], str]] = None
    search_names = (concept_name,) + HOT_CONCEPT_ALIASES.get(concept_name, ())
    for index, search_name in enumerate(search_names):
        search = request_json(EASTMONEY_SEARCH_URL, {
            "input": search_name, "type": 14,
            "token": "D43BF722C8E33BDC906FB84D85E326E8",
        })
        matches = ((search.get("QuotationCodeTable") or {}).get("Data") or [])
        for row in matches:
            if not str(row.get("Code") or "").startswith("BK"):
                continue
            candidate = normalize_concept_name(row.get("Name"))
            target = normalize_concept_name(search_name)
            if candidate == requested:
                return row, "exact"
            score = difflib.SequenceMatcher(None, target, candidate).ratio()
            if target and (target in candidate or candidate in target):
                score = max(score, .86)
            if index > 0 and candidate == target:
                score = .95
            if best is None or score > best[0]:
                best = (score, row, "alias" if index > 0 else "similar")
    if best and best[0] >= .72:
        return best[1], best[2]
    raise MarketDataError(f"未能可靠匹配概念板块：{concept_name}")


def fetch_hot_concept_data(concept_name: str) -> Dict[str, Any]:
    exact, match_type = resolve_hot_concept(concept_name)
    if not exact:
        raise MarketDataError(f"未找到概念板块：{concept_name}")
    board_code = str(exact["Code"])
    resolved_name = str(exact.get("Name") or concept_name)
    params = {
        "pn": 1, "pz": 100, "po": 1, "np": 1, "fltt": 2, "invt": 2,
        "fid": "f3", "fs": f"b:{board_code}",
        "fields": "f2,f3,f5,f6,f7,f8,f9,f10,f12,f14,f15,f16,f17,f18,f20,f21,f23,f62,f100,f124",
    }
    payload = request_json(EASTMONEY_LIST_URL, params)
    data = payload.get("data") or {}
    rows = list(data.get("diff") or [])
    updated_at = datetime.now(CHINA_TZ)
    quotes = [dynamic_snapshot(row, updated_at, "all") for row in rows]
    quotes = [quote for quote in quotes if quote is not None]
    screened, _ = filter_steady_decline_quotes(quotes)
    sector_strength = aggregate_sector_strength(quotes)
    add_order_flow_factors(screened, sector_strength)
    add_probability_models(screened, sector_strength, updated_at.strftime("%Y-%m-%d"))
    screened.sort(key=lambda item: (item.get("probability", {}).get("todayLimitUp", 0), item.get("changePct", 0)), reverse=True)
    for quote in screened:
        quote["relatedSectors"] = [concept_name]
        quote["popularity"] = {"available": False, "source": "热门板块页不重复请求人气"}
    return {
        "source": f"东方财富{resolved_name}板块行情",
        "concept": concept_name,
        "resolvedConcept": resolved_name,
        "matchType": match_type,
        "boardCode": board_code,
        "marketDate": updated_at.strftime("%Y-%m-%d"),
        "updatedAt": updated_at.isoformat(),
        "scannedCount": int(data.get("total") or len(rows)),
        "matchedCount": len(screened),
        "averageChangePct": round(sum(float(quote.get("changePct") or 0) for quote in quotes) / len(quotes), 2) if quotes else None,
        "quotes": screened,
    }


def get_hot_concept_data(concept_name: str, force_refresh: bool = False) -> Dict[str, Any]:
    now = time.monotonic()
    with _cache_lock:
        cached = _hot_concept_cache.get(concept_name)
        if cached and now - cached["created_at"] < DYNAMIC_CACHE_TTL_SECONDS and not force_refresh:
            return cached["payload"]
    payload = fetch_hot_concept_data(concept_name)
    with _cache_lock:
        _hot_concept_cache[concept_name] = {"created_at": time.monotonic(), "payload": payload}
    return payload


def fetch_hot_concept_summary(concept_name: str) -> Dict[str, Any]:
    board, match_type = resolve_hot_concept(concept_name)
    board_code = str(board["Code"])
    payload = request_json(EASTMONEY_LIST_URL, {
        "pn": 1, "pz": 100, "po": 1, "np": 1, "fltt": 2, "invt": 2,
        "fid": "f3", "fs": f"b:{board_code}", "fields": "f3,f12",
    })
    data = payload.get("data") or {}
    changes = [number(row.get("f3")) for row in (data.get("diff") or [])]
    changes = [value for value in changes if value is not None]
    return {
        "concept": concept_name,
        "resolvedConcept": str(board.get("Name") or concept_name),
        "matchType": match_type,
        "averageChangePct": round(sum(changes) / len(changes), 2) if changes else None,
        "memberCount": int(data.get("total") or len(changes)),
        "available": bool(changes),
    }


def get_hot_concept_summaries(names: List[str], force_refresh: bool = False) -> List[Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    pending: List[str] = []
    now = time.monotonic()
    with _cache_lock:
        for name in names:
            cached = _hot_concept_summary_cache.get(name)
            if cached and now - cached["created_at"] < DYNAMIC_CACHE_TTL_SECONDS and not force_refresh:
                results[name] = cached["payload"]
            else:
                pending.append(name)
    if pending:
        with ThreadPoolExecutor(max_workers=min(4, len(pending))) as executor:
            futures = {executor.submit(fetch_hot_concept_summary, name): name for name in pending}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                except MarketDataError as exc:
                    results[name] = {"concept": name, "available": False, "error": str(exc), "averageChangePct": None}
        with _cache_lock:
            for name in pending:
                _hot_concept_summary_cache[name] = {"created_at": time.monotonic(), "payload": results[name]}
    return [results[name] for name in names]


def fetch_dynamic_market_data(
    market_key: str, min_amount: float, min_change: float, min_turnover: float, limit: int,
    include_all: bool = False,
) -> Dict[str, Any]:
    market = MARKET_SCOPES[market_key]
    rows, total = fetch_market_rows(market_key)
    updated_at = datetime.now(CHINA_TZ)
    quotes: List[Dict[str, Any]] = []
    market_quotes: List[Dict[str, Any]] = []
    market_cap_excluded_count = 0
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
        # 盘中选股只按流通市值过滤；总市值不再作为硬性入池条件。
        if not bse_code(quote["code"]) and (quote.get("floatMarketCap") is None or quote["floatMarketCap"] <= 3_000_000_000):
            market_cap_excluded_count += 1
            continue
        if (
            quote["amount"] < min_amount
            or quote["changePct"] < min_change
            or quote["screener"]["turnover"] < min_turnover
        ):
            continue
        quotes.append(quote)

    # 以综合评分优先，避免涨停股因涨幅排序占满候选池，导致评分达标但未涨停的股票进不来。
    quotes.sort(
        key=lambda item: (item["screener"]["snapshotScore"], item["changePct"]),
        reverse=True,
    )
    # 首屏仍限制历史筛选规模，但扩大到默认展示量附近，兼顾响应速度和候选覆盖面。
    screening_pool_size = len(quotes) if include_all else min(
        len(quotes), min(120, limit + 20)
    )
    screened_quotes, downtrend_excluded_count = filter_steady_decline_quotes(
        quotes[:screening_pool_size]
    )
    # 盘中选股不再要求过去一年有涨停记录；历史涨停统计仍保留在涨停复盘页面。
    limit_up_excluded_count = 0
    final_quotes = screened_quotes if include_all else screened_quotes[:limit]
    sector_strength = aggregate_sector_strength(market_quotes)
    market_changes = [quote["changePct"] for quote in market_quotes if quote.get("changePct") is not None]
    average_market_change = sum(market_changes) / len(market_changes) if market_changes else None
    market_regime = "超跌" if average_market_change is not None and average_market_change <= -1.5 else "常态"
    if not include_all:
        add_order_flow_factors(final_quotes, sector_strength)
    add_probability_models(final_quotes, sector_strength, updated_at.strftime("%Y-%m-%d"))
    calibration = {"samples": 0, "updatedAt": datetime.now(CHINA_TZ).isoformat()}
    # 回测写入在后台任务中进行，不阻塞盘中实时行情首屏。
    apply_follow_up_calibration(final_quotes, calibration)
    # 人气与关联概念互不依赖，合并并发请求，全部完成后再一次性返回页面。
    with ThreadPoolExecutor(max_workers=2) as executor:
        popularity_future = executor.submit(add_ths_popularity, final_quotes, updated_at.strftime("%Y-%m-%d"))
        related_future = executor.submit(add_related_sectors, final_quotes)
        popularity_future.result()
        related_future.result()
    return {
        "source": f"东方财富{market['label']}行情快照",
        "sector": {
            "key": market_key,
            "label": f"{market['label']}动态选股",
            "shortLabel": market["label"],
            "description": market["description"],
            "count": len(final_quotes),
        },
        "marketDate": updated_at.strftime("%Y-%m-%d"),
        "updatedAt": updated_at.isoformat(),
        "fetchedAt": datetime.now(CHINA_TZ).isoformat(),
        "scannedCount": total,
        "matchedCount": len(quotes),
        "marketCapExcludedCount": market_cap_excluded_count,
        "downtrendExcludedCount": downtrend_excluded_count,
        "limitUpExcludedCount": limit_up_excluded_count,
        "sectorStrength": sector_strength,
        "marketRegime": {
            "label": market_regime,
            "averageChangePct": round(average_market_change, 2) if average_market_change is not None else None,
            "minScore": 60 if market_regime == "超跌" else 70,
            "rule": "大盘平均涨跌幅≤-1.5%视为超跌，否则最低综合分70。",
        },
        "followUpBacktest": {
            "samples": calibration["samples"],
            "target": "未来 10 个交易日最高价较信号日收盘上涨至少 5%",
            "updatedAt": calibration["updatedAt"],
        },
        "unavailableCodes": [],
        "quotes": final_quotes,
    }


def get_dynamic_market_data(
    market_key: str,
    min_amount: float,
    min_change: float,
    min_turnover: float,
    limit: int,
    include_all: bool = False,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    cache_key = f"{market_key}:{min_amount:.0f}:{min_change:.2f}:{min_turnover:.2f}:{limit}:{include_all}"
    now = time.monotonic()
    with _cache_lock:
        cached = _dynamic_cache.get(cache_key)
        if cached and now - cached["created_at"] < DYNAMIC_CACHE_TTL_SECONDS and not force_refresh:
            return cached["payload"]

    payload = fetch_dynamic_market_data(market_key, min_amount, min_change, min_turnover, limit, include_all)
    with _cache_lock:
        _dynamic_cache[cache_key] = {"created_at": time.monotonic(), "payload": payload}
    return payload


MARKET_INDEX_CODES = {
    "sh-main": ("上证指数", "s_sh000001"),
    "sz-main": ("深证成指", "s_sz399001"),
    "chinext": ("创业板指", "s_sz399006"),
    "star": ("科创50", "s_sh000688"),
    "bse": ("北证50", "s_bj899050"),
}


def _parse_index_payload(text: str, source: str) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for line in text.splitlines():
        if '="' not in line:
            continue
        symbol, raw = line.split('="', 1)
        fields = raw.rstrip('";').split("~" if source == "tencent" else ",")
        if source == "tencent":
            code = symbol[2:] if symbol.startswith("v_") else ""
            key = next((k for k, item in MARKET_INDEX_CODES.items() if item[1] == code), None)
            values = (fields[1], fields[3], fields[4], fields[5]) if len(fields) > 5 else None
        else:
            code = symbol.replace("var hq_str_", "")
            key = next((k for k, item in MARKET_INDEX_CODES.items() if item[1] == code), None)
            values = (fields[0], fields[1], fields[2], fields[3]) if len(fields) > 3 else None
        if key and values:
            try:
                result[key] = {"name": values[0], "price": float(values[1] or 0), "change": float(values[2] or 0), "changePct": float(values[3] or 0)}
            except (TypeError, ValueError):
                continue
    return result


def fetch_market_indices() -> Dict[str, Dict[str, Any]]:
    symbols = ",".join(item[1] for item in MARKET_INDEX_CODES.values())
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"}
    parsed: Dict[str, Dict[str, Any]] = {}
    try:
        request = urllib.request.Request(f"https://qt.gtimg.cn/q={symbols}", headers=headers)
        with urllib.request.urlopen(request, timeout=5) as response:
            parsed = _parse_index_payload(response.read().decode("gbk", errors="ignore"), "tencent")
        if len(parsed) == len(MARKET_INDEX_CODES):
            return parsed
    except Exception:
        pass
    try:
        request = urllib.request.Request(f"https://hq.sinajs.cn/list={symbols}", headers=headers)
        with urllib.request.urlopen(request, timeout=5) as response:
            fallback = _parse_index_payload(response.read().decode("gbk", errors="ignore"), "sina")
            parsed.update(fallback)
            return parsed
    except Exception:
        return {}


def fetch_major_sector_money_flow() -> List[Dict[str, Any]]:
    """Return the most active industry-board main-force flows from Eastmoney.

    Industry boards are used here rather than a hand-picked concept list so the
    comparison remains complete and does not depend on a concept-name mapping.
    """
    now = time.monotonic()
    with _cache_lock:
        cached = _sector_money_flow_cache.get("industry")
        if cached and now - cached["created_at"] < DYNAMIC_CACHE_TTL_SECONDS:
            return cached["payload"]
    try:
        query = {
            "pn": 1, "pz": 100, "np": 1, "fltt": 2, "invt": 2,
            "fid": "f62", "fs": "m:90+t:2",
            "fields": "f12,f14,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87",
        }
        # The provider caps a response at roughly 100 rows. Query both sort
        # directions so large outflows are not hidden behind all net inflows.
        inflow_payload = request_json(EASTMONEY_LIST_URL, {**query, "po": 1})
        outflow_payload = request_json(EASTMONEY_LIST_URL, {**query, "po": 0})

        def parse_flows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
            parsed = []
            for row in list(((payload.get("data") or {}).get("diff") or [])):
                value = number(row.get("f62"))
                name = str(row.get("f14") or "").strip()
                if value is None or not name:
                    continue
                parsed.append({
                    "code": str(row.get("f12") or ""),
                    "name": name,
                    "netInflow": float(value),
                    "netInflowRatio": number(row.get("f184")),
                })
            return parsed

        inflow_rows = parse_flows(inflow_payload)
        outflow_rows = parse_flows(outflow_payload)
        positives = sorted((item for item in inflow_rows if item["netInflow"] > 0), key=lambda item: item["netInflow"], reverse=True)[:10]
        negatives = sorted((item for item in outflow_rows if item["netInflow"] < 0), key=lambda item: item["netInflow"])[:10]
        result = positives + negatives
        # A near-universal rally may genuinely have no industry-level outflow.
        # In that case show the weakest boards rather than silently omitting it.
        if not negatives:
            weakest = sorted(inflow_rows, key=lambda item: item["netInflow"])[:5]
            seen = {item["code"] for item in result}
            result.extend(item for item in weakest if item["code"] not in seen)
    except Exception:
        result = []
    with _cache_lock:
        _sector_money_flow_cache["industry"] = {"created_at": time.monotonic(), "payload": result}
    return result


def market_limit_ratio(code: str, name: str) -> float:
    if str(code).startswith(("300", "301", "688", "689")):
        return 0.20
    if bse_code(str(code)):
        return 0.30
    if "ST" in str(name or "").upper():
        return 0.05
    return 0.10


def build_market_distribution(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build close-to-close distribution plus seal/fried-board statistics."""
    bins = [
        {"key": "limitDown", "label": "跌停", "count": 0, "side": "down"},
        {"key": "down7", "label": ">7", "count": 0, "side": "down"},
        {"key": "down5", "label": "5–7", "count": 0, "side": "down"},
        {"key": "down3", "label": "3–5", "count": 0, "side": "down"},
        {"key": "down0", "label": "0–3", "count": 0, "side": "down"},
        {"key": "flat", "label": "0", "count": 0, "side": "flat"},
        {"key": "up0", "label": "0–3", "count": 0, "side": "up"},
        {"key": "up3", "label": "3–5", "count": 0, "side": "up"},
        {"key": "up5", "label": "5–7", "count": 0, "side": "up"},
        {"key": "up7", "label": ">7", "count": 0, "side": "up"},
        {"key": "limitUp", "label": "涨停", "count": 0, "side": "up"},
    ]
    lookup = {item["key"]: item for item in bins}
    rising = falling = flat = suspended = limit_up = limit_down = one_word = fried = 0
    for row in rows:
        change = number(row.get("f3"))
        price = number(row.get("f2"))
        if change is None or price is None or price <= 0:
            continue
        code, name = str(row.get("f12") or ""), str(row.get("f14") or "")
        limit_pct = market_limit_ratio(code, name) * 100
        high, low, opening, previous = (number(row.get(field)) for field in ("f15", "f16", "f17", "f18"))
        eligible = previous is not None and previous > 0 and not name.upper().startswith(("N", "C"))
        upper = float((Decimal(str(previous or 0)) * (Decimal(1) + Decimal(str(limit_pct / 100)))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        lower = float((Decimal(str(previous or 0)) * (Decimal(1) - Decimal(str(limit_pct / 100)))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        is_up_limit = eligible and abs(price - upper) < 0.005
        is_down_limit = eligible and abs(price - lower) < 0.005
        if is_up_limit:
            limit_up += 1
            lookup["limitUp"]["count"] += 1
            if all(value is not None and abs(value - price) <= max(0.01, price * 0.0005) for value in (high, low, opening)):
                one_word += 1
        elif is_down_limit:
            limit_down += 1
            lookup["limitDown"]["count"] += 1
        elif change > 0:
            rising += 1
            lookup["up0" if change < 3 else "up3" if change < 5 else "up5" if change < 7 else "up7"]["count"] += 1
        elif change < 0:
            falling += 1
            lookup["down0" if change > -3 else "down3" if change > -5 else "down5" if change > -7 else "down7"]["count"] += 1
        else:
            flat += 1
            lookup["flat"]["count"] += 1
        if eligible and not is_up_limit and high is not None:
            if abs(high - upper) < 0.005:
                fried += 1
    return {
        "bins": bins, "rising": rising + limit_up, "falling": falling + limit_down,
        "flat": flat, "suspended": None, "limitUp": limit_up,
        "limitDown": limit_down, "oneWordLimitUp": one_word, "friedBoard": fried,
    }


def fetch_index_analysis() -> Dict[str, Any]:
    rows, _ = fetch_market_rows("all")
    distribution = build_market_distribution(rows)
    changes = []
    amounts = 0.0
    limit_up = limit_down = 0
    for row in rows:
        change = number(row.get("f3"))
        amount = number(row.get("f6"))
        if change is None:
            continue
        changes.append(float(change))
        amounts += float(amount or 0)
        if change >= 9.5:
            limit_up += 1
        if change <= -9.5:
            limit_down += 1
    rising = sum(value > 0 for value in changes)
    falling = sum(value < 0 for value in changes)
    flat = len(changes) - rising - falling
    breadth = rising / len(changes) * 100 if changes else None
    average = statistics.fmean(changes) if changes else None
    median = statistics.median(changes) if changes else None
    if breadth is not None and breadth >= 65 and (average or 0) > 0:
        strategy = "市场广度偏强：优先关注强势板块中的回踩承接，避免盲目追逐高开个股。"
        mood = "普涨偏强"
    elif breadth is not None and breadth <= 35 and (average or 0) < 0:
        strategy = "市场广度偏弱：控制仓位，优先观察防守板块和超跌修复，不宜追涨。"
        mood = "普跌偏弱"
    else:
        strategy = "指数与市场广度存在分化：降低追涨仓位，等待板块联动和成交额同步确认。"
        mood = "结构分化"
    return {
        "indices": fetch_market_indices(),
        "total": len(changes), "rising": rising, "falling": falling, "flat": flat,
        "limitUp": distribution["limitUp"], "limitDown": distribution["limitDown"], "breadth": breadth,
        "distribution": distribution,
        "averageChange": average, "medianChange": median, "amount": amounts,
        "mood": mood, "strategy": strategy,
        "sectorMoneyFlow": fetch_major_sector_money_flow(),
        "updatedAt": datetime.now(CHINA_TZ).isoformat(),
    }


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


def fetch_concept_rank_data() -> Dict[str, Any]:
    """Fetch Eastmoney's live concept-board ranking."""
    params = {
        "pn": 1, "pz": 500, "po": 1, "np": 1, "fltt": 2, "invt": 2,
        "fid": "f3", "fs": "m:90+t:3",
        "fields": "f2,f3,f12,f14,f62,f104,f105,f106,f107,f108,f128,f140,f141",
    }
    errors = []
    payload = None
    for source_url in (EASTMONEY_LIST_URL, EASTMONEY_LIST_FALLBACK_URL):
        try:
            candidate = request_json(source_url, params)
            if isinstance(candidate.get("data"), dict) and isinstance(candidate["data"].get("diff"), list):
                payload = candidate
                break
            errors.append(f"{source_url}: 返回结构无效")
        except MarketDataError as exc:
            errors.append(str(exc))
    if payload is None:
        raise MarketDataError("概念排行数据源暂不可用；已尝试东方财富主接口和备用接口。")
    data = payload.get("data") or {}
    rows = []
    for row in data.get("diff") or []:
        rows.append({
            "code": str(row.get("f12") or ""),
            "name": str(row.get("f14") or ""),
            "index": number(row.get("f2")),
            "changePct": number(row.get("f3")),
            "mainNetInflow": number(row.get("f62")),
            "risingCount": int(number(row.get("f104")) or 0),
            "fallingCount": int(number(row.get("f105")) or 0),
            "flatCount": int(number(row.get("f106")) or 0),
            "limitUpCount": int(number(row.get("f107")) or 0),
            "limitDownCount": int(number(row.get("f108")) or 0),
            "leader": str(row.get("f140") or row.get("f128") or "--"),
            "leaderChangePct": number(row.get("f141")),
        })
    leader_codes = {item["leader"] for item in rows if re.fullmatch(r"\d{6}", item["leader"] or "")}
    leader_names: Dict[str, str] = {}
    if leader_codes:
        symbols = ",".join(tencent_quote_symbol(code) for code in sorted(leader_codes))
        try:
            result = subprocess.run(
                ["curl", "-fsSL", "--max-time", str(REQUEST_TIMEOUT_SECONDS), f"{TENCENT_BATCH_QUOTE_URL}{symbols}"],
                check=True, capture_output=True, timeout=REQUEST_TIMEOUT_SECONDS + 2,
            )
            for line in result.stdout.decode("gb18030", errors="replace").splitlines():
                if '="' not in line:
                    continue
                fields = line.split('="', 1)[1].rstrip('";').split("~")
                if len(fields) > 2 and re.fullmatch(r"\d{6}", fields[2]):
                    previous = number(fields[4]) if len(fields) > 4 else None
                    current = number(fields[3]) if len(fields) > 3 else None
                    leader_names[fields[2]] = {
                        "name": fields[1],
                        "changePct": round((current - previous) / previous * 100, 2) if current is not None and previous not in (None, 0) else None,
                    }
        except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
            pass
    for item in rows:
        code = item["leader"]
        if code in leader_names:
            item["leaderName"] = leader_names[code]["name"]
            if leader_names[code]["changePct"] is not None:
                item["leaderChangePct"] = leader_names[code]["changePct"]
    rows.sort(key=lambda item: item.get("changePct") if item.get("changePct") is not None else -999, reverse=True)
    for index, item in enumerate(rows, 1):
        item["rank"] = index
    return {"updatedAt": datetime.now(CHINA_TZ).isoformat(), "concepts": rows, "count": len(rows)}


def get_concept_scoring_context() -> Dict[str, Dict[str, Any]]:
    """Map members of the hottest live concepts back to stocks; refresh at most every 3 minutes."""
    now = time.monotonic()
    with _cache_lock:
        cached = _concept_scoring_context_cache.get("all")
        if cached and now - cached["created_at"] < 180:
            return cached["payload"]
    ranking = fetch_concept_rank_data().get("concepts") or []
    eligible = [item for item in ranking if item.get("code") and item.get("changePct") is not None][:24]
    result: Dict[str, Dict[str, Any]] = {}

    def fetch_members(board: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        payload = request_json(EASTMONEY_LIST_URL, {
            "pn": 1, "pz": 500, "po": 1, "np": 1, "fltt": 2, "invt": 2,
            "fid": "f3", "fs": f"b:{board['code']}", "fields": "f3,f12",
        })
        return board, list(((payload.get("data") or {}).get("diff") or []))

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(fetch_members, board) for board in eligible]
        for future in as_completed(futures):
            try:
                board, members = future.result()
            except MarketDataError:
                continue
            changes = [float(number(row.get("f3")) or 0) for row in members]
            strong_count = sum(value >= 2 for value in changes)
            total = max(1, int(board.get("risingCount") or 0) + int(board.get("fallingCount") or 0) + int(board.get("flatCount") or 0))
            breadth = int(board.get("risingCount") or 0) / total * 100
            change = float(board.get("changePct") or 0)
            net_flow = float(board.get("mainNetInflow") or 0)
            heat_score = clamp(50 + change * 10 + (breadth - 50) * .4 + min(strong_count, 10) * 1.5 + (6 if net_flow > 0 else -6))
            peer_score = clamp(25 + breadth * .55 + min(strong_count, 10) * 3)
            context = {
                "type": "concept", "name": board.get("name"), "rank": board.get("rank"),
                "heatScore": heat_score, "peerScore": peer_score, "averageChange": change,
                "breadth": breadth, "strongCount": strong_count, "memberCount": len(members),
                "netFlow": net_flow, "confirmed": bool(change > 0 and breadth >= 55 and strong_count >= 3 and net_flow >= 0),
            }
            for row in members:
                code = str(row.get("f12") or "")
                key = f"code:{code}"
                previous = result.get(key)
                if code and (previous is None or int(context["rank"] or 999) < int(previous.get("rank") or 999)):
                    result[key] = context
    with _cache_lock:
        _concept_scoring_context_cache["all"] = {"created_at": time.monotonic(), "payload": result}
    return result


def refresh_concept_scoring_context() -> None:
    """Refresh heavy concept membership data without blocking quote endpoints."""
    try:
        get_concept_scoring_context()
    except (MarketDataError, OSError, ValueError):
        pass
    finally:
        with _cache_lock:
            _concept_scoring_refresh_state["running"] = False


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


def fetch_limit_up_history(code: str, datalen: Optional[int] = None) -> Dict[str, Any]:
    payload = request_json(
        SINA_DAILY_KLINE_URL,
        {
            "symbol": stock_symbol(code),
            "scale": 240,
            "ma": "no",
            "datalen": datalen or LIMIT_UP_HISTORY_DAYS,
        },
    )
    rows: List[Dict[str, Any]] = []
    for row in payload if isinstance(payload, list) else []:
        if not isinstance(row, dict):
            continue
        close = number(row.get("close"))
        high = number(row.get("high"))
        if close is not None and high is not None:
            rows.append({
                "date": str(row.get("day") or ""),
                "open": number(row.get("open")),
                "close": close,
                "high": high,
                "low": number(row.get("low")),
            })
    if len(rows) < 2:
        raise MarketDataError(f"{code} 一年日线数据不足")

    try:
        latest_date = datetime.strptime(rows[-1]["date"], "%Y-%m-%d")
        cutoff = latest_date - timedelta(days=365)
    except ValueError:
        cutoff = datetime.now(CHINA_TZ).replace(tzinfo=None) - timedelta(days=365)

    sealed = 0
    touched = 0
    recent_limit_date: Optional[str] = None
    recent_limit_index: Optional[int] = None
    for index in range(1, len(rows)):
        current = rows[index]
        try:
            trade_date = datetime.strptime(current["date"], "%Y-%m-%d")
        except ValueError:
            continue
        if trade_date < cutoff:
            continue
        previous_close = rows[index - 1]["close"]
        if previous_close in (None, 0):
            continue
        limit_price = normal_limit_price(previous_close)
        if current["high"] >= limit_price - 0.001:
            touched += 1
        if current["close"] >= limit_price - 0.001:
            sealed += 1
            recent_limit_date = current["date"]
            recent_limit_index = index
    current_streak = summarize_current_limit_streak(rows)
    return {
        "available": True,
        "sealedCount": sealed,
        "touchedCount": touched,
        "brokenCount": max(0, touched - sealed),
        "recentLimitDate": recent_limit_date,
        "sessionsSinceRecentLimit": (
            len(rows) - 1 - recent_limit_index if recent_limit_index is not None else None
        ),
        "currentStreak": current_streak,
        "bars": rows,
    }


def get_limit_up_history(code: str) -> Dict[str, Any]:
    now = time.monotonic()
    with _cache_lock:
        cached = _limit_up_history_cache.get(code)
        if cached and now - cached["created_at"] < LIMIT_UP_HISTORY_CACHE_TTL_SECONDS:
            return cached["payload"]
    stored_rows: List[Dict[str, Any]] = []
    try:
        connection = open_backtest_db()
        cache_row = connection.execute(
            "SELECT bars_json FROM midterm_history_cache WHERE code=?", (code,)
        ).fetchone()
        connection.close()
        if cache_row:
            stored_rows = json.loads(cache_row[0]) or []
    except (OSError, sqlite3.Error, TypeError, json.JSONDecodeError):
        stored_rows = []

    # 首次查询取完整750日；已有缓存只取最近45日，再按日期合并。
    payload = fetch_limit_up_history(code, 45 if stored_rows else LIMIT_UP_HISTORY_DAYS)
    if stored_rows:
        merged = {str(row.get("date")): row for row in stored_rows if row.get("date")}
        merged.update({str(row.get("date")): row for row in payload.get("bars", []) if row.get("date")})
        payload["bars"] = [merged[key] for key in sorted(merged)]
        payload = summarize_limit_up_history(payload["bars"])
    try:
        connection = open_backtest_db()
        connection.execute(
            "INSERT OR REPLACE INTO midterm_history_cache(code,latest_date,bars_json,fetched_at) VALUES(?,?,?,?)",
            (code, payload["bars"][-1]["date"], json.dumps(payload["bars"], ensure_ascii=False), datetime.now(CHINA_TZ).isoformat()),
        )
        connection.commit()
        connection.close()
    except (OSError, sqlite3.Error, IndexError, KeyError, TypeError):
        pass
    with _cache_lock:
        _limit_up_history_cache[code] = {"created_at": time.monotonic(), "payload": payload}
    return payload


def summarize_limit_up_history(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows = sorted(rows, key=lambda row: str(row.get("date") or ""))
    if len(rows) < 2:
        raise MarketDataError("历史日线数据不足")
    latest_date = datetime.strptime(rows[-1]["date"], "%Y-%m-%d")
    cutoff = latest_date - timedelta(days=365)
    sealed = touched = 0
    recent_limit_date = None
    recent_limit_index = None
    for index in range(1, len(rows)):
        current = rows[index]
        try:
            trade_date = datetime.strptime(current["date"], "%Y-%m-%d")
        except (TypeError, ValueError):
            continue
        if trade_date < cutoff:
            continue
        previous_close = rows[index - 1].get("close")
        if previous_close in (None, 0):
            continue
        limit_price = normal_limit_price(previous_close)
        if current.get("high") is not None and current["high"] >= limit_price - 0.001:
            touched += 1
        if current.get("close") is not None and current["close"] >= limit_price - 0.001:
            sealed += 1
            recent_limit_date = current["date"]
            recent_limit_index = index
    return {"available": True, "sealedCount": sealed, "touchedCount": touched,
            "brokenCount": max(0, touched - sealed), "recentLimitDate": recent_limit_date,
            "sessionsSinceRecentLimit": len(rows) - 1 - recent_limit_index if recent_limit_index is not None else None,
            "currentStreak": summarize_current_limit_streak(rows),
            "bars": rows}


def summarize_current_limit_streak(rows: List[Dict[str, Any]]) -> int:
    streak = 0
    for index in range(len(rows) - 1, 0, -1):
        previous_close = rows[index - 1].get("close")
        close = rows[index].get("close")
        if previous_close in (None, 0) or close is None or close < normal_limit_price(previous_close) - 0.001:
            break
        streak += 1
    return streak


def fetch_midterm_activity_history(code: str, signal_year: int) -> List[Dict[str, Any]]:
    params = {
        "code": f"cn_{code}",
        "start": f"{signal_year}0801",
        "end": "20500101",
        "stat": "1",
        "order": "A",
        "period": "d",
        "rt": "json",
    }
    full_url = f"{SOHU_HISTORY_URL}?{urlencode(params, safe=',')}"
    try:
        result = subprocess.run(
            ["curl", "-fsSL", "--max-time", str(REQUEST_TIMEOUT_SECONDS), "--retry", "2", "--retry-delay", "0", full_url],
            check=True,
            capture_output=True,
            timeout=REQUEST_TIMEOUT_SECONDS + 2,
        )
        payload = json.loads(result.stdout.decode("gb18030"))
    except (
        FileNotFoundError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise MarketDataError(f"搜狐历史行情请求失败: {exc}") from exc
    rows: List[Dict[str, Any]] = []
    source_rows = payload[0].get("hq") if isinstance(payload, list) and payload else []
    for fields in source_rows or []:
        if len(fields) < 10:
            continue
        close = number(fields[2])
        change_amount = number(fields[3])
        low = number(fields[5])
        high = number(fields[6])
        turnover = number(str(fields[9]).replace("%", ""))
        previous_close = close - change_amount if close is not None and change_amount is not None else None
        amplitude = (
            (high - low) / previous_close * 100
            if high is not None and low is not None and previous_close not in (None, 0)
            else None
        )
        if amplitude is None or turnover is None:
            continue
        rows.append({"date": str(fields[0]), "amplitude": amplitude, "turnover": turnover})
    if not rows:
        raise MarketDataError(f"{code} 8月以来活跃度数据不足")
    return rows


def get_midterm_activity_history(code: str, signal_year: int) -> List[Dict[str, Any]]:
    cache_key = f"{code}:{signal_year}"
    now = time.monotonic()
    with _cache_lock:
        cached = _midterm_activity_cache.get(cache_key)
        if cached and now - cached["created_at"] < MIDTERM_ACTIVITY_CACHE_TTL_SECONDS:
            return cached["payload"]
    payload = fetch_midterm_activity_history(code, signal_year)
    with _cache_lock:
        _midterm_activity_cache[cache_key] = {"created_at": time.monotonic(), "payload": payload}
    return payload


def summarize_midterm_activity(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(rows)
    amplitude_days = sum(1 for row in rows if float(row.get("amplitude") or 0) >= 4)
    required_days = math.ceil(total * .2) if total else 0
    return {
        "available": total > 0,
        "days": total,
        "amplitudePassedDays": amplitude_days,
        "turnoverPassedDays": None,
        "amplitudePassed": total > 0 and amplitude_days >= required_days,
        "turnoverPassed": True,
    }


def filter_recent_limit_up_quotes(quotes: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    """Keep only stocks with at least one confirmed close-at-limit day in the past year."""
    if not quotes:
        return [], 0
    histories: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=min(8, len(quotes))) as executor:
        futures = {
            executor.submit(get_limit_up_history, quote["code"]): quote["code"]
            for quote in quotes
            if not bse_code(quote["code"])
        }
        for future in as_completed(futures):
            code = futures[future]
            try:
                histories[code] = future.result()
            except MarketDataError:
                histories[code] = {"available": False, "sealedCount": 0}

    filtered_quotes: List[Dict[str, Any]] = []
    excluded_count = 0
    for quote in quotes:
        if bse_code(quote["code"]):
            quote["screener"]["limitUpHistory"] = {"available": False, "sealedCount": None, "notRequired": True}
            filtered_quotes.append(quote)
            continue
        history = histories[quote["code"]]
        quote["screener"]["limitUpHistory"] = history
        if not history.get("available") or int(history.get("sealedCount") or 0) < 1:
            excluded_count += 1
            continue
        filtered_quotes.append(quote)
    return filtered_quotes, excluded_count


def add_probability_models(quotes: List[Dict[str, Any]], sectors: List[Dict[str, Any]], trade_date: str) -> None:
    """Display-only heuristic probabilities; they are not calibrated forecasts or trade advice."""
    sector_by_name = {item["name"]: item for item in sectors}
    auction_records = get_auction_snapshot(trade_date, capture_if_due=True).get("records") or {}
    for quote in quotes:
        sector = sector_by_name.get(quote.get("actualIndustry"), {})
        history = quote.get("screener", {}).get("limitUpHistory") or {}
        analysis = quote.get("screener", {}).get("downtrend") or {}
        change = float(quote.get("changePct") or 0)
        turnover = float(quote.get("screener", {}).get("turnover") or 0)
        volume_ratio = float(quote.get("volumeRatio") or 0)
        amount = float(quote.get("amount") or 0)
        net_inflow = float(quote.get("netInflow") or 0)
        float_cap = max(float(quote.get("floatMarketCap") or 1), 1)
        auction = auction_records.get(quote["code"])
        auction_score = auction_strength_score(auction, quote.get("floatMarketCap"))
        order_flow = quote.get("orderFlow") or {}
        order_flow_adjustment = (
            (float(order_flow.get("score") or 50) - 50) * .08
            if order_flow.get("available") else 0
        )
        sector_score = float(sector.get("strengthScore") or 50)
        flow_ratio = net_inflow / max(amount, 1)
        attack = clamp((change - 1) / 8.8 * 100)
        today_probability = clamp(
            12 + attack * .26 + min(volume_ratio, 4) / 4 * 16
            + clamp(100 - abs(turnover - 9) * 7) * .12
            + sector_score * .14 + clamp(flow_ratio * 500) * .12
            + min(int(history.get("sealedCount") or 0) * 10, 100) * .10
            + auction_score["score"] * .10
            + order_flow_adjustment
        , 1, 95)
        averages = quote.get("screener", {}).get("movingAverages") or {}
        ma_bull = sum(
            1 for key in ("ma5", "ma10", "ma20", "ma30")
            if (averages.get(key) or {}).get("value") is not None and quote.get("price") is not None and quote["price"] >= averages[key]["value"]
        )
        pattern_score = float(analysis.get("patternScore") or 0)
        macd = analysis.get("macd") or {}
        macd_signal = str(macd.get("signal") or "")
        macd_bars = [float(value) for value in (macd.get("bars") or []) if isinstance(value, (int, float))]
        macd_score = 50.0
        if macd_signal == "金叉":
            macd_score += 22
        elif macd_signal == "多头增强":
            macd_score += 18
        elif macd_signal == "多头收敛":
            macd_score += 7
        elif macd_signal == "死叉":
            macd_score -= 22
        elif macd_signal == "空头增强":
            macd_score -= 22
        elif macd_signal == "空头收敛":
            macd_score -= 8
        if macd_bars:
            recent_bars = macd_bars[-5:]
            if len(recent_bars) >= 3 and recent_bars[-1] > recent_bars[0]:
                macd_score += 10
            elif len(recent_bars) >= 3 and recent_bars[-1] < recent_bars[0]:
                macd_score -= 10
        # 高位金叉可能只是加速末端，限制 MACD 对评分的正向贡献，避免追高。
        if change >= 8 and macd_score > 50:
            macd_score = 50 + (macd_score - 50) * .5
        macd_score = max(0.0, min(100.0, macd_score))
        macd_adjustment = round((macd_score - 50) * .30, 1)
        macd_today_adjustment = round((macd_score - 50) * .06, 1)
        today_probability = clamp(today_probability + macd_today_adjustment, 1, 95)
        risk = float(quote.get("base", {}).get("risk") or 50)
        non_macd_score = 34 + pattern_score + ma_bull * 5 + sector_score * .10 - risk * .14
        # 综合评分中 MACD 占 15%，后续上涨概率中 MACD 占 25%。
        composite_score = non_macd_score * .85 + macd_score * .15
        base_score = non_macd_score * .75 + macd_score * .25
        if analysis.get("excluded"):
            base_score -= 18
        if float(analysis.get("drawdown20") or 0) >= 15:
            base_score -= 8
        quote["probability"] = {
            "todayLimitUp": int(today_probability),
            "followUp": int(clamp(base_score, 1, 90)),
            "rawFollowUp": int(clamp(base_score, 1, 90)),
            "compositeScore": int(clamp(composite_score, 1, 100)),
            "signals": analysis.get("patternSignals") or [],
            "auctionAvailable": auction_score["available"],
            "orderFlowAvailable": bool(order_flow.get("available")),
            "orderFlowScore": order_flow.get("score") if order_flow.get("available") else None,
            "orderFlowAdjustment": round(order_flow_adjustment, 1),
            "macdSignal": macd.get("signal"),
            "macdScore": round(macd_score),
            "macdAdjustment": macd_adjustment,
            "macdTodayAdjustment": macd_today_adjustment,
            "netInflow": net_inflow,
            "note": "今日概率以经委比、位置、量能、K线、均线和板块联动确认后的内外盘作小幅修正；模型参考，非历史回测胜率或投资建议。",
        }


def open_backtest_db() -> sqlite3.Connection:
    BACKTEST_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(BACKTEST_DB_PATH)
    connection.execute("""CREATE TABLE IF NOT EXISTS follow_up_observations (
        code TEXT NOT NULL, signal_date TEXT NOT NULL, score INTEGER NOT NULL,
        outcome INTEGER NOT NULL, source TEXT NOT NULL, PRIMARY KEY (code, signal_date, source)
    )""")
    connection.execute("""CREATE TABLE IF NOT EXISTS follow_up_snapshots (
        code TEXT NOT NULL, signal_date TEXT NOT NULL, score INTEGER NOT NULL,
        settled INTEGER NOT NULL DEFAULT 0, outcome INTEGER, PRIMARY KEY (code, signal_date)
    )""")
    connection.execute("""CREATE TABLE IF NOT EXISTS score_traces (
        trade_date TEXT NOT NULL, minute TEXT NOT NULL, code TEXT NOT NULL,
        name TEXT, score REAL, predictive_score REAL, predictive_probability REAL,
            price REAL, change_pct REAL, volume_ratio REAL, net_inflow REAL, macd_signal TEXT,
        base_json TEXT, order_flow_score REAL, macd_score REAL,
        PRIMARY KEY (trade_date, minute, code)
    )""")
    connection.execute("""CREATE TABLE IF NOT EXISTS rebound_daily_scores (
        trade_date TEXT NOT NULL, code TEXT NOT NULL, name TEXT,
        daily_score INTEGER NOT NULL, factors_json TEXT,
        calculated_at TEXT NOT NULL, PRIMARY KEY (trade_date, code)
    )""")
    connection.execute("""CREATE TABLE IF NOT EXISTS rebound_intraday_scores (
        trade_date TEXT NOT NULL, minute TEXT NOT NULL, code TEXT NOT NULL, name TEXT,
        intraday_score INTEGER NOT NULL, price REAL, change_pct REAL,
        factors_json TEXT, calculated_at TEXT NOT NULL,
        PRIMARY KEY (trade_date, minute, code)
    )""")
    connection.execute("""CREATE TABLE IF NOT EXISTS alert_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_date TEXT NOT NULL, code TEXT NOT NULL, name TEXT,
        alert_kind TEXT NOT NULL, threshold INTEGER NOT NULL, alert_time TEXT NOT NULL,
        price REAL, change_pct REAL, score REAL, predictive_score REAL,
        predictive_probability REAL, volume_ratio REAL, turnover REAL, amplitude REAL,
        net_inflow REAL, factors_json TEXT, macd_score REAL,
        outcome_settled INTEGER NOT NULL DEFAULT 0,
        UNIQUE(trade_date, code, alert_kind, threshold)
    )""")
    connection.execute("""CREATE TABLE IF NOT EXISTS alert_outcomes (
        event_id INTEGER PRIMARY KEY,
        close_price REAL, close_change_pct REAL,
        high_1d_pct REAL, high_3d_pct REAL, high_5d_pct REAL, high_10d_pct REAL,
        target_hit INTEGER, max_drawdown_pct REAL, settled_at TEXT
    )""")
    for column, definition in (("base_json", "TEXT"), ("order_flow_score", "REAL"), ("macd_score", "REAL")):
        try:
            connection.execute(f"ALTER TABLE score_traces ADD COLUMN {column} {definition}")
        except sqlite3.OperationalError:
            pass
    connection.execute("""CREATE TABLE IF NOT EXISTS midterm_history_cache (
        code TEXT PRIMARY KEY, latest_date TEXT NOT NULL,
        bars_json TEXT NOT NULL, fetched_at TEXT NOT NULL
    )""")
    connection.execute("""CREATE TABLE IF NOT EXISTS stock_concept_cache (
        code TEXT PRIMARY KEY,
        concepts_json TEXT NOT NULL,
        fetched_at REAL NOT NULL,
        source TEXT NOT NULL
    )""")
    return connection


def historical_follow_up_score(bars: List[Dict[str, Any]], index: int) -> int:
    window = bars[index - BACKTEST_LOOKBACK_DAYS + 1:index + 1]
    analysis = analyze_steady_decline(window)
    latest = float(bars[index]["close"])
    averages = analysis.get("movingAverages") or {}
    ma_bull = sum(1 for value in averages.values() if value is not None and latest >= value)
    highs = [float(row.get("high") or row["close"]) for row in window]
    lows = [float(row.get("low") or row["close"]) for row in window]
    risk = clamp((max(highs) - min(lows)) / max(latest, 0.01) * 250)
    score = 34 + float(analysis.get("patternScore") or 0) + ma_bull * 5 - risk * .14
    if analysis.get("excluded"):
        score -= 18
    return int(clamp(score, 1, 90))


def ensure_midterm_table(connection: sqlite3.Connection) -> None:
    connection.execute("""CREATE TABLE IF NOT EXISTS midterm_candidates (
        code TEXT PRIMARY KEY, name TEXT NOT NULL, entry_date TEXT NOT NULL,
        signal_year INTEGER NOT NULL, signal_json TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active', last_checked TEXT NOT NULL
    )""")
    connection.execute("CREATE TABLE IF NOT EXISTS midterm_scan_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    connection.execute("""CREATE TABLE IF NOT EXISTS midterm_display_entries (
        code TEXT NOT NULL, trade_date TEXT NOT NULL,
        created_at TEXT NOT NULL, PRIMARY KEY (code, trade_date)
    )""")


def trade_date_from_market_row(row: Dict[str, Any]) -> str:
    timestamp = number(row.get("f124"))
    if timestamp is not None:
        if timestamp > 10_000_000_000:
            timestamp /= 1000
        try:
            return datetime.fromtimestamp(timestamp, CHINA_TZ).strftime("%Y-%m-%d")
        except (OverflowError, OSError, ValueError):
            pass
    return datetime.now(CHINA_TZ).strftime("%Y-%m-%d")


def moving_average_at(bars: List[Dict[str, Any]], period: int, end: int) -> Optional[float]:
    rows = bars[max(0, end - period + 1):end + 1]
    closes = [number(row.get("close")) for row in rows]
    return sum(closes) / period if len(closes) == period and all(value is not None for value in closes) else None


def evaluate_midterm_pattern(bars: List[Dict[str, Any]], now: datetime, required_ma_count: int = 3, simplified_market: bool = False) -> Optional[Dict[str, Any]]:
    signal_year = now.year if now.month >= 9 else now.year - 1
    july = [row for row in bars if str(row.get("date", "")).startswith(f"{signal_year}-07")]
    august = [row for row in bars if str(row.get("date", "")).startswith(f"{signal_year}-08")]
    prior_end = f"{signal_year}-07-01"
    prior = [row for row in bars if str(row.get("date", "")) < prior_end]
    if len(july) < 10 or len(august) < 10 or len(bars) < 40 or (not simplified_market and len(prior) < 240):
        return None
    july_low = min(float(row.get("low") or row["close"]) for row in july)
    prior_1y = prior[-250:]
    prior_2y = prior[-500:]
    low_1y = min(float(row.get("low") or row["close"]) for row in prior_1y)
    low_2y = min(float(row.get("low") or row["close"]) for row in prior_2y)
    near_1y = low_1y <= july_low <= low_1y * 1.05 if prior_1y else False
    near_2y = len(prior_2y) >= 450 and low_2y <= july_low <= low_2y * 1.05
    year_start = [row for row in bars if str(row.get("date", "")) >= f"{signal_year}-01-01" and str(row.get("date", "")) <= f"{signal_year}-06-30"]
    new_low_since_year_start = bool(year_start) and july_low <= min(float(row.get("low") or row["close"]) for row in year_start)
    august_first = float(august[0]["close"])
    august_last = float(august[-1]["close"])
    august_rebound = august_last >= july_low * 1.08 and august_last > august_first
    latest_index = len(bars) - 1
    slopes: Dict[str, bool] = {}
    averages: Dict[str, Optional[float]] = {}
    for period in (5, 10, 20, 30):
        current = moving_average_at(bars, period, latest_index)
        previous = moving_average_at(bars, period, latest_index - 5)
        averages[f"ma{period}"] = round(current, 2) if current is not None else None
        slopes[f"ma{period}"] = current is not None and previous is not None and current > previous
    rising_count = sum(slopes.values())
    latest_close = float(bars[-1]["close"])
    recovered_1y = (
        july_low < low_1y
        and latest_close >= low_1y * .98
        and slopes.get("ma20", False)
        and slopes.get("ma30", False)
    )
    required_ma_count = max(2, min(4, int(required_ma_count)))
    qualified = (
        august_rebound and new_low_since_year_start
        if simplified_market
        else august_rebound and rising_count >= required_ma_count and ((near_1y or near_2y) or recovered_1y)
    )
    if not qualified:
        return None
    broken = latest_close < july_low * .98 or ((not simplified_market) and rising_count < required_ma_count) or (
        averages["ma20"] is not None and averages["ma30"] is not None
        and latest_close < averages["ma20"] and latest_close < averages["ma30"]
    )
    low_cycle = "1年修复" if recovered_1y and not (near_1y or near_2y) else (
        "1年/2年" if near_1y and near_2y else ("2年" if near_2y else "1年")
    )
    return {
        "signalYear": signal_year, "julyLow": round(july_low, 2),
        "prior1yLow": round(low_1y, 2), "prior2yLow": round(low_2y, 2),
        "lowCycle": low_cycle,
        "recovered1y": recovered_1y, "newLowSinceYearStart": new_low_since_year_start,
        "augustReboundPct": round((august_last / july_low - 1) * 100, 2),
        "maRisingCount": rising_count, "requiredMaCount": required_ma_count,
        "maSlopes": slopes, "movingAverages": averages,
        "broken": broken,
    }


def run_midterm_scan() -> None:
    global _midterm_scan_status
    try:
        # 中期选股覆盖全部 A 股市场，排除 ST/退市标识。
        rows, _ = fetch_market_rows("all")
        rows = [
            row for row in rows
            if str(row.get("f12") or "").strip()
            and "ST" not in str(row.get("f14") or "").upper()
            and "退" not in str(row.get("f14") or "")
        ]
        today = datetime.now(CHINA_TZ)
        with _midterm_lock:
            connection = open_backtest_db()
            ensure_midterm_table(connection)
            existing_signals = {row[0]: json.loads(row[1]) for row in connection.execute("SELECT code,signal_json FROM midterm_candidates")}
            connection.close()
        _midterm_scan_status = {"status": "running", "total": len(rows), "processed": 0, "added": 0, "failed": 0, "startedAt": today.isoformat()}
        for start in range(0, len(rows), 12):
            batch = rows[start:start + 12]
            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = {executor.submit(get_limit_up_history, str(row.get("f12"))): row for row in batch}
                for future in as_completed(futures):
                    row = futures[future]
                    code, name = str(row.get("f12")), str(row.get("f14") or row.get("f12"))
                    try:
                        bars = future.result().get("bars") or []
                        required_ma_count = midterm_required_ma_count(code)
                        signal = evaluate_midterm_pattern(bars, today, required_ma_count, midterm_simplified_market(code))
                        if signal and not signal["broken"]:
                            with _midterm_lock:
                                connection = open_backtest_db()
                                ensure_midterm_table(connection)
                                connection.execute(
                                    "INSERT OR IGNORE INTO midterm_candidates(code,name,entry_date,signal_year,signal_json,status,last_checked) VALUES(?,?,?,?,?,?,?)",
                                    (code, name, today.strftime("%Y-%m-%d"), signal["signalYear"], json.dumps(signal, ensure_ascii=False), "broken" if signal["broken"] else "active", today.isoformat()),
                                )
                                connection.execute(
                                    "UPDATE midterm_candidates SET name=?,signal_json=?,status=?,last_checked=? WHERE code=?",
                                    (name, json.dumps(signal, ensure_ascii=False), "broken" if signal["broken"] else "active", today.isoformat(), code),
                                )
                                connection.commit()
                                if code not in existing_signals:
                                    _midterm_scan_status["added"] += 1
                                    existing_signals[code] = signal
                                connection.close()
                        elif code in existing_signals and bars:
                            saved = existing_signals[code]
                            latest = float(bars[-1]["close"])
                            slopes = []
                            for period in (5, 10, 20, 30):
                                current = moving_average_at(bars, period, len(bars) - 1)
                                previous = moving_average_at(bars, period, len(bars) - 6)
                                slopes.append(current is not None and previous is not None and current > previous)
                            ma20 = moving_average_at(bars, 20, len(bars) - 1)
                            ma30 = moving_average_at(bars, 30, len(bars) - 1)
                            required_ma_count = midterm_required_ma_count(code)
                            with _midterm_lock:
                                connection = open_backtest_db()
                                ensure_midterm_table(connection)
                                # 形态走坏或本次不再满足入池条件，直接移出当前池；
                                # display_entries 保留，以便未来重新满足时累计进入次数。
                                connection.execute("DELETE FROM midterm_candidates WHERE code=?", (code,))
                                connection.commit()
                                connection.close()
                    except (MarketDataError, OSError, ValueError, sqlite3.Error):
                        _midterm_scan_status["failed"] += 1
                    _midterm_scan_status["processed"] += 1
            _midterm_scan_status["updatedAt"] = datetime.now(CHINA_TZ).isoformat()
        _midterm_scan_status["status"] = "completed"
        _midterm_scan_status["completedAt"] = datetime.now(CHINA_TZ).isoformat()
        with _midterm_lock:
            connection = open_backtest_db()
            ensure_midterm_table(connection)
            connection.execute("INSERT OR REPLACE INTO midterm_scan_meta(key,value) VALUES('completedAt',?)", (_midterm_scan_status["completedAt"],))
            connection.execute("INSERT OR REPLACE INTO midterm_scan_meta(key,value) VALUES('ruleVersion',?)", ("growth-markets-july-low-aug-rebound",))
            connection.commit()
            connection.close()
    except Exception as exc:
        _midterm_scan_status.update({"status": "error", "message": str(exc)})


def get_midterm_pool(force_scan: bool = False) -> Dict[str, Any]:
    global _midterm_scan_status
    today = datetime.now(CHINA_TZ).strftime("%Y-%m-%d")
    with _midterm_lock:
        meta_connection = open_backtest_db()
        ensure_midterm_table(meta_connection)
        meta_row = meta_connection.execute("SELECT value FROM midterm_scan_meta WHERE key='completedAt'").fetchone()
        rule_row = meta_connection.execute("SELECT value FROM midterm_scan_meta WHERE key='ruleVersion'").fetchone()
        meta_connection.close()
    completed_at = str(meta_row[0]) if meta_row else str(_midterm_scan_status.get("completedAt", ""))
    should_start = _midterm_scan_status.get("status") not in ("running",) and (force_scan or completed_at[:10] != today or not rule_row or rule_row[0] != "growth-markets-july-low-aug-rebound")
    if completed_at[:10] == today and _midterm_scan_status.get("status") == "idle":
        _midterm_scan_status = {"status": "completed", "total": 0, "processed": 0, "added": 0, "failed": 0, "completedAt": completed_at}
    if should_start:
        _midterm_scan_status = {"status": "running", "total": 0, "processed": 0, "added": 0, "failed": 0, "startedAt": datetime.now(CHINA_TZ).isoformat()}
        threading.Thread(target=run_midterm_scan, name="midterm-scan", daemon=True).start()
    with _midterm_lock:
        connection = open_backtest_db()
        ensure_midterm_table(connection)
        records = connection.execute("SELECT code,name,entry_date,signal_json,status,last_checked FROM midterm_candidates ORDER BY entry_date DESC, code").fetchall()
        connection.close()
    candidates = [
        {"code": row[0], "name": row[1], "entryDate": row[2], "signal": json.loads(row[3]), "status": row[4], "lastChecked": row[5]}
        for row in records
    ]
    now_monotonic = time.monotonic()
    with _cache_lock:
        quote_rows = dict(_midterm_quote_cache["rows"]) if now_monotonic - float(_midterm_quote_cache["created_at"]) < 30 else {}
    if not quote_rows:
        try:
            rows_all, _ = fetch_market_rows("all")
            quote_rows = {str(row.get("f12") or ""): row for row in rows_all}
            with _cache_lock:
                _midterm_quote_cache.update({"created_at": time.monotonic(), "rows": quote_rows})
        except MarketDataError:
            quote_rows = {}
    prepared_candidates: List[Dict[str, Any]] = []
    activity_inputs: Dict[str, int] = {}
    for candidate in candidates:
        row = quote_rows.get(candidate["code"], {})
        amplitude = number(row.get("f7"))
        amount = number(row.get("f6"))
        turnover = number(row.get("f8"))
        price = number(row.get("f2"))
        signal = candidate.get("signal", {})
        july_low = number(signal.get("julyLow"))
        low_to_latest_gain = (
            (price / july_low - 1) * 100
            if price is not None and july_low not in (None, 0)
            else None
        )
        base_available = all(value is not None for value in (amount, price, july_low))
        amount_passed = amount is not None and amount >= 500_000_000
        low_to_latest_min = 20 if str(candidate["code"]).startswith(("300", "301", "688", "689")) else 10
        low_to_latest_passed = low_to_latest_gain is not None and low_to_latest_min < low_to_latest_gain < 50
        simplified_market = midterm_simplified_market(candidate["code"])
        required_ma_count = int(signal.get("requiredMaCount") or midterm_required_ma_count(candidate["code"]))
        ma_all_passed = simplified_market or int(signal.get("maRisingCount") or 0) >= required_ma_count
        if base_available and amount_passed and low_to_latest_passed and ma_all_passed:
            activity_inputs[candidate["code"]] = int(signal.get("signalYear") or datetime.now(CHINA_TZ).year)
        prepared_candidates.append({
            "candidate": candidate,
            "amplitude": amplitude,
            "amount": amount,
            "turnover": turnover,
            "price": price,
            "baseAvailable": base_available,
            "amountPassed": amount_passed,
            "lowToLatestGain": low_to_latest_gain,
            "lowToLatestPassed": low_to_latest_passed,
            "maAllPassed": ma_all_passed,
        })

    activity_summaries: Dict[str, Dict[str, Any]] = {}
    if activity_inputs:
        for code, signal_year in activity_inputs.items():
            try:
                activity_summaries[code] = summarize_midterm_activity(get_midterm_activity_history(code, signal_year))
            except (MarketDataError, OSError, ValueError):
                time.sleep(.2)
                try:
                    activity_summaries[code] = summarize_midterm_activity(get_midterm_activity_history(code, signal_year))
                except (MarketDataError, OSError, ValueError):
                    activity_summaries[code] = {"available": False}
            time.sleep(.04)

    eligible_count = 0
    eligible_trade_dates: List[Tuple[str, str]] = []
    for prepared in prepared_candidates:
        candidate = prepared["candidate"]
        row = quote_rows.get(candidate["code"], {})
        activity = activity_summaries.get(candidate["code"], {"available": False})
        available = bool(prepared["baseAvailable"] and activity.get("available"))
        eligible = bool(
            prepared["baseAvailable"]
            and prepared["amountPassed"]
            and prepared["lowToLatestPassed"]
            and prepared["maAllPassed"]
            and activity.get("amplitudePassed")
        )
        if eligible:
            eligible_count += 1
            eligible_trade_dates.append((candidate["code"], trade_date_from_market_row(row)))
        candidate["quote"] = {
            "price": prepared["price"], "changePct": number(row.get("f3")),
            "amplitude": prepared["amplitude"], "amount": prepared["amount"], "turnover": prepared["turnover"],
            "netInflow": number(row.get("f62")),
        }
        candidate["tStrategy"] = {
            "available": available, "eligible": eligible,
            "amplitudePassed": bool(activity.get("amplitudePassed")),
            "amountPassed": prepared["amountPassed"],
            "turnoverPassed": bool(activity.get("turnoverPassed")),
            "maAllPassed": prepared["maAllPassed"],
            "activityDays": activity.get("days"),
            "amplitudePassedDays": activity.get("amplitudePassedDays"),
            "turnoverPassedDays": activity.get("turnoverPassedDays"),
            "lowToLatestGainPct": round(prepared["lowToLatestGain"], 2) if prepared["lowToLatestGain"] is not None else None,
            "lowToLatestGainPassed": prepared["lowToLatestPassed"],
        }
    with _midterm_lock:
        connection = open_backtest_db()
        ensure_midterm_table(connection)
        if eligible_trade_dates:
            now_iso = datetime.now(CHINA_TZ).isoformat()
            connection.executemany(
                "INSERT OR IGNORE INTO midterm_display_entries(code,trade_date,created_at) VALUES(?,?,?)",
                [(code, trade_date, now_iso) for code, trade_date in eligible_trade_dates],
            )
            connection.commit()
        entry_counts = {
            str(row[0]): int(row[1])
            for row in connection.execute("SELECT code,COUNT(*) FROM midterm_display_entries GROUP BY code")
        }
        connection.close()
    for candidate in candidates:
        candidate["entryCount"] = entry_counts.get(candidate["code"], 0)
    return {
        "source": "主板历史日线中期形态池", "updatedAt": datetime.now(CHINA_TZ).isoformat(),
        "scan": dict(_midterm_scan_status),
        "poolCount": len(candidates), "tEligibleCount": eligible_count,
        "candidates": candidates,
    }


def build_follow_up_calibration(quotes: List[Dict[str, Any]], trade_date: str) -> Dict[str, Any]:
    """Backtest the K-line-only component against a +5% / next-10-session target."""
    with _backtest_lock:
        connection = open_backtest_db()
        try:
            bars_by_code: Dict[str, List[Dict[str, Any]]] = {}
            for quote in quotes:
                try:
                    bars = get_limit_up_history(quote["code"]).get("bars") or []
                except MarketDataError:
                    continue
                bars_by_code[quote["code"]] = bars
                for index in range(BACKTEST_LOOKBACK_DAYS - 1, len(bars) - BACKTEST_HORIZON_DAYS):
                    if bars[index].get("close") in (None, 0):
                        continue
                    future_high = max(float(row.get("high") or row.get("close") or 0) for row in bars[index + 1:index + 1 + BACKTEST_HORIZON_DAYS])
                    outcome = int(future_high >= float(bars[index]["close"]) * (1 + BACKTEST_TARGET_RETURN))
                    connection.execute(
                        "INSERT OR REPLACE INTO follow_up_observations (code, signal_date, score, outcome, source) VALUES (?, ?, ?, ?, 'historical')",
                        (quote["code"], bars[index].get("date"), historical_follow_up_score(bars, index), outcome),
                    )
            pending = connection.execute(
                "SELECT code, signal_date, score FROM follow_up_snapshots WHERE settled = 0"
            ).fetchall()
            for code, signal_date, score in pending:
                bars = bars_by_code.get(code)
                if bars is None:
                    try:
                        bars = get_limit_up_history(code).get("bars") or []
                    except MarketDataError:
                        continue
                signal_index = next((i for i, row in enumerate(bars) if row.get("date") == signal_date), None)
                if signal_index is None:
                    continue
                future = bars[signal_index + 1:signal_index + 1 + BACKTEST_HORIZON_DAYS]
                if len(future) < BACKTEST_HORIZON_DAYS or not bars[signal_index].get("close"):
                    continue
                future_high = max(float(row.get("high") or row.get("close") or 0) for row in future)
                outcome = int(future_high >= float(bars[signal_index]["close"]) * (1 + BACKTEST_TARGET_RETURN))
                connection.execute(
                    "UPDATE follow_up_snapshots SET settled = 1, outcome = ? WHERE code = ? AND signal_date = ?",
                    (outcome, code, signal_date),
                )
                connection.execute(
                    "INSERT OR REPLACE INTO follow_up_observations (code, signal_date, score, outcome, source) VALUES (?, ?, ?, ?, 'live-snapshot')",
                    (code, signal_date, score, outcome),
                )
            now = datetime.now(CHINA_TZ)
            if now.weekday() < 5 and now.strftime("%Y-%m-%d") == trade_date and (now.hour, now.minute) >= (15, 0):
                for quote in quotes:
                    probability = quote.get("probability") or {}
                    connection.execute(
                        "INSERT OR IGNORE INTO follow_up_snapshots (code, signal_date, score) VALUES (?, ?, ?)",
                        (quote["code"], trade_date, int(probability.get("rawFollowUp") or 0)),
                    )
            connection.commit()
            rows = connection.execute("SELECT score, outcome FROM follow_up_observations").fetchall()
        finally:
            connection.close()
    buckets: Dict[int, List[int]] = {}
    for score, outcome in rows:
        buckets.setdefault(int(score) // 10 * 10, []).append(int(outcome))
    rates = {bucket: round((sum(values) + 2) / (len(values) + 4) * 100) for bucket, values in buckets.items()}
    return {"samples": len(rows), "rates": rates, "updatedAt": datetime.now(CHINA_TZ).isoformat()}


def apply_follow_up_calibration(quotes: List[Dict[str, Any]], calibration: Dict[str, Any]) -> None:
    rates = calibration.get("rates") or {}
    sample_count = int(calibration.get("samples") or 0)
    for quote in quotes:
        probability = quote.get("probability") or {}
        raw = int(probability.get("rawFollowUp") or 0)
        bucket = raw // 10 * 10
        empirical = rates.get(bucket)
        if empirical is not None and sample_count >= 30:
            probability["followUp"] = int(round(raw * .35 + empirical * .65))
        probability["backtestSamples"] = sample_count
        probability["target"] = "未来 10 个交易日最高价较信号日收盘上涨至少 5%"


def run_all_market_backtest() -> None:
    """Build the K-line calibration set for the whole market in resumable batches."""
    global _all_market_backtest_status
    try:
        rows, _ = fetch_market_rows("all")
        codes = sorted({str(row.get("f12") or "") for row in rows if str(row.get("f12") or "").isdigit()})
        connection = open_backtest_db()
        completed = {
            row[0] for row in connection.execute(
                "SELECT DISTINCT code FROM follow_up_observations WHERE source = 'all-market'"
            ).fetchall()
        }
        pending = [code for code in codes if code not in completed]
        connection.close()
        _all_market_backtest_status = {
            "status": "running", "total": len(codes), "processed": len(completed),
            "records": 0, "failed": 0, "startedAt": datetime.now(CHINA_TZ).isoformat(),
        }

        def build_rows(code: str) -> Tuple[str, List[Tuple[Any, ...]]]:
            try:
                bars = get_limit_up_history(code).get("bars") or []
            except (MarketDataError, OSError, ValueError):
                return code, []
            observations: List[Tuple[Any, ...]] = []
            for index in range(BACKTEST_LOOKBACK_DAYS - 1, len(bars) - BACKTEST_HORIZON_DAYS):
                close = bars[index].get("close")
                if close in (None, 0):
                    continue
                future = bars[index + 1:index + 1 + BACKTEST_HORIZON_DAYS]
                future_high = max(float(row.get("high") or row.get("close") or 0) for row in future)
                outcome = int(future_high >= float(close) * (1 + BACKTEST_TARGET_RETURN))
                observations.append((code, bars[index].get("date"), historical_follow_up_score(bars, index), outcome))
            return code, observations

        for start in range(0, len(pending), 40):
            batch = pending[start:start + 40]
            with ThreadPoolExecutor(max_workers=6) as executor:
                results = list(executor.map(build_rows, batch))
            connection = open_backtest_db()
            try:
                for code, observations in results:
                    connection.executemany(
                        "INSERT OR REPLACE INTO follow_up_observations (code, signal_date, score, outcome, source) VALUES (?, ?, ?, ?, 'all-market')",
                        observations,
                    )
                connection.commit()
            finally:
                connection.close()
            _all_market_backtest_status["processed"] += len(batch)
            _all_market_backtest_status["records"] += sum(len(items) for _, items in results)
            _all_market_backtest_status["failed"] += sum(1 for _, items in results if not items)
            _all_market_backtest_status["updatedAt"] = datetime.now(CHINA_TZ).isoformat()
        _all_market_backtest_status["status"] = "completed"
        _all_market_backtest_status["completedAt"] = datetime.now(CHINA_TZ).isoformat()
    except (MarketDataError, OSError, sqlite3.Error, ValueError) as exc:
        _all_market_backtest_status.update({"status": "error", "message": str(exc)})
        print(f"全市场历史回测失败: {exc}")


def all_market_backtest_loop(stop_event: threading.Event) -> None:
    """Run the initial full-market job once after service startup."""
    if stop_event.wait(3):
        return
    run_all_market_backtest()


def limit_up_candidate_score(
    quote: Dict[str, Any], sector_score: float, history_count: int = 0, auction: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    change_pct = float(quote.get("changePct") or 0)
    amount = float(quote.get("amount") or 0)
    turnover = float(quote.get("screener", {}).get("turnover") or 0)
    amplitude = float(quote.get("screener", {}).get("amplitude") or 0)
    volume_ratio = float(quote.get("volumeRatio") or 0)
    float_cap = float(quote.get("floatMarketCap") or 0)
    attack = clamp((change_pct - 2) / 7.8 * 100)
    activity = clamp(35 + math.log10(max(amount, 1) / 100_000_000) * 24 + min(volume_ratio, 5) * 8)
    turnover_score = clamp(100 - abs(turnover - 9) * 6)
    cap_score = clamp(100 - abs(math.log10(max(float_cap, 1)) - 10.5) * 38)
    stability = clamp(100 - max(0, amplitude - abs(change_pct) - 2) * 9)
    history_score = clamp(history_count * 12)
    auction_score = auction_strength_score(auction, quote.get("floatMarketCap"))
    total = round(
        sector_score * 0.20
        + attack * 0.20
        + activity * 0.12
        + turnover_score * 0.08
        + history_score * 0.08
        + cap_score * 0.08
        + stability * 0.04
        + auction_score["score"] * 0.20
    )
    return {
        "total": total,
        "sector": round(sector_score),
        "attack": attack,
        "activity": activity,
        "turnover": turnover_score,
        "history": history_score,
        "marketCap": cap_score,
        "stability": stability,
        "auction": auction_score,
    }


def fetch_limit_up_candidates() -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    totals = 0
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(fetch_market_rows, key) for key in ("sh-main", "sz-main")]
        for future in as_completed(futures):
            market_rows, total = future.result()
            rows.extend(market_rows)
            totals += total

    updated_at = datetime.now(CHINA_TZ)
    quotes: List[Dict[str, Any]] = []
    market_quotes: List[Dict[str, Any]] = []
    for row in rows:
        code = str(row.get("f12") or "")
        name = str(row.get("f14") or "")
        if not main_board_code(code) or "ST" in name.upper() or "退" in name:
            continue
        timestamp = number(row.get("f124"))
        row_updated_at = updated_at
        if timestamp is not None:
            if timestamp > 10_000_000_000:
                timestamp /= 1000
            try:
                row_updated_at = datetime.fromtimestamp(timestamp, CHINA_TZ)
                updated_at = max(updated_at, row_updated_at)
            except (OverflowError, OSError, ValueError):
                pass
        quote = dynamic_snapshot(row, row_updated_at, "all")
        if quote is None or quote["previousClose"] in (None, 0):
            continue
        market_quotes.append(quote)
        if (
            quote["changePct"] < 2
            or quote["amount"] < 100_000_000
            or quote.get("floatMarketCap") is None
            or quote["floatMarketCap"] <= 3_000_000_000
        ):
            continue
        quotes.append(quote)

    sectors = aggregate_sector_strength(market_quotes)
    sector_scores = {item["name"]: item["strengthScore"] for item in sectors}
    sector_details = {item["name"]: item for item in sectors}
    market_date = updated_at.strftime("%Y-%m-%d")
    auction_snapshot = get_auction_snapshot(market_date, capture_if_due=True)
    auction_records = auction_snapshot.get("records") or {}
    for quote in quotes:
        quote["limitUp"] = {
            "score": limit_up_candidate_score(quote, sector_scores.get(quote.get("actualIndustry"), 50), 0),
        }
    quotes.sort(key=lambda item: (item["limitUp"]["score"]["total"], item["changePct"]), reverse=True)
    candidate_pool = quotes[:LIMIT_UP_CANDIDATE_LIMIT]

    histories: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=min(8, len(candidate_pool))) as executor:
        futures = {executor.submit(get_limit_up_history, item["code"]): item["code"] for item in candidate_pool}
        for future in as_completed(futures):
            code = futures[future]
            try:
                histories[code] = future.result()
            except MarketDataError:
                histories[code] = {"available": False, "sealedCount": 0, "touchedCount": 0, "brokenCount": 0, "recentLimitDate": None}

    candidates: List[Dict[str, Any]] = []
    for quote in candidate_pool:
        history = histories[quote["code"]]
        limit_price = normal_limit_price(quote["previousClose"])
        at_limit = quote["price"] >= limit_price - 0.001
        touched_today = (quote.get("high") or 0) >= limit_price - 0.001
        status = "sealed" if at_limit else "broken" if touched_today else "candidate"
        one_word = (
            at_limit
            and (quote.get("open") or 0) >= limit_price - 0.001
            and (quote.get("low") or 0) >= limit_price - 0.001
            and (quote.get("high") or 0) >= limit_price - 0.001
        )
        re_sealed = at_limit and not one_word and (quote.get("low") or 0) < limit_price - 0.001
        board_tag = (
            {"key": "one-word", "label": "一字", "note": "开盘、最低、最高和现价均在涨停价，全天一字封板。"}
            if one_word
            else {"key": "re-seal", "label": "回封", "note": "日内价格低于涨停价，随后收盘重新封住涨停。"}
            if re_sealed
            else {"key": "broken", "label": "炸板", "note": "盘中触及涨停价，但当前未封在涨停价。"}
            if status == "broken"
            else None
        )
        score = limit_up_candidate_score(
            quote,
            sector_scores.get(quote.get("actualIndustry"), 50),
            history["sealedCount"],
            auction_records.get(quote["code"]),
        )
        if status == "broken":
            score["total"] = max(0, score["total"] - 12)
        sector = sector_details.get(quote.get("actualIndustry"), {})
        open_gap = return_pct(quote["previousClose"], quote.get("open"))
        volume_ratio = quote.get("volumeRatio")
        turnover = quote.get("screener", {}).get("turnover")
        float_cap = quote.get("floatMarketCap")
        auction = score["auction"]
        auction_record = auction_records.get(quote["code"])
        sessions_since_limit = history.get("sessionsSinceRecentLimit")
        signals = [
            {"key": "sector", "label": "板块前列", "met": sector.get("rank", 99) <= 9 and sector.get("risingCount", 0) >= 3},
            {"key": "gain", "label": "5%–9.8%强攻", "met": 5 <= quote["changePct"] <= 9.8 and (quote.get("price") or 0) >= (quote.get("open") or 0)},
            {"key": "linkage", "label": "板块联动", "met": sector.get("risingCount", 0) >= 4 and sector.get("advanceRatio", 0) >= 50},
            {"key": "amount", "label": "有效成交额", "met": quote["amount"] >= 100_000_000},
            {"key": "volume", "label": "量比放大", "met": volume_ratio is not None and volume_ratio >= 1.5},
            {"key": "turnover", "label": "换手适中", "met": turnover is not None and 3 <= turnover <= 18},
            {"key": "cap", "label": "流通市值>30亿", "met": float_cap is not None and float_cap > 3_000_000_000},
            {"key": "history", "label": "一年封板记录", "met": history["sealedCount"] >= 1},
            {"key": "recent", "label": "20–60日涨停记忆", "met": sessions_since_limit is not None and 20 <= sessions_since_limit <= 60},
            {"key": "open", "label": "开盘承接稳定", "met": open_gap is not None and open_gap <= 4 and (quote.get("price") or 0) >= (quote.get("open") or 0) and (quote.get("screener", {}).get("amplitude") or 0) <= 12},
            {"key": "auction", "label": "9:25 委买强度", "met": auction["available"] and auction["score"] >= 60},
        ]
        quote["limitUp"] = {
            "limitPrice": limit_price,
            "distancePct": round(max(0.0, (limit_price - quote["price"]) / quote["price"] * 100), 2),
            "status": status,
            "boardTag": board_tag,
            "history": history,
            "score": score,
            "signals": signals,
            "signalCount": sum(1 for signal in signals if signal["met"]),
            "sector": {
                "rank": sector.get("rank"),
                "risingCount": sector.get("risingCount"),
                "memberCount": sector.get("memberCount"),
            },
            "openGapPct": round(open_gap, 2) if open_gap is not None else None,
            "auction": {
                "available": auction["available"],
                "unmatchedBuyAmount": auction_record.get("unmatchedBuyAmount") if auction_record else None,
                "auctionAmount": auction_record.get("auctionAmount") if auction_record else None,
                "auctionVolume": auction_record.get("auctionVolume") if auction_record else None,
                "strengthScore": auction["score"],
                "capturedAt": auction_record.get("capturedAt") if auction_record else auction_snapshot.get("capturedAt"),
            },
        }
        candidates.append(quote)
    candidates.sort(key=lambda item: (item["limitUp"]["score"]["total"], item["changePct"]), reverse=True)
    return {
        "source": "东方财富沪深主板行情快照 + 新浪证券不复权日线" + (" + 东方财富 EMT 09:25 竞价快照" if auction_snapshot.get("available") else ""),
        "marketDate": market_date,
        "updatedAt": updated_at.isoformat(),
        "fetchedAt": datetime.now(CHINA_TZ).isoformat(),
        "scannedCount": totals,
        "candidateCount": len(candidates),
        "auction": {
            "available": auction_snapshot.get("available", False),
            "status": auction_snapshot.get("status"),
            "message": auction_snapshot.get("message"),
            "capturedAt": auction_snapshot.get("capturedAt"),
        },
        "quotes": candidates,
    }


def get_limit_up_candidates(force_refresh: bool = False) -> Dict[str, Any]:
    now = time.monotonic()
    with _cache_lock:
        cached = _limit_up_cache.get("main-board")
        if cached and now - cached["created_at"] < LIMIT_UP_CACHE_TTL_SECONDS and not force_refresh:
            return cached["payload"]
    payload = fetch_limit_up_candidates()
    with _cache_lock:
        _limit_up_cache["main-board"] = {"created_at": time.monotonic(), "payload": payload}
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


def rebound_daily_score(stock: Dict[str, Any]) -> Dict[str, Any]:
    """Independent 0-100 stabilization score based on daily trend, MACD, volume and support."""
    rows = [row for row in stock.get("history", []) if number(row.get("close")) is not None]
    if len(rows) < 25:
        return {"score": None, "factors": {"available": False}}
    closes = [float(row["close"]) for row in rows]
    volumes = [float(number(row.get("volume")) or 0) for row in rows]
    latest = rows[-1]
    close = closes[-1]
    ma5 = sum(closes[-5:]) / 5
    ma10 = sum(closes[-10:]) / 10
    ma20 = sum(closes[-20:]) / 20
    previous_ma5 = sum(closes[-6:-1]) / 5
    trend = 35 + (12 if close >= ma5 else -8) + (10 if close >= ma10 else -5) + (8 if close >= ma20 else -6) + (10 if ma5 >= previous_ma5 else -5)
    trend = clamp(trend)

    def ema(values: List[float], period: int) -> List[float]:
        alpha = 2 / (period + 1)
        result = [values[0]]
        for value in values[1:]:
            result.append(value * alpha + result[-1] * (1 - alpha))
        return result

    dif = [fast - slow for fast, slow in zip(ema(closes, 12), ema(closes, 26))]
    dea = ema(dif, 9)
    hist = [(d - e) * 2 for d, e in zip(dif, dea)]
    macd = 45 + (18 if dif[-1] >= dif[-2] else -8) + (12 if hist[-1] >= hist[-2] else -6) + (10 if hist[-1] > 0 else 0)
    macd = clamp(macd)
    avg_volume = sum(volumes[-6:-1]) / 5 if sum(volumes[-6:-1]) else 0
    volume_ratio = volumes[-1] / avg_volume if avg_volume else 1
    price_volume = clamp(48 + (18 if close >= closes[-2] and volume_ratio >= 1.1 else 0) + (12 if close < closes[-2] and volume_ratio < .85 else 0) - (12 if close < closes[-2] and volume_ratio > 1.3 else 0))
    low20 = min(float(number(row.get("low")) or row["close"]) for row in rows[-20:])
    recent_low = min(float(number(row.get("low")) or row["close"]) for row in rows[-5:])
    support = clamp(45 + (20 if recent_low > low20 else 0) + (15 if close <= low20 * 1.08 else 0) + (10 if close >= closes[-2] else 0))
    score = round(trend * .40 + macd * .30 + price_volume * .20 + support * .10)
    return {"score": int(clamp(score)), "factors": {"trend": round(trend), "macd": round(macd), "priceVolume": round(price_volume), "support": round(support)}}


def rebound_intraday_score(stock: Dict[str, Any], intraday: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    points = list((intraday or {}).get("points") or [])
    prices = [float(item["price"]) for item in points if number(item.get("price")) is not None]
    if len(prices) < 5:
        change = float(number(stock.get("changePct")) or 0)
        return {"score": round(clamp(45 + change * 5)), "factors": {"available": False}}
    current = prices[-1]
    average = sum(prices) / len(prices)
    recent = prices[-min(15, len(prices)):]
    prior = prices[-min(30, len(prices)):-min(15, len(prices))] or prices[:1]
    position = clamp(45 + (20 if current >= average else -10) + (15 if current >= min(recent) else 0))
    structure = clamp(45 + (20 if min(recent) >= min(prior) else -10) + (15 if current >= max(prior) else 0))
    momentum = clamp(50 + ((current / prices[max(0, len(prices) - 6)] - 1) * 100) * 15)
    volumes = [float(number(item.get("volume")) or 0) for item in points]
    recent_volume = sum(volumes[-5:]) / max(1, min(5, len(volumes)))
    previous_volume = sum(volumes[-15:-5]) / max(1, len(volumes[-15:-5])) if len(volumes) > 5 else recent_volume
    volume_score = clamp(45 + (20 if recent_volume >= previous_volume * 1.2 else 0) - (10 if recent_volume < previous_volume * .7 else 0))
    score = round(position * .35 + structure * .30 + momentum * .20 + volume_score * .15)
    return {"score": int(clamp(score)), "factors": {"position": round(position), "structure": round(structure), "momentum": round(momentum), "volume": round(volume_score)}}


def get_rebound_scores(codes: List[str], concept_name: str = "", force_refresh: bool = False, enable_aggressive_alerts: bool = False) -> Dict[str, Any]:
    trade_date = datetime.now(CHINA_TZ).strftime("%Y-%m-%d")
    results: Dict[str, Any] = {}
    sector_score = 50
    if concept_name:
        try:
            summary = get_hot_concept_summaries([concept_name], force_refresh)[0]
            sector_score = round(clamp(50 + float(number(summary.get("averageChangePct")) or 0) * 8))
        except (MarketDataError, OSError, ValueError, IndexError):
            sector_score = 50
    def calculate(code: str) -> Tuple[str, Dict[str, Any]]:
        stock = get_stock_data(code, force_refresh)
        history_rows = list(stock.get("history") or [])
        score_trade_date = str(history_rows[-1].get("date") or trade_date) if history_rows else trade_date
        try:
            intraday = get_intraday_data(code, score_trade_date, force_refresh)
        except MarketDataError:
            intraday = None
        daily = rebound_daily_score(stock)
        intraday_result = rebound_intraday_score(stock, intraday)
        if intraday_result.get("score") is not None:
            intraday_result["score"] = round(float(intraday_result["score"]) * .90 + sector_score * .10)
            intraday_result.setdefault("factors", {})["sector"] = sector_score
        if daily.get("score") is not None:
            with open_backtest_db() as connection:
                connection.execute("INSERT OR REPLACE INTO rebound_daily_scores(trade_date,code,name,daily_score,factors_json,calculated_at) VALUES(?,?,?,?,?,?)",
                    (score_trade_date, code, stock.get("name") or code, daily["score"], json.dumps(daily["factors"], ensure_ascii=False), datetime.now(CHINA_TZ).isoformat()))
                connection.commit()
        if intraday_result.get("score") is not None:
            now_local = datetime.now(CHINA_TZ)
            intraday_points = list((intraday or {}).get("points") or [])
            snapshot_minute = str(intraday_points[-1].get("time") or now_local.strftime("%H:%M"))[:5] if intraday_points else now_local.strftime("%H:%M")
            previous_score = None
            last_alert_time = None
            with open_backtest_db() as connection:
                previous = connection.execute(
                    "SELECT intraday_score FROM rebound_intraday_scores WHERE trade_date=? AND code=? AND minute<? ORDER BY minute DESC LIMIT 1",
                    (score_trade_date, code, snapshot_minute),
                ).fetchone()
                previous_score = number(previous[0]) if previous else None
                latest_alert = connection.execute(
                    """SELECT alert_time FROM alert_events
                       WHERE trade_date=? AND code=? AND alert_kind LIKE 'rebound-60%'
                       ORDER BY alert_time DESC,id DESC LIMIT 1""",
                    (score_trade_date, code),
                ).fetchone()
                last_alert_time = str(latest_alert[0]) if latest_alert else None
                connection.execute("INSERT OR REPLACE INTO rebound_intraday_scores(trade_date,minute,code,name,intraday_score,price,change_pct,factors_json,calculated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (score_trade_date, snapshot_minute, code, stock.get("name") or code, intraday_result["score"], number(stock.get("price")), number(stock.get("changePct")), json.dumps(intraday_result.get("factors") or {}, ensure_ascii=False), now_local.isoformat()))
                connection.commit()
            score_time = f"{snapshot_minute}:00"
            last_seconds, current_seconds = _clock_seconds(last_alert_time), _clock_seconds(score_time)
            crossed_up = previous_score is not None and float(previous_score) < 60 <= float(intraday_result["score"])
            repeat_due = (
                float(intraday_result["score"]) >= 60
                and last_seconds is not None and current_seconds is not None
                and current_seconds - last_seconds >= 300
            )
            if (
                enable_aggressive_alerts
                and is_intraday_alert_session(now_local)
                and score_trade_date == now_local.strftime("%Y-%m-%d")
                and (crossed_up or repeat_due)
            ):
                save_alert_event(
                    score_trade_date, code, str(stock.get("name") or code), f"rebound-60-{snapshot_minute.replace(':', '')}", 60,
                    score_time, {"price": number(stock.get("price")), "changePct": number(stock.get("changePct"))},
                    float(intraday_result["score"]), 0, 0,
                    {
                        **(intraday_result.get("factors") or {}), "concept": concept_name,
                        "previousScore": previous_score, "dailyScore": daily.get("score"),
                    }, None,
                )
        return code, {"code": code, "name": stock.get("name") or code, "dailyScore": daily.get("score"), "intradayScore": intraday_result.get("score"), "dailyFactors": daily.get("factors"), "intradayFactors": intraday_result.get("factors")}
    with ThreadPoolExecutor(max_workers=min(8, len(codes))) as executor:
        futures = [executor.submit(calculate, code) for code in codes]
        for future in as_completed(futures):
            try:
                code, value = future.result()
                results[code] = value
            except (MarketDataError, OSError, ValueError, sqlite3.Error):
                continue
    return {"tradeDate": trade_date, "updatedAt": datetime.now(CHINA_TZ).isoformat(), "concept": concept_name, "sectorScore": sector_score, "scores": results}


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
            self.send_json(200, {"status": "ok", "version": 11, "allMarketBacktest": _all_market_backtest_status})
            return
        if parsed.path == "/api/alert-pool":
            market_key = query.get("market", ["all"])[0]
            if market_key not in MARKET_SCOPES:
                self.send_json(400, {"error": "未知市场范围"})
                return
            raw_codes = [code for code in query.get("codes", [""])[0].split(",") if code]
            if len(raw_codes) > 300 or any(len(code) != 6 or not code.isdigit() for code in raw_codes):
                self.send_json(400, {"error": "告警池股票范围不正确"})
                return
            self.send_json(200, get_today_alert_pool(market_key, set(raw_codes) if raw_codes else None))
            return
        if parsed.path == "/api/score-alerts":
            trade_date = datetime.now(CHINA_TZ).strftime("%Y-%m-%d")
            missing_codes = [code for (date, code, threshold), item in _score_threshold_quotes.items() if date == trade_date and threshold == 70 and (not item.get("price") or not item.get("industry"))]
            if missing_codes:
                try:
                    live_rows, _ = fetch_market_rows("all")
                    live = {str(row.get("f12")): row for row in live_rows}
                    for code in missing_codes:
                        row = live.get(code) or {}
                        _score_threshold_quotes.setdefault((trade_date, code, 70), {}).update({"price": number(row.get("f2")), "changePct": number(row.get("f3")), "industry": str(row.get("f100") or "")})
                    persist_threshold_times()
                except (MarketDataError, OSError, ValueError):
                    pass
            alerts = [
                {"code": code, "name": _score_threshold_names.get((date, code, threshold), code), "time": value, "kind": "score", "threshold": threshold, **(_score_threshold_quotes.get((date, code, threshold)) or {})}
                for (date, code, threshold), value in _score_threshold_times.items()
                if date == trade_date and threshold == 70 and value <= "15:00:00"
            ]
            alerts.extend(
                {"code": code, "name": _prediction_threshold_names.get((date, code, threshold), code), "time": value, "kind": "prediction", "threshold": threshold, **(_prediction_threshold_quotes.get((date, code, threshold)) or {})}
                for (date, code, threshold), value in _prediction_threshold_times.items()
                if date == trade_date and value <= "15:00:00"
            )
            alerts.extend(
                {"code": code, "name": _nonmain_repeat_alert_names.get((date, code), code), "time": value, "kind": "score-repeat", "threshold": 70, **(_nonmain_repeat_alert_quotes.get((date, code)) or {})}
                for (date, code), value in _nonmain_repeat_alert_times.items()
                if date == trade_date and value <= "15:00:00"
            )
            with open_backtest_db() as connection:
                rebound_rows = connection.execute(
                    """SELECT code,name,alert_time,price,change_pct,score,factors_json
                       FROM alert_events
                       WHERE trade_date=? AND alert_kind LIKE 'rebound-60%' AND alert_time<='15:00:00'
                       ORDER BY alert_time,id""",
                    (trade_date,),
                ).fetchall()
            previous_rebound_scores: Dict[str, Optional[float]] = {}
            for row in rebound_rows:
                code = str(row[0])
                factors = json.loads(row[6] or "{}")
                alerts.append({
                    "code": code, "name": row[1] or code, "time": row[2],
                    "kind": "rebound-60", "threshold": 60, "price": row[3],
                    "changePct": row[4], "strategyScore": row[5],
                    "dailyScore": number(factors.get("dailyScore")),
                    "previousScore": previous_rebound_scores.get(code, number(factors.get("previousScore"))),
                    "industry": f"AI算力 / {(factors.get('concept') or '企稳反弹')}",
                })
                previous_rebound_scores[code] = number(row[5])
            # 防止规则修改前已写入内存/持久化的告警继续显示。
            # 命中“高开 >=7% 且低于开盘价”或“盘中涨停后跳水”
            # 的当日股票，从告警框中移除。
            if alerts:
                try:
                    live_rows, _ = fetch_market_rows("all")
                    excluded_codes = {
                        str(row.get("f12") or "") for row in live_rows
                        if high_open_fade(row) or intraday_limit_up_fade(row) or blacklisted_industry(row)
                    }
                    if excluded_codes:
                        alerts = [item for item in alerts if item["code"] not in excluded_codes]
                except (MarketDataError, OSError, ValueError):
                    pass
            for alert in alerts:
                alert_kind = "score" if alert["kind"] == "score-repeat" else alert["kind"]
                alert["consecutiveDays"] = alert_consecutive_days(alert["code"], alert_kind, trade_date)
            repeat_alerts = [alert for alert in alerts if alert["kind"] == "score-repeat"]
            if repeat_alerts:
                with open_backtest_db() as connection:
                    for alert in repeat_alerts:
                        previous = connection.execute(
                            """SELECT change_pct FROM alert_events
                               WHERE trade_date=? AND code=? AND alert_time<?
                                 AND (alert_kind='score' OR alert_kind LIKE 'score-repeat-%')
                                 AND change_pct IS NOT NULL
                               ORDER BY alert_time DESC, id DESC LIMIT 1""",
                            (trade_date, alert["code"], alert["time"]),
                        ).fetchone()
                        alert["previousChangePct"] = number(previous[0]) if previous else None
            alerts.sort(key=lambda item: item["time"])
            # 告警框与盘中选股卡片使用同一题材口径：所属行业 / 第一关联概念。
            # 只补充两个告警框各自最新 10 条，避免历史告警过多拖慢轮询。
            latest_main = [item for item in alerts if is_main_board_code(item["code"]) and item["kind"] != "prediction"][-10:]
            latest_nonmain = [item for item in alerts if not is_main_board_code(item["code"]) and item["kind"] != "prediction"][-10:]
            topic_targets = latest_main + latest_nonmain
            if topic_targets:
                missing_industries = {item["code"] for item in topic_targets if not item.get("industry")}
                if missing_industries:
                    try:
                        live_rows, _ = fetch_market_rows("all")
                        industry_by_code = {
                            str(row.get("f12") or ""): str(row.get("f100") or "")
                            for row in live_rows if str(row.get("f12") or "") in missing_industries
                        }
                        for item in topic_targets:
                            if not item.get("industry"):
                                item["industry"] = industry_by_code.get(item["code"]) or "--"
                                if item["kind"] == "score-repeat":
                                    _nonmain_repeat_alert_quotes.setdefault((trade_date, item["code"]), {}).update({"industry": item["industry"]})
                        persist_threshold_times()
                    except (MarketDataError, OSError, ValueError):
                        pass
                concept_by_code: Dict[str, str] = {}
                with ThreadPoolExecutor(max_workers=min(12, len(topic_targets))) as executor:
                    futures = {executor.submit(get_stock_concepts, item["code"]): item["code"] for item in topic_targets}
                    for future in as_completed(futures):
                        code = futures[future]
                        try:
                            concepts = future.result()
                            concept_names = [str(item.get("name") or "") for item in concepts]
                            target = next((item for item in topic_targets if item["code"] == code), {})
                            concept_by_code[code] = select_primary_business_concept(code, target.get("industry"), concept_names)
                        except (MarketDataError, OSError, ValueError):
                            concept_by_code[code] = ""
                for item in topic_targets:
                    industry = str(item.get("industry") or "--")
                    concept = concept_by_code.get(item["code"], "")
                    if concept in ("共封装光学(CPO)", "共封装光学（CPO）"):
                        concept = "CPO"
                    item["topicLabel"] = f"{industry} / {concept}" if concept else industry
            self.send_json(200, {"tradeDate": trade_date, "alerts": alerts})
            return
        if parsed.path == "/api/score-traces":
            code = query.get("code", [""])[0]
            trade_date = query.get("date", [datetime.now(CHINA_TZ).strftime("%Y-%m-%d")])[0]
            if len(code) != 6 or not code.isdigit():
                self.send_json(400, {"error": "股票代码应为 6 位数字"})
                return
            with open_backtest_db() as connection:
                rows = connection.execute(
                    "SELECT minute, score, predictive_score, predictive_probability, price, change_pct, base_json, order_flow_score, macd_score FROM score_traces WHERE trade_date=? AND code=? ORDER BY minute",
                    (trade_date, code),
                ).fetchall()
            self.send_json(200, {"code": code, "date": trade_date, "traces": [
                {"time": row[0], "score": row[1], "predictiveScore": row[2], "probability": row[3], "price": row[4], "changePct": row[5], "base": json.loads(row[6] or "{}"), "orderFlowScore": row[7], "macdScore": row[8]}
                for row in rows
            ]})
            return
        if parsed.path == "/api/rebound-scores":
            codes = [code.strip() for code in query.get("codes", [""])[0].split(",") if code.strip()]
            if not codes or len(codes) > 120 or any(len(code) != 6 or not code.isdigit() for code in codes):
                self.send_json(400, {"error": "股票代码列表不正确"})
                return
            concept_name = query.get("concept", [""])[0].strip()
            self.send_json(200, get_rebound_scores(
                list(dict.fromkeys(codes)), concept_name, query.get("refresh") == ["1"], query.get("aggressive") == ["1"]
            ))
            return
        if parsed.path == "/api/rebound-daily-history":
            code = query.get("code", [""])[0]
            try:
                page = max(1, int(query.get("page", ["1"])[0]))
                page_size = min(50, max(5, int(query.get("pageSize", ["10"])[0])))
            except ValueError:
                self.send_json(400, {"error": "分页参数不正确"})
                return
            if len(code) != 6 or not code.isdigit():
                self.send_json(400, {"error": "股票代码应为6位数字"})
                return
            with open_backtest_db() as connection:
                total = int(connection.execute("SELECT COUNT(*) FROM rebound_daily_scores WHERE code=?", (code,)).fetchone()[0])
                rows = connection.execute("SELECT trade_date,name,daily_score,factors_json,calculated_at FROM rebound_daily_scores WHERE code=? ORDER BY trade_date DESC LIMIT ? OFFSET ?", (code, page_size, (page - 1) * page_size)).fetchall()
            self.send_json(200, {"code": code, "page": page, "pageSize": page_size, "total": total, "pages": max(1, math.ceil(total / page_size)), "rows": [{"tradeDate": row[0], "name": row[1], "dailyScore": row[2], "factors": json.loads(row[3] or "{}"), "calculatedAt": row[4]} for row in rows]})
            return
        if parsed.path == "/api/rebound-intraday-history":
            code = query.get("code", [""])[0]
            requested_date = query.get("date", [""])[0]
            if len(code) != 6 or not code.isdigit():
                self.send_json(400, {"error": "股票代码应为6位数字"})
                return
            with open_backtest_db() as connection:
                if requested_date:
                    trade_date = requested_date
                else:
                    latest = connection.execute("SELECT MAX(trade_date) FROM rebound_intraday_scores WHERE code=?", (code,)).fetchone()
                    trade_date = str(latest[0] or datetime.now(CHINA_TZ).strftime("%Y-%m-%d"))
                rows = connection.execute("SELECT minute,name,intraday_score,price,change_pct,factors_json FROM rebound_intraday_scores WHERE code=? AND trade_date=? AND ((minute>='09:30' AND minute<='11:30') OR (minute>='13:00' AND minute<='15:00')) ORDER BY minute", (code, trade_date)).fetchall()
            self.send_json(200, {"code": code, "tradeDate": trade_date, "name": rows[-1][1] if rows else code, "points": [{"time": row[0], "score": row[2], "price": row[3], "changePct": row[4], "factors": json.loads(row[5] or "{}")} for row in rows]})
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
        if parsed.path == "/api/market-indices":
            self.send_json(200, {"indices": fetch_market_indices(), "updatedAt": datetime.now(CHINA_TZ).isoformat()})
            return
        if parsed.path == "/api/index-analysis":
            self.send_json(200, fetch_index_analysis())
            return
        if parsed.path == "/api/sector-money-flow":
            self.send_json(200, {
                "source": "东方财富行业板块主力资金流",
                "sectorMoneyFlow": fetch_major_sector_money_flow(),
                "updatedAt": datetime.now(CHINA_TZ).isoformat(),
            })
            return
        if parsed.path == "/api/hot-concept":
            concept_name = query.get("name", [""])[0].strip()
            if not concept_name or len(concept_name) > 20:
                self.send_json(400, {"error": "概念板块名称不正确"})
                return
            try:
                self.send_json(200, get_hot_concept_data(concept_name, query.get("refresh") == ["1"]))
            except MarketDataError as exc:
                self.send_json(502, {"error": str(exc)})
            return
        if parsed.path == "/api/hot-concept-summary":
            names = [name.strip() for name in query.get("names", [""])[0].split(",") if name.strip()]
            if not names or len(names) > 8 or any(len(name) > 20 for name in names):
                self.send_json(400, {"error": "概念板块名称列表不正确"})
                return
            self.send_json(200, {
                "updatedAt": datetime.now(CHINA_TZ).isoformat(),
                "concepts": get_hot_concept_summaries(names, query.get("refresh") == ["1"]),
            })
            return
        if parsed.path == "/api/midterm":
            self.send_json(200, get_midterm_pool(query.get("refresh") == ["1"]))
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
                        query.get("includeAll") == ["1"],
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
        if parsed.path == "/api/concept-rank":
            try:
                self.send_json(200, fetch_concept_rank_data())
            except MarketDataError as exc:
                self.send_json(502, {"error": str(exc)})
            return
        if parsed.path == "/api/limit-up":
            try:
                self.send_json(
                    200,
                    get_limit_up_candidates(query.get("refresh") == ["1"]),
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
    capture_stop = threading.Event()
    threading.Thread(target=auction_capture_loop, args=(capture_stop,), name="auction-capture", daemon=True).start()
    threading.Thread(target=follow_up_snapshot_loop, args=(capture_stop,), name="follow-up-snapshot", daemon=True).start()
    threading.Thread(target=all_market_backtest_loop, args=(capture_stop,), name="all-market-backtest", daemon=True).start()
    print(f"TradingAgents 板块选股工作台已启动: http://{args.host}:{args.port}/")
    if EMT_AUCTION_COMMAND:
        print("EMT 竞价采集已配置：交易日 09:25 后将自动保存竞价快照")
    else:
        print("EMT 竞价采集未配置：涨停复盘将把竞价委买因子标为待接入")
    print("后续上涨模型将在交易日 15:05 后保存当日候选，并在满 10 个交易日后自动校准")
    print("全市场历史回测已在后台启动：已完成股票将自动跳过，支持断点续跑")
    print("按 Ctrl+C 停止服务")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        capture_stop.set()
        server.server_close()


if __name__ == "__main__":
    main()
