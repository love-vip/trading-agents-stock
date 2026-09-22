"""Minute-by-minute price/volume evidence, persisted locally and dated strictly.

Scores are NOT probabilities. Empirical probabilities require chronological
holdout validation; historical replays always train on dates before the replay.
Only public market data is stored. No account information is used.
"""
import json
import math
import sqlite3
import statistics
import threading
import time
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

TZ = timezone(timedelta(hours=8))
VERSION = 1


def number(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def market(code):
    return ('chinext' if code.startswith(('300', '301')) else
            'star' if code.startswith(('688', '689')) else
            'bse' if code.startswith(('4', '8', '92')) else 'main')


def session(now):
    t = now.strftime('%H:%M')
    return now.weekday() < 5 and ('09:30' <= t <= '11:30' or '13:00' <= t < '15:00')


def ratio(a, b):
    return round(a / b, 3) if a is not None and b is not None and b > 0 else None


def parse_tencent(payload, symbol):
    """Tencent prices are unadjusted; its minute volume/amount are cumulative.

    BSE may omit amount and use a different volume unit: use within-stock volume
    ratios only, never fabricate turnover or a VWAP from price * volume.
    """
    data = (payload.get('data') or {}).get(symbol) or {}
    days = {}
    for day in data.get('data') or []:
        raw_date = str(day.get('date') or '')
        if len(raw_date) != 8 or not raw_date.isdigit():
            continue
        date = f'{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}'
        points = {}
        for raw in day.get('data') or []:
            parts = str(raw).split()
            if len(parts) < 3 or len(parts[0]) != 4:
                continue
            minute = parts[0][:2] + ':' + parts[0][2:]
            price, volume = number(parts[1]), number(parts[2])
            amount = number(parts[3]) if len(parts) > 3 else None
            if not ('09:25' <= minute <= '11:30' or '13:00' <= minute <= '15:00'):
                continue
            if price is None or price <= 0 or volume is None or volume < 0:
                continue
            points[minute] = {'time': minute, 'price': price, 'volume': volume,
                              'amount': amount if amount is not None and amount >= 0 else None}
        ordered = sorted(points.values(), key=lambda p: p['time'])
        if ordered and all(b['volume'] >= a['volume'] for a, b in zip(ordered, ordered[1:])):
            days[date] = ordered
    if not days:
        raise ValueError('分钟行情源未返回有效数据')
    return days


def feature_at(days, date, minute):
    """Use data through minute only; compare two *observed trading days*.

    Rolling ten-minute windows never straddle lunch. Exact endpoints and at
    least nine observations per window are required (a missing bar isn't zero).
    """
    points = {p['time']: p for p in days.get(date, []) if p['time'] <= minute}
    if minute not in points or minute < '09:50' or minute > '14:59':
        return None
    hour, mins = map(int, minute.split(':'))
    def earlier(n):
        v = hour * 60 + mins - n
        return f'{v // 60:02d}:{v % 60:02d}'
    t10, t20 = earlier(10), earlier(20)
    if minute >= '13:00' and t20 < '13:00':
        return None
    if any(t not in points for t in (t10, t20)):
        return None
    if any(sum(a < t <= b for t in points) < 9 for a, b in ((t20, t10), (t10, minute))):
        return None
    p, a, b = points[minute], points[t10], points[t20]
    past_dates = sorted(d for d in days if d < date)
    if not past_dates:
        return None
    previous_day = {x['time']: x for x in days[past_dates[-1]]}
    previous_close = (previous_day.get('15:00') or {}).get('price')
    if not previous_close:
        return None
    change = (p['price'] / previous_close - 1) * 100
    v10, vprev = p['volume'] - a['volume'], a['volume'] - b['volume']
    ret10 = (p['price'] / a['price'] - 1) * 100
    retprev = (a['price'] / b['price'] - 1) * 100
    avg = p['amount'] / p['volume'] if p['amount'] and p['volume'] else None
    if avg is not None and not 0.5 < avg / p['price'] < 1.5:
        avg = None  # Ambiguous units must not become an apparent price mean.
    comparisons = []
    for old_date in reversed(past_dates[-2:]):
        old = {x['time']: x for x in days[old_date]}
        q, q10 = old.get(minute), old.get(t10)
        before_old = [d for d in past_dates if d < old_date]
        old_close = next((x['price'] for x in days[before_old[-1]] if x['time'] == '15:00'), None) if before_old else None
        comparisons.append({'date': old_date,
            'cumulativeVolumeRatio': ratio(p['volume'], q['volume']) if q else None,
            'windowVolumeRatio': ratio(v10, q['volume'] - q10['volume']) if q and q10 else None,
            'cumulativeAmountRatio': ratio(p['amount'], q['amount']) if q else None,
            'changePct': round((q['price'] / old_close - 1) * 100, 3) if q and old_close else None})
    # Explicit rule strength; these weights are not a learned probability model.
    score = 50 + max(-15, min(15, ret10 * 8)) + max(-8, min(8, (ret10-retprev)*4))
    reasons = [f'近10分钟 {ret10:+.2f}%']
    if avg:
        score += max(-12, min(12, (p['price']/avg-1)*100*4))
        reasons.append('价格在分时均价上方' if p['price'] >= avg else '价格在分时均价下方')
    # ``rv`` measures the current rolling ten-minute volume against the
    # immediately preceding ten minutes.  The two same-time ratios below use
    # yesterday and the day before, so a quiet morning is not mistaken for
    # contraction simply because the whole market is quieter than yesterday.
    rv = ratio(v10, vprev)
    same_time = [x['windowVolumeRatio'] for x in comparisons if x['windowVolumeRatio'] is not None]
    same_time_ratio = round(statistics.mean(same_time), 3) if same_time else None
    volume_state = '放量' if rv is not None and rv >= 1.2 else '缩量' if rv is not None and rv <= .8 else '量能平稳'
    same_time_state = ('较昨日/前日同期放量' if same_time_ratio is not None and same_time_ratio >= 1.2 else
                       '较昨日/前日同期缩量' if same_time_ratio is not None and same_time_ratio <= .8 else
                       '较昨日/前日同期量能平稳')
    reasons.append(f'近10分钟{volume_state}')
    if same_time_ratio is not None:
        reasons.append(same_time_state)
    if rv is not None and rv >= 1.2:
        score += 7 if ret10 > 0 else -7
        reasons.append('本区间放量上涨' if ret10 > 0 else '本区间放量回落')
    if rv is not None and rv <= .8 and ret10 < 0:
        reasons.append('本区间缩量回落')
    if same_time_ratio is not None and same_time_ratio >= 1.2:
        score += 5 if ret10 > 0 else -5
        reasons.append('高于前两日同时段成交量')
    score = round(max(0, min(100, score)))
    return {'date': date, 'minute': minute, 'price': p['price'], 'previousClose': previous_close,
            'changePct': round(change, 3), 'averagePrice': round(avg, 4) if avg else None,
            'return10': round(ret10, 3), 'previousReturn10': round(retprev, 3),
            'windowVolumeRatio': rv, 'sameTimeVolumeRatio': same_time_ratio,
            'volumeState': volume_state, 'sameTimeVolumeState': same_time_state,
            'comparisons': comparisons, 'score': score,
            'signal': '量价偏强' if score >= 65 else '量价偏弱' if score <= 35 else '量价中性',
            'reasons': reasons, 'baselineDays': len([c for c in comparisons if c['cumulativeVolumeRatio'] is not None])}


def cell(feature, code):
    # Keep market limit regimes, forecast horizon and current return distinct.
    return '|'.join((market(code), feature['minute'][:2],
                     str(max(-3, min(3, math.floor(feature['changePct']/3)))),
                     str(feature['score']//20)))


class MinutePredictions:
    def __init__(self, db_path, universe, fetch_json):
        self.db_path, self.universe, self.fetch_json = str(db_path), universe, fetch_json
        self.lock = threading.RLock()
        self.running = False
        self.last_started = 0.0
        self.members = {}
        self.results = {}
        self.progress = 0
        self.error = ''
        self.models = {}
        with self.db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS prediction_minutes (
                    code TEXT, trade_date TEXT, minute TEXT, price REAL, volume REAL, amount REAL,
                    PRIMARY KEY(code,trade_date,minute));
                CREATE TABLE IF NOT EXISTS prediction_samples (
                    code TEXT, trade_date TEXT, minute TEXT, cell TEXT, feature_json TEXT,
                    close_up INTEGER, further_up INTEGER, version INTEGER,
                    PRIMARY KEY(code,trade_date,minute,version));
                CREATE TABLE IF NOT EXISTS prediction_outputs (
                    code TEXT, trade_date TEXT, minute TEXT, payload_json TEXT, computed_at TEXT,
                    PRIMARY KEY(code,trade_date,minute));
            ''')

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.db_path, timeout=15)
        db.execute('PRAGMA busy_timeout=15000')
        try:
            with db:
                yield db
        finally:
            db.close()

    def request(self):
        with self.lock:
            if self.running or time.monotonic() - self.last_started < 60:
                return
            self.running, self.progress, self.error = True, 0, ''
            self.last_started = time.monotonic()
        threading.Thread(target=self.run, daemon=True, name='minute-predictions').start()

    def view(self):
        now = datetime.now(TZ)
        with self.lock:
            stocks = []
            # Database removals take effect without waiting for the next scan.
            with self.db() as db:
                excluded = {x[0] for x in db.execute('SELECT code FROM custom_blacklist WHERE active=1')}
            for code in self.members:
                if code in excluded:
                    continue
                item = dict(self.results.get(code) or {'code': code, 'name': self.members[code].get('name'),
                                                       'status': 'loading', 'signal': '等待分钟数据'})
                # Search metadata comes from the same pinyin index as the
                # whitelist, including while predictions are loading/cached.
                item['nameInitials'] = self.members[code].get('nameInitials') or ''
                if item.get('date') and session(now):
                    stamp = datetime.fromisoformat(item['date'] + 'T' + item['minute']).replace(tzinfo=TZ)
                    if (now-stamp).total_seconds() > 180:
                        item.update(status='stale', signal='数据延迟', closeUpProbability=None, furtherUpProbability=None)
                stocks.append(item)
            return {'running': self.running, 'processed': self.progress, 'total': len(self.members),
                    'intervalSeconds': 60, 'active': session(now), 'error': self.error,
                    'updatedAt': now.isoformat(), 'stocks': stocks,
                    'note': '每分钟计算；最近10分钟滚动对比。概率来自预测日前历史相似样本并做贝叶斯收缩；不足20个交易日时标为初步估计，满20日后采用时间外验证。休市展示最近交易日11:30回放。'}

    def run(self):
        try:
            members = {str(s['code']): s for s in self.universe()['stocks']}
            with self.lock:
                self.members = members
                self.results = {k: v for k, v in self.results.items() if k in members}
            # Bounded concurrency; no retry storm or overlapping full scans.
            codes = sorted(members, key=lambda c: self.results.get(c, {}).get('computedAt', ''))
            with ThreadPoolExecutor(max_workers=8) as executor:
                jobs = {executor.submit(self.predict, code, members[code]): code for code in codes}
                for job in as_completed(jobs):
                    code = jobs[job]
                    try:
                        result = job.result()
                    except Exception:
                        result = {'code': code, 'name': members[code].get('name'), 'status': 'missing',
                                  'signal': '分钟数据暂不可用', 'computedAt': datetime.now(TZ).isoformat()}
                    with self.lock:
                        self.results[code] = result
                        self.progress += 1
        except Exception as exc:
            with self.lock:
                self.error = '白名单读取失败，稍后重试'
        finally:
            with self.lock:
                self.running = False
                self.models.clear()

    def model(self, before_date):
        with self.lock:
            if before_date in self.models:
                return self.models[before_date]
        with self.db() as db:
            rows = db.execute('''SELECT trade_date,cell,close_up,further_up FROM prediction_samples
                WHERE trade_date<? AND version=? AND close_up IS NOT NULL
                AND substr(minute,5,1)='0' ORDER BY trade_date''', (before_date, VERSION)).fetchall()
        dates = sorted({r[0] for r in rows})[-90:]
        result = {'valid': False, 'provisional': False, 'days': len(dates), 'samples': len(rows)}
        # Before enough independent trading days have accumulated for a strict
        # chronological holdout, expose a clearly labelled empirical-Bayes
        # estimate. Every sample is earlier than ``before_date``. Cell rates
        # are shrunk toward the market-wide base rate, preventing sparse cells
        # from displaying misleading 0% or 100% probabilities.
        if len(dates) >= 2:
            preliminary_rows = [r for r in rows if dates[0] <= r[0] <= dates[-1]]
            global_p = [statistics.mean(r[i] for r in preliminary_rows) for i in (2, 3)]
            groups = {}
            for row in preliminary_rows:
                groups.setdefault(row[1], []).append(row)
            prior_strength = 100
            cells = {}
            counts = {}
            cell_days = {}
            for key, group in groups.items():
                distinct_days = len({r[0] for r in group})
                if len(group) < 30 or distinct_days < 2:
                    continue
                cells[key] = [
                    (sum(r[i] for r in group) + prior_strength * global_p[i - 2]) /
                    (len(group) + prior_strength)
                    for i in (2, 3)
                ]
                counts[key] = len(group)
                cell_days[key] = distinct_days
            result.update(provisional=bool(cells), cells=cells, counts=counts,
                          cellDays=cell_days, globalProbability=global_p,
                          priorStrength=prior_strength)
        if len(dates) >= 20:
            train = [r for r in rows if dates[0] <= r[0] < dates[-5]]
            test = [r for r in rows if r[0] >= dates[-5]]
            global_p = [statistics.mean(r[i] for r in train) for i in (2, 3)]
            groups = {}
            for row in train:
                groups.setdefault(row[1], []).append(row)
            cells = {}
            for key, group in groups.items():
                if len(group) >= 40 and len({r[0] for r in group}) >= 8:
                    cells[key] = [(sum(r[i] for r in group)+20*global_p[i-2])/(len(group)+20) for i in (2, 3)]
            valid_test = [r for r in test if r[1] in cells]
            if len(valid_test) >= 100 and len({r[0] for r in valid_test}) >= 5:
                brier = [statistics.mean((cells[r[1]][i-2]-r[i])**2 for r in valid_test) for i in (2, 3)]
                baseline = [statistics.mean((global_p[i-2]-r[i])**2 for r in valid_test) for i in (2, 3)]
                gap = [abs(statistics.mean(cells[r[1]][i-2]-r[i] for r in valid_test)) for i in (2, 3)]
                valid = all(b < ref and g <= .10 for b, ref, g in zip(brier, baseline, gap))
                result.update(valid=valid, brier=brier, baselineBrier=baseline,
                              validationSamples=len(valid_test), validationEnd=dates[-1])
                if valid:
                    result.update(cells=cells, counts={k: len(v) for k, v in groups.items()},
                                  cellDays={k: len({r[0] for r in v}) for k, v in groups.items()})
        with self.lock:
            self.models[before_date] = result
        return result

    def predict(self, code, meta):
        now = datetime.now(TZ)
        with self.db() as db:
            rows = db.execute('SELECT trade_date,minute,price,volume,amount FROM prediction_minutes WHERE code=? ORDER BY trade_date,minute', (code,)).fetchall()
        days = {}
        for date, minute, price, volume, amount in rows:
            days.setdefault(date, []).append(dict(time=minute, price=price, volume=volume, amount=amount))
        last_date = max(days, default='')
        # Completed local history is enough for review; never call it today's data.
        local_complete = last_date and any(p['time'] == '15:00' for p in days[last_date])
        need_fetch = session(now) or not local_complete or last_date < (now-timedelta(days=3 if now.weekday() == 6 else 1)).strftime('%Y-%m-%d')
        fetch_failed = False
        if need_fetch:
            symbol = ('sh' if code.startswith(('6',)) else 'bj' if market(code)=='bse' else 'sz') + code
            try:
                data = self.fetch_json('https://web.ifzq.gtimg.cn/appstock/app/day/query', {'code': symbol})
                fetched = parse_tencent(data, symbol)
                with self.db() as db:
                    db.executemany('INSERT OR REPLACE INTO prediction_minutes VALUES (?,?,?,?,?,?)',
                        [(code,d,p['time'],p['price'],p['volume'],p['amount']) for d,points in fetched.items() for p in points])
                for d, points in fetched.items():
                    merged = {p['time']: p for p in days.get(d, [])}
                    merged.update({p['time']: p for p in points})
                    days[d] = sorted(merged.values(), key=lambda p:p['time'])
            except Exception:
                fetch_failed = True
        if not days:
            raise ValueError('无分钟数据')
        date = max(days)
        mode = 'live' if session(now) else 'review'
        cutoff = now.strftime('%H:%M') if mode == 'live' else '11:30'
        # Exclude the in-progress minute, and never consume future timestamps.
        available = [p['time'] for p in days[date] if p['time'] <= cutoff and (mode != 'live' or p['time'] < cutoff)]
        minute = max(available, default='09:30')
        feature = feature_at(days, date, minute)
        base = {'code': code, 'name': meta.get('name') or code, 'mode': mode, 'date': date,
                'minute': minute, 'industry': meta.get('industry'), 'relatedConcepts': meta.get('relatedConcepts', []),
                'source': '腾讯分钟行情 / 本地存档', 'computedAt': now.isoformat(),
                'closeUpProbability': None, 'furtherUpProbability': None}
        # Save completed ten-minute training samples separately from live outputs.
        with self.db() as db:
            done = {r[0] for r in db.execute('SELECT DISTINCT trade_date FROM prediction_samples WHERE code=? AND close_up IS NOT NULL AND version=?', (code, VERSION))}
            for d in sorted(days):
                if d in done:
                    continue
                closing = next((p['price'] for p in days[d] if p['time']=='15:00'), None)
                if closing is None or d > now.strftime('%Y-%m-%d') or (d == now.strftime('%Y-%m-%d') and now.strftime('%H:%M') <= '15:00'):
                    continue
                for p in days[d]:
                    if not p['time'].endswith('0'):
                        continue
                    f = feature_at(days, d, p['time'])
                    if not f or f['baselineDays'] < 2:
                        continue
                    db.execute('INSERT OR REPLACE INTO prediction_samples VALUES (?,?,?,?,?,?,?,?)',
                        (code,d,p['time'],cell(f,code),json.dumps(f,ensure_ascii=False),int(closing>f['previousClose']),int(closing>f['price']),VERSION))
        if not feature:
            return dict(base, status='insufficient', signal='分钟窗口不足', message='需连续20分钟及上一交易日收盘数据；午后13:20起恢复滚动比较')
        model = self.model(date)
        base.update(feature, status='ready', probabilityStatus='历史样本不足', trainingDays=model['days'],
                    historicalSamples=model['samples'])
        model_cell = cell(feature, code)
        if (model['valid'] or model['provisional']) and feature['baselineDays'] == 2 and model_cell in model.get('cells', {}):
            probs = model['cells'][model_cell]
            verified = model['valid']
            base.update(closeUpProbability=round(probs[0]*100,1), furtherUpProbability=round(probs[1]*100,1),
                        probabilityStatus=('历史验证估计' if verified else f'初步历史估计（{model["days"]}日）'),
                        probabilityVerified=verified, similarSamples=model['counts'][model_cell],
                        similarTradingDays=model.get('cellDays', {}).get(model_cell),
                        validationSamples=model.get('validationSamples'), validationEnd=model.get('validationEnd'))
        if mode == 'live' and (fetch_failed or date != now.strftime('%Y-%m-%d') or
             (now-datetime.fromisoformat(date+'T'+minute).replace(tzinfo=TZ)).total_seconds() > 180):
            base.update(status='stale', signal='数据延迟', closeUpProbability=None, furtherUpProbability=None)
        with self.db() as db:
            db.execute('INSERT OR IGNORE INTO prediction_outputs VALUES (?,?,?,?,?)',
                       (code,date,minute,json.dumps(base,ensure_ascii=False),now.isoformat()))
        return base

    def loop(self, stop):
        last_minute = ''
        while not stop.is_set():
            now = datetime.now(TZ)
            key = now.strftime('%Y-%m-%d %H:%M')
            # Closing pass archives the day's outcomes. Holidays produce stale
            # status, never a silently substituted date or a live prediction.
            active = session(now) or (now.weekday()<5 and '15:01' <= now.strftime('%H:%M') <= '15:05')
            if active and key != last_minute:
                self.request()
                last_minute = key
            stop.wait(1)
