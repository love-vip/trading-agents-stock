"""Independent bearish-MA pool; only its M signal is mirrored to global alerts."""
import json
import logging
import math
import re
import sqlite3
import threading
import time
from datetime import datetime
from types import SimpleNamespace


RULES = ('双线同步翻红', '慢线连红≥3日+快线翻红', '快线连红≥2日+慢线翻红')
# This is the only MACD空头 signal that is allowed into the shared alert frame.
TOP_ALERT_RULE = RULES[2]


class MacdBearService:
    def __init__(self, api):
        self.api = SimpleNamespace(**api)
        self.lock = threading.Lock()
        self.status = {'running': False, 'lastError': ''}
        self.finalized = ''
        self.last_started = 0.0
        with self.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS macd_bear_members (trade_date TEXT NOT NULL, code TEXT NOT NULL, stock_json TEXT NOT NULL, scanned_at TEXT NOT NULL, PRIMARY KEY(trade_date,code))')
            db.execute('CREATE TABLE IF NOT EXISTS macd_bear_alert_history (id INTEGER PRIMARY KEY AUTOINCREMENT, trade_date TEXT NOT NULL, code TEXT NOT NULL, signal TEXT NOT NULL, event_json TEXT NOT NULL, global_mirrored INTEGER NOT NULL DEFAULT 0)')
            db.execute('CREATE INDEX IF NOT EXISTS macd_bear_alert_lookup ON macd_bear_alert_history(trade_date,code,signal,id)')
            db.execute('CREATE TABLE IF NOT EXISTS macd_bear_scans (trade_date TEXT PRIMARY KEY, completed_at TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS macd_bear_quotes (trade_date TEXT, code TEXT, quote_json TEXT, PRIMARY KEY(trade_date,code))')
            # Existing records were created before M alerts were eligible for the
            # public stream.  Mark them as handled so deployment never fabricates
            # delayed alerts from historical scans.
            columns = {row[1] for row in db.execute('PRAGMA table_info(macd_bear_alert_history)')}
            if 'global_mirrored' not in columns:
                db.execute('ALTER TABLE macd_bear_alert_history ADD COLUMN global_mirrored INTEGER NOT NULL DEFAULT 0')
                db.execute('UPDATE macd_bear_alert_history SET global_mirrored=1')

    def connect(self):
        return sqlite3.connect(self.api.BACKTEST_DB_PATH, timeout=20)

    def rules(self, closes, price, change):
        a = self.api
        if price is None or price < a.MACD_MIN_ENTRY_PRICE or change is None or change < 0 or len(closes) < 30:
            return [], 0, 0
        values = closes + [price]
        if not a.macd_is_bearish_arrangement(values):
            return [], 0, 0
        slow = a.macd_histogram_series(values, 10, 20, 7)
        fast = a.macd_histogram_series(values, 5, 10, 3)
        def red_days(bars):
            count = 0
            for value in reversed(bars):
                if value < 0:
                    break
                count += 1
            return count
        sd, fd = red_days(slow), red_days(fast)
        sf, both, fs = a.macd_flip_strength_passes(slow, fast, sd, fd, change)
        return [rule for rule, hit in zip(RULES, (both, sf, fs)) if hit], sd, fd

    def scan(self, day, live=False):
        if not self.lock.acquire(blocking=False):
            return
        a = self.api
        self.last_started = time.monotonic()
        self.status.update(running=True, lastError='')
        try:
            now = datetime.now(a.CHINA_TZ)
            should_mirror = False
            with self.connect() as db:
                histories = db.execute('SELECT code,bars_json FROM midterm_history_cache').fetchall()
                retired = {r[0] for r in db.execute('SELECT code FROM retired_security_codes WHERE active=1')}
                old = {c: json.loads(raw) for c, raw in db.execute('SELECT code,stock_json FROM macd_bear_members WHERE trade_date=?', (day,))}
                prev_day = db.execute('SELECT MAX(trade_date) FROM daily_close_quotes WHERE trade_date<?', (day,)).fetchone()[0]
                amounts = {c: a.number(json.loads(raw).get('amount',json.loads(raw).get('f6'))) for c, raw in db.execute('SELECT code,quote_json FROM daily_close_quotes WHERE trade_date=?', (prev_day,))}
                snapshots = {c: json.loads(raw) for c, raw in db.execute('SELECT code,quote_json FROM daily_close_quotes WHERE trade_date=?', (day,))}
            if not prev_day:
                raise ValueError('缺少前一交易日数据')
            if live:
                rows = a.fetch_tencent_market_rows([c for c, _ in histories if c not in retired])
                if not rows:
                    raise ValueError('MACD空头实时行情为空')
                quotes = {c: {'code': c, 'name': r.get('f14'), 'price': a.number(r.get('f2')), 'changePct': a.number(r.get('f3')),
                              'open': a.number(r.get('f17')), 'high': a.number(r.get('f15')), 'low': a.number(r.get('f16')),
                              'amount': a.number(r.get('f6')), 'netInflow': a.number(r.get('f62')),
                              'marketCap': a.number(r.get('f20')), 'floatMarketCap': a.number(r.get('f21')),
                              'turnover': a.number(r.get('f8')), 'industry': r.get('f100') or '--'} for c, r in rows.items()}
            else:
                quotes = snapshots
                if len(quotes) < max(1, int((len(histories)-len(retired)) * .95)):
                    raise ValueError('MACD空头收盘快照不完整，保留原池等待补齐')
            selected, unavailable = {}, 0
            for code, raw in histories:
                if code in retired or not a.macd_passes_previous_day_liquidity(amounts.get(code)):
                    continue
                quote = quotes.get(code)
                history = sorted((dict(b, date=a.canonical_history_date(b.get('date'))) for b in json.loads(raw) if isinstance(b, dict)), key=lambda b: b['date'])
                history = [b for b in history if b['date'] and b['date'] < day]
                valid = (quote is not None and a.number(quote.get('price')) is not None
                         and a.number(quote.get('changePct')) is not None and a.quote_price_within_day_range(quote)
                         and len(history) >= 30 and history[-1]['date'] == prev_day)
                if not valid:
                    unavailable += 1
                    if code in old:
                        selected[code] = dict(old[code], quoteUnavailable=True)
                    continue
                closes = [a.number(b.get('close')) for b in history]
                if any(v is None for v in closes):
                    unavailable += 1
                    if code in old:
                        selected[code] = dict(old[code], quoteUnavailable=True)
                    continue
                rules, sd, fd = self.rules(closes, a.number(quote['price']), a.number(quote['changePct']))
                if not rules:
                    continue
                stock = dict(quote, code=code, name=quote.get('name') or code,
                             quoteTradeDate=day, liveQuote=live, updatedAt=now.isoformat(),
                             quoteUnavailable=False, macdSelectionRules=rules, macdSlowRedDays=sd,
                             macdFastRedDays=fd, macdLatestChangePct=quote['changePct'],
                             macdThreeDayGain=(quote['price']/closes[-3]-1)*100,
                             macdPreviousDayAmount=amounts.get(code), macdPreviousDayTradeDate=prev_day)
                # Preserve the entry concept; do not recalculate its rank every minute.
                stock['industry'] = (old.get(code) or {}).get('industry') or a.top_concept_for_stock(code, quote.get('industry'), day)
                stock['actualIndustry'] = stock['industry']
                stock['sector'] = stock['industry']
                selected[code] = stock
            with self.connect() as db:
                db.execute('DELETE FROM macd_bear_members WHERE trade_date=?', (day,))
                db.executemany('INSERT INTO macd_bear_members VALUES (?,?,?,?)', [(day,c,json.dumps(s,ensure_ascii=False),now.isoformat()) for c,s in selected.items()])
                db.execute('INSERT OR REPLACE INTO macd_bear_scans VALUES (?,?)', (day,now.isoformat()))
                tracked = set(selected) | {r[0] for r in db.execute('SELECT DISTINCT code FROM macd_bear_alert_history WHERE trade_date=?',(day,))}
                db.executemany('INSERT OR REPLACE INTO macd_bear_quotes VALUES (?,?,?)',
                               [(day,c,json.dumps(q,ensure_ascii=False)) for c,q in quotes.items()
                                if c in tracked and a.quote_price_within_day_range(q)])
                # A slow scan must not emit after the market has already closed.
                if live and day == now.date().isoformat() and a.is_alert_scan_session(datetime.now(a.CHINA_TZ)):
                    self.record_alerts(db, day, now, old, selected)
                    should_mirror = True
            # record_market_alert opens the shared SQLite database itself.  Do it
            # only after this connection commits, otherwise concurrent writers can
            # leave the bear event recorded but the top-frame event unavailable.
            if should_mirror:
                self.mirror_pending_m_alerts(day)
            self.status.update(lastCompletedAt=now.isoformat(), unavailable=unavailable, count=len(selected))
            if not live:
                self.finalized = day
        except Exception as exc:
            self.status.update(lastError=str(exc), lastErrorAt=datetime.now(a.CHINA_TZ).isoformat())
            logging.exception('MACD空头扫描失败')
        finally:
            self.status['running'] = False
            self.lock.release()

    def record_alerts(self, db, day, now, previous, selected):
        a = self.api
        first = db.execute('SELECT 1 FROM macd_bear_alert_history WHERE trade_date=? LIMIT 1',(day,)).fetchone() is None
        for code, stock in selected.items():
            if stock.get('quoteUnavailable'):
                continue
            change = a.number(stock.get('changePct'))
            if change is None or change >= a.market_limit_ratio(code, stock.get('name',''))*100-.01:
                continue
            for rule in stock['macdSelectionRules']:
                signal = a.macd_selection_rule_label(rule)
                prior = db.execute('SELECT event_json FROM macd_bear_alert_history WHERE trade_date=? AND code=? AND signal=? ORDER BY id DESC LIMIT 1',(day,code,signal)).fetchone()
                if not first and code in previous and prior and abs(change-json.loads(prior[0])['changePct']) < 1:
                    continue
                event = dict(tradeDate=day,time=now.strftime('%H:%M:%S'),code=code,name=stock['name'],
                             board=a.alert_board_for_code(code),pool='MACD空头',price=stock['price'],changePct=change,
                             industry=stock.get('industry','--'),signal=signal,signals=[],tone='red',
                             alertType='macdScreenerEnter',config='macd-bear:'+rule)
                db.execute('INSERT INTO macd_bear_alert_history(trade_date,code,signal,event_json) VALUES (?,?,?,?)',(day,code,signal,json.dumps(event,ensure_ascii=False)))

    def mirror_pending_m_alerts(self, day):
        """Mirror new M records to the shared top alert / alert-pool stream.

        The explicit acknowledgement makes the operation safe across a restart:
        a successfully persisted shared alert is never pushed a second time, and
        a transient shared-database failure remains eligible for the next scan.
        """
        a = self.api
        signal = a.macd_selection_rule_label(TOP_ALERT_RULE)
        with self.connect() as db:
            pending = list(db.execute(
                'SELECT id,event_json FROM macd_bear_alert_history '
                'WHERE trade_date=? AND signal=? AND global_mirrored=0 ORDER BY id',
                (day, signal),
            ))
        for event_id, raw in pending:
            event = json.loads(raw)
            # The shared history endpoint and in-memory alert queue expect a
            # durable event id; make it distinct from normal-pool records.
            event['id'] = 'macd-bear-%s' % event_id
            a.record_market_alert(event)
            with self.connect() as db:
                db.execute('UPDATE macd_bear_alert_history SET global_mirrored=1 WHERE id=?', (event_id,))

    def start(self, stop):
        def loop():
            while not stop.is_set():
                try:
                    now = datetime.now(self.api.CHINA_TZ)
                    if self.api.is_macd_screener_session(now):
                        if time.monotonic()-self.last_started >= 60:
                            self.scan(now.date().isoformat(), True)
                    else:
                        with self.connect() as db:
                            day = db.execute('SELECT MAX(trade_date) FROM daily_close_quotes WHERE trade_date<=?',(now.date().isoformat(),)).fetchone()[0]
                        if day and day != self.finalized and time.monotonic()-self.last_started >= 60 and (day < now.date().isoformat() or now.hour*60+now.minute>=905):
                            self.scan(day)
                except Exception as exc:
                    self.status['lastError'] = str(exc)
                    logging.exception('MACD空头定时任务失败')
                stop.wait(10)
        threading.Thread(target=loop, name='macd-bear-scan', daemon=True).start()

    def members(self):
        with self.connect() as db:
            day = db.execute('SELECT MAX(trade_date) FROM macd_bear_scans').fetchone()[0]
            rows = db.execute('SELECT stock_json,scanned_at FROM macd_bear_members WHERE trade_date=?',(day,)).fetchall()
        stocks = [json.loads(r[0]) for r in rows]
        concept_changes = {}
        try:
            # Display reads must not start a network ranking refresh or delay
            # the normal page merely because it shows this menu's count.
            with self.connect() as db:
                rank = db.execute("SELECT payload_json FROM concept_rank_cache WHERE cache_key='concept-all'").fetchone()
            concept_changes = {str(c.get('code')): c.get('changePct') for c in (json.loads(rank[0]).get('concepts', []) if rank else [])}
        except Exception:
            logging.warning('MACD空头概念涨幅暂不可用', exc_info=True)
        for s in stocks:
            s['nameInitials'] = self.api.stock_name_initials(s['name'])
            s['sectorChangePct'] = concept_changes.get(self.api.concept_board_code_for_stock(s['code'], s.get('industry')))
        return dict(stocks=stocks,tradeDate=day,updatedAt=max((r[1] for r in rows),default=None),
                    membershipUpdatedAt=self.status.get('lastCompletedAt'),source='本地 SQLite MACD空头独立选股池',scanStatus=dict(self.status))

    def history(self, query):
        a = self.api
        get = lambda key,default='': query.get(key,[default])[0]
        with self.connect() as db:
            latest_day = db.execute('SELECT MAX(trade_date) FROM macd_bear_scans').fetchone()[0]
        day = get('date') or latest_day or datetime.now(a.CHINA_TZ).date().isoformat()
        datetime.strptime(day,'%Y-%m-%d')
        with self.connect() as db:
            events = [dict(json.loads(raw),id=str(i)) for i,raw in db.execute('SELECT id,event_json FROM macd_bear_alert_history WHERE trade_date=? ORDER BY id DESC',(day,))]
            latest = {c:json.loads(raw) for c,raw in db.execute('SELECT code,stock_json FROM macd_bear_members WHERE trade_date=?',(day,))}
            latest.update({c:json.loads(raw) for c,raw in db.execute('SELECT code,quote_json FROM macd_bear_quotes WHERE trade_date=?',(day,))})
            if not a.is_intraday_alert_session(datetime.now(a.CHINA_TZ)):
                latest.update({c:json.loads(raw) for c,raw in db.execute('SELECT code,quote_json FROM daily_close_quotes WHERE trade_date=?',(day,))})
        needle=get('q').lower().strip(); codes=set(filter(None,get('codes').split(','))); market=get('market','all')
        filtered=[]
        for e in events:
            if needle and not any(needle in str(v).lower() for v in (e['code'],e['name'],a.stock_name_initials(e['name']))): continue
            if codes and e['code'] not in codes: continue
            if market in ('main','chinext','starBse') and e['board'] != {'main':'main','chinext':'growth','starBse':'starBse'}[market]: continue
            if market=='star' and not e['code'].startswith(('688','689')): continue
            if market=='bse' and not e['code'].startswith(('8','4','92')): continue
            q=latest.get(e['code'],{})
            e.update(closePrice=q.get('price'),closeChangePct=q.get('changePct'))
            if get('includeDrawdowns')!='1' and e['closeChangePct'] is not None and e['closeChangePct'] < -1: continue
            filtered.append(e)
        if get('dedupe')=='earliest':
            unique={e['code']:e for e in filtered};filtered=sorted(unique.values(),key=lambda e:(e['time'],int(e['id'])),reverse=True)
        counts={a.macd_selection_rule_label(r):sum(e['signal']==a.macd_selection_rule_label(r) for e in filtered) for r in RULES}
        if get('signalType') not in ('','all'):filtered=[e for e in filtered if e['signal']==get('signalType')]
        directions=dict(up=0,down=0,flat=0)
        for e in filtered:
            p=e.get('closePrice');e['_direction']='' if p is None else 'up' if p>e['price'] else 'down' if p<e['price'] else 'flat'
            if e['_direction']:directions[e['_direction']]+=1
        if get('direction') not in ('','all'):filtered=[e for e in filtered if e['_direction']==get('direction')]
        key=get('sort');desc=get('sortDir','desc')=='desc'
        if key=='alertTime':filtered.sort(key=lambda e:e['time'],reverse=desc)
        elif key in ('changePct','closePrice','closeChangePct','changeDelta'):
            def sortval(e):
                v=(e['closeChangePct']-e['changePct'] if e['closeChangePct'] is not None else None) if key=='changeDelta' else e.get(key)
                return (v is None,(-v if desc else v) if v is not None else 0)
            filtered.sort(key=sortval)
        total=len(filtered);page=max(1,int(get('page','1')));size=max(20,min(100,int(get('pageSize','50'))))
        return dict(tradeDate=day,rows=filtered[(page-1)*size:page*size],total=total,page=page,pageSize=size,pages=math.ceil(total/size),signalCounts=counts,directionCounts=directions)


def handle(service, handler, path, query):
    if path == '/macd-bear':
        html = service.api.HTML_FILE.read_text(encoding='utf-8')
        html = html.replace('/api/macd-screener','/api/macd-bear/members').replace('/api/market-alert-history','/api/macd-bear/history')
        html = html.replace('MACD选股','MACD空头')
        # Specialize the new document only; the original template/logic stays intact.
        options = [[r, service.api.macd_selection_rule_label(r)] for r in RULES]
        html = re.sub(r'const MACD_SELECTION_RULES = \[.*?\n    \];',
                      'const MACD_SELECTION_RULES = '+json.dumps(options,ensure_ascii=False)+';', html, count=1, flags=re.S)
        html = re.sub(r'(function syncMarketAlertSignalOptions\(counts = \{\}\) \{\s*)const labels = \{.*?\n      \};',
                      lambda match: match[1]+'const labels = '+json.dumps(dict((label,label) for _,label in options),ensure_ascii=False)+';',
                      html,count=1,flags=re.S)
        body=html.encode('utf-8')
        handler.send_response(200);handler.send_header('Content-Type','text/html; charset=utf-8');handler.send_header('Cache-Control','no-store');handler.send_header('Content-Length',str(len(body)));handler.end_headers();handler.wfile.write(body)
        return True
    if path == '/macd-bear-nav.js':
        body=(service.api.BASE_DIR/'macd-bear-nav.js').read_bytes()
        handler.send_response(200);handler.send_header('Content-Type','application/javascript');handler.send_header('Cache-Control','no-store');handler.send_header('Content-Length',str(len(body)));handler.end_headers();handler.wfile.write(body)
        return True
    if path not in ('/api/macd-bear/members','/api/macd-bear/history'):
        return False
    try:
        handler.send_json(200,service.members() if path.endswith('/members') else service.history(query))
    except (ValueError,sqlite3.Error,KeyError) as exc:
        handler.send_json(502,{'error':str(exc)})
    return True
