#!/usr/bin/env python3
import json, sqlite3
from collections import defaultdict
from pathlib import Path

DB = Path(__file__).resolve().parents[1] / "data" / "backtest.sqlite"
WEIGHTS = {"balanced": (.10,.10), "growth": (.12,.13), "value": (.06,.06), "event": (.10,.12)}
con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row
concepts = {}
for row in con.execute("select code,concepts_json from stock_concept_cache"):
    try: concepts[row["code"]] = [x["name"] for x in json.loads(row["concepts_json"])[:5]]
    except Exception: concepts[row["code"]] = []
alerts = con.execute("""with a as (
 select code,name,alert_time,price,change_pct,score,
 row_number() over(partition by code order by alert_time,id) rn
 from alert_events where trade_date='2026-09-10' and alert_kind='score'
 and (code like '300%' or code like '688%'))
 select * from a where rn=1 order by alert_time""").fetchall()
last = {r["code"]: r for r in con.execute("""with z as (
 select code,price,change_pct,row_number() over(partition by code order by minute desc) rn
 from score_traces where trade_date='2026-09-10') select * from z where rn=1""")}
by_minute = defaultdict(list)
for a in alerts: by_minute[a["alert_time"][:5]].append(a)
results=[]
for minute, targets in by_minute.items():
    snaps = list(con.execute("select code,change_pct,net_inflow,base_json from score_traces where trade_date='2026-09-10' and minute=?",(minute,)))
    agg=defaultdict(lambda:[0,0,0,0.0,0.0,0.0])
    snap_by={r["code"]:r for r in snaps}
    for s in snaps:
        try: amount=float(json.loads(s["base_json"] or "{}").get("_amount") or 0)
        except Exception: amount=0
        ch=float(s["change_pct"] or 0); flow=float(s["net_inflow"] or 0)
        for c in concepts.get(s["code"],[]):
            x=agg[c]; x[0]+=1; x[1]+=ch>0; x[2]+=ch>=2; x[3]+=ch; x[4]+=flow; x[5]+=amount
    boards=[]
    for name,x in agg.items():
        if x[0]<3: continue
        avg=x[3]/x[0]; breadth=x[1]/x[0]*100; fr=x[4]/x[5]*100 if x[5] else 0
        heat=max(0,min(100,50+avg*10+(breadth-50)*.4+min(x[2],10)*1.5+(6 if x[4]>0 else -6)))
        peer=max(0,min(100,25+breadth*.55+min(x[2],10)*3))
        boards.append((avg,name,breadth,x[2],x[4],heat,peer,x[0]))
    boards.sort(reverse=True); top={x[1]:x for x in boards[:24]}
    for a in targets:
        s=snap_by.get(a["code"]); related=[top[c] for c in concepts.get(a["code"],[]) if c in top]
        board=max(related,key=lambda x:x[5]) if related else None
        confirmed=bool(board and board[0]>0 and board[2]>=55 and board[3]>=3 and board[4]>=0)
        old={}
        if s:
            try: old=json.loads(s["base_json"] or "{}").get("_strategyScores") or {}
            except Exception: pass
        scores={}
        for mode,(wh,wp) in WEIGHTS.items():
            old_score=float(old.get(mode,a["score"] or 0)); base=(old_score-7.5)/.85
            adjusted=round(old_score+(wh*((board[5] if board else 40)-base))+(wp*((board[6] if board else 40)-base)))
            if not confirmed and mode!="value": adjusted=min(69,adjusted)
            scores[mode]=adjusted
        passed=sum(v>=70 for v in scores.values())>=2
        close=last.get(a["code"]); ret=(float(close["price"])/float(a["price"])*100-100) if close and a["price"] else None
        results.append({"code":a["code"],"name":a["name"],"time":a["alert_time"],"alertChange":a["change_pct"],"closeChange":close["change_pct"] if close else None,"return":round(ret,2) if ret is not None else None,"board":board[1] if board else "无热门概念匹配","boardChange":round(board[0],2) if board else None,"breadth":round(board[2],1) if board else None,"strong":board[3] if board else 0,"boardFlow":round(board[4],2) if board else None,"scores":scores,"passed":passed})
print(json.dumps(results,ensure_ascii=False))
