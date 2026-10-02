# 세션 녹화 결과 대조: cells (symbol, bar, n, volume) vs 1m 캔들 (하루치, before 페이지네이션). 시세 값은 출력하지 않는다.
import sys, os, json, glob, time, collections, urllib.request, urllib.parse; sys.path.insert(0, os.path.dirname(__file__))
from common import get, ROOT
from datetime import datetime, timedelta
P = sys.argv[1]  # session prefix, e.g. data/session_20260930T230404Z
cells = [json.loads(l) for l in open(P + '_cells.jsonl')]
mins = [json.loads(l) for l in open(P + '_minutes.jsonl')]
events = open(P + '_events.log').read()
syms = sorted({c['symbol'] for c in cells}); day = cells[0]['bar'][:10]
bars_by = collections.defaultdict(dict)
for c in cells: bars_by[c['symbol']][c['bar']] = c
# 완결 창: 첫 메시지 분 + 2 ~ 마지막 메시지 분 - 1 (KST 문자열 비교)
first = min(c['bar'] for c in cells); last = max(c['bar'] for c in cells)
lo = (datetime.fromisoformat(first) + timedelta(minutes=2)).isoformat(); hi = (datetime.fromisoformat(last) - timedelta(minutes=1)).isoformat()
tot = ex = 0; nc = 0; exact = 0; worst = None; missing_syms = 0; t0 = time.time(); reqs = 0; e429 = 0
for s in syms:
    before = f'{day}T20:00:00+09:00'; cand = {}
    for _ in range(4):
        r, h = get('/api/v1/candles', {'symbol': s, 'interval': '1m', 'count': 200, 'before': before, 'adjusted': 'false'}); reqs += 1
        cs = (r.get('result') or {}).get('candles', []); nb = (r.get('result') or {}).get('nextBefore')
        for b in cs:
            if b['timestamp'].startswith(day): cand[datetime.fromisoformat(b['timestamp']).isoformat()] = float(b['volume'])
        if not nb or not nb.startswith(day) or len(cs) < 200: break
        before = nb
        time.sleep(0.06)
    if not cand: missing_syms += 1; continue
    for bar, v in cand.items():
        if not (lo <= bar <= hi): continue
        if v == 0 and (s, bar) not in [(c['symbol'], c['bar']) for c in []]: pass
        ws_v = bars_by[s].get(bar, {}).get('volume', 0.0)
        if v == 0 and ws_v == 0: continue     # 체결 없는 분은 대조 대상 아님
        nc += 1; tot += ws_v; ex += v; same = abs(ws_v - v) < 0.5; exact += same
        if v and (worst is None or ws_v / v < worst[0]): worst = (round(ws_v / v, 4), s, bar)
lag = [m for m in mins if m['lag_p95'] is not None]; msgs = [m['msgs'] for m in mins]
print(f"symbols={len(syms)} window={lo[11:16]}~{hi[11:16]} KST cells={nc} exact={exact} ws_sum/candle_sum={tot:.0f}/{ex:.0f} = {100*tot/ex if ex else 0:.3f}% worst={worst} candle_fetch_reqs={reqs} sym_no_candles={missing_syms} elapsed={time.time()-t0:.0f}s")
print(f"minutes={len(mins)} msgs/min p50={sorted(msgs)[len(msgs)//2] if msgs else 0} max={max(msgs) if msgs else 0} total={sum(msgs)} | lag_p95 median over minutes={sorted(m['lag_p95'] for m in lag)[len(lag)//2] if lag else None} max lag_max={max(m['lag_max'] for m in lag) if lag else None}")
print("events:", events.count('reconnect #'), "reconnects |", events.count('180s no frame'), "idle-breaks |", events.count('error frame'), "error frames")
print("reconnects final:", mins[-1]['reconnects'] if mins else None)
