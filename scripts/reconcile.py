# 4) 대조: 웹소켓 체결을 (종목, 분) 으로 합친 거래량 vs 1분봉 volume. 봉 규칙(FAQ): 1m 의 timestamp 는 봉 종료 시각이라
#    09:00:00~09:00:59.999 체결은 09:01 봉. 즉 체결 timestamp 의 분을 +1 해서 봉과 맞춘다. 완결된 분만 센다(녹화 시작·끝 분 제외).
import json, sys, os
from collections import defaultdict
from datetime import datetime, timedelta
ws_path, candle_path = sys.argv[1], sys.argv[2]
vol = defaultdict(int); tmin = {}; tmax = {}
for ln in open(ws_path):
    o = json.loads(ln); fr = json.loads(o['raw'])
    if fr.get('type') != 'message': continue
    sym = fr['topic'].split(':')[-1]; d = fr['data']
    ts = datetime.fromisoformat(d['timestamp']); bar = (ts.replace(second=0, microsecond=0) + timedelta(minutes=1)).isoformat()
    vol[(sym, bar)] += int(float(d['volume']))
    tmin[sym] = min(tmin.get(sym, ts), ts); tmax[sym] = max(tmax.get(sym, ts), ts)
cand = {}
for ln in open(candle_path):
    c = json.loads(ln); cand[(c['symbol'], datetime.fromisoformat(c['timestamp']).isoformat())] = int(float(c['volume']))
cells = 0; exact = 0; ws_sum = 0; ex_sum = 0; worst = None
for (sym, bar), v in sorted(vol.items()):
    b = datetime.fromisoformat(bar)
    if sym not in tmin or b - timedelta(minutes=1) <= tmin[sym].replace(second=0, microsecond=0) or b > tmax[sym].replace(second=0, microsecond=0):
        continue  # 첫·마지막 분은 부분 녹화라 제외
    if (sym, bar) not in cand: continue
    e = cand[(sym, bar)]; cells += 1; ws_sum += v; ex_sum += e; exact += (v == e)
    if e and (worst is None or v / e < worst[0]): worst = (v / e, sym, bar, v, e)
print(f'cells={cells} exact={exact} ws_sum/candle_sum={ws_sum}/{ex_sum} = {100*ws_sum/ex_sum if ex_sum else 0:.3f}%')
print('worst cell (ratio, symbol, bar):', (round(worst[0], 4), worst[1], worst[2]) if worst else None)  # 종목 시세 값은 출력하지 않는다
