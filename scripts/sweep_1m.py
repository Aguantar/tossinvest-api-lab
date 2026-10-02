# 전 종목 순회 실측 (docs/02 §3): 초당 20(버킷)을 넘지 않게 간격을 두고 병렬로 캔들을 받는다.
#   mode=1m  : 지정일 국내 1분봉 720개 = 종목당 4요청(200개씩 before 페이지네이션). 순회 시간·429·0봉 종목 수를 잰다
#   mode=adj : 일봉 60개를 adjusted=true/false 두 벌 받아 종가가 다른 날이 있는 종목 = 최근 기업행위. 종목당 2요청
# 원문 봉은 저장하지 않는다(약관·용량). 종목별 요약만 data/ 에 남긴다.
import sys, os, json, time, threading, queue, urllib.request, urllib.error, urllib.parse; sys.path.insert(0, os.path.dirname(__file__))
from common import get_token, BASE, ROOT
mode = sys.argv[1]; uni = sys.argv[2]; day = sys.argv[3] if len(sys.argv) > 3 else '2026-09-23'
RATE = 20.0; THREADS = 6
U = json.load(open(uni)); symbols = [x['symbol'] for m in ('KOSPI', 'KOSDAQ') for x in U[m]]
tok = get_token(); lock = threading.Lock(); next_slot = [time.time()]; stats = {'req': 0, 'ok': 0, 'e429': 0, 'err': 0, 'lat': []}
def slot():
    with lock:
        t = max(next_slot[0], time.time()); next_slot[0] = t + 1.0 / RATE
    d = t - time.time()
    if d > 0: time.sleep(d)
def req(params):
    slot(); url = BASE + '/api/v1/candles?' + urllib.parse.urlencode(params); t0 = time.time()
    for attempt in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={'Authorization': f'Bearer {tok}'}), timeout=15) as r:
                d = json.load(r)
            with lock: stats['req'] += 1; stats['ok'] += 1; stats['lat'].append(time.time() - t0)
            return (d.get('result') or {}).get('candles', []), (d.get('result') or {}).get('nextBefore')
        except urllib.error.HTTPError as e:
            with lock: stats['req'] += 1
            if e.code == 429:
                with lock: stats['e429'] += 1
                time.sleep(float(e.headers.get('Retry-After', '1'))); slot(); continue
            with lock: stats['err'] += 1
            return None, None
        except Exception:
            with lock: stats['err'] += 1
            return None, None
    return None, None
out = []; q = queue.Queue()
for s in symbols: q.put(s)
def worker():
    while True:
        try: s = q.get_nowait()
        except queue.Empty: return
        if mode == '1m':
            before = f'{day}T20:00:00+09:00'; bars = []; pages = 0
            while pages < 4:
                c, nb = req({'symbol': s, 'interval': '1m', 'count': 200, 'before': before, 'adjusted': 'false'}); pages += 1
                if c is None: bars = None; break
                c = [b for b in c if b['timestamp'].startswith(day)]; bars += c
                if not nb or not nb.startswith(day) or len(c) < 200: break
                before = nb
            out.append({'symbol': s, 'bars': len(bars) if bars is not None else -1, 'pages': pages, 'zero_vol': sum(1 for b in (bars or []) if float(b['volume']) == 0)})
        else:
            a, _ = req({'symbol': s, 'interval': '1d', 'count': 60, 'adjusted': 'true'}); b, _ = req({'symbol': s, 'interval': '1d', 'count': 60, 'adjusted': 'false'})
            if a is None or b is None: out.append({'symbol': s, 'days': -1}); continue
            ta = {x['timestamp'][:10]: x for x in a}; tb = {x['timestamp'][:10]: x for x in b}
            diff = sorted(d for d in ta if d in tb and (ta[d]['closePrice'] != tb[d]['closePrice'] or ta[d]['volume'] != tb[d]['volume']))
            out.append({'symbol': s, 'days': len(ta), 'diff_days': len(diff), 'first_diff': diff[0] if diff else None, 'last_diff': diff[-1] if diff else None})
t0 = time.time(); ths = [threading.Thread(target=worker) for _ in range(THREADS)]
for t in ths: t.start()
last = 0
while any(t.is_alive() for t in ths):
    time.sleep(30)
    if time.time() - last >= 60:
        last = time.time(); print(f"{time.strftime('%H:%M:%S')} done={len(out)}/{len(symbols)} req={stats['req']} 429={stats['e429']} err={stats['err']} rate={stats['req']/(time.time()-t0):.1f}/s", flush=True)
el = time.time() - t0; lat = sorted(stats['lat'])
p = os.path.join(ROOT, 'data', f"sweep_{mode}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.jsonl")
with open(p, 'w') as f:
    for r in out: f.write(json.dumps(r) + '\n')
print(f"DONE mode={mode} symbols={len(symbols)} elapsed={el:.0f}s req={stats['req']} ok={stats['ok']} 429={stats['e429']} err={stats['err']} rate={stats['req']/el:.1f}/s lat_p50={lat[len(lat)//2]:.3f} p95={lat[int(len(lat)*0.95)]:.3f} -> {p}")
if mode == '1m':
    bars = [r['bars'] for r in out]; print(f"bars: 720={sum(1 for b in bars if b==720)} 0={sum(1 for b in bars if b==0)} other={sum(1 for b in bars if b not in (0,720,-1))} fail={sum(1 for b in bars if b==-1)} total_bars={sum(b for b in bars if b>0)}")
else:
    d = [r for r in out if r.get('diff_days', 0) > 0]; print(f"adjusted!=raw 종목: {len(d)} / {len(out)}; 예: {[(r['symbol'], r['diff_days'], r['first_diff']) for r in d[:8]]}")
