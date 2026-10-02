# 하루 세션 녹화 (docs/02 §3 마지막 미측정 둘): 연결 2개 × 구독 100 = 200 종목, 지정 시각까지. 원문은 저장하지 않고 분 단위로 집계한다.
#   - data/session_<UTC>_minutes.jsonl : 분마다 {minute, msgs, conn별 msgs, lag p50/p95/max, reconnects 누적}
#   - data/session_<UTC>_cells.jsonl   : 끝날 때 (symbol, bar) 별 {n, volume}  (bar = 체결 분 + 1 = 1m 캔들의 timestamp)
#   - data/session_<UTC>_events.log    : 연결 열림·끊김·재연결·ack 거부
# 실행: docker run --rm --network host -e PYTHONUNBUFFERED=1 -v ~/tossinvest-lab:/lab -w /lab --entrypoint python cdc-realtime-pipeline-binance-collector:latest scripts/ws_session.py <symbols.csv> <end UTC HH:MM>
import asyncio, json, os, sys, time, collections
sys.path.insert(0, os.path.dirname(__file__))
from common import get_token, ROOT
import websockets
from datetime import datetime, timedelta, timezone

WS = 'wss://openapi-ws.tossinvest.com/ws/v1'
symbols = [s for s in open(sys.argv[1]).read().replace('\n', ',').split(',') if s]
end_hm = sys.argv[2]
now_utc = datetime.now(timezone.utc); end_at = now_utc.replace(hour=int(end_hm[:2]), minute=int(end_hm[3:]), second=0, microsecond=0)
if end_at <= now_utc: end_at += timedelta(days=1)
TS = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()); P = os.path.join(ROOT, 'data', f'session_{TS}')
mins = open(P + '_minutes.jsonl', 'a'); ev = open(P + '_events.log', 'a')
cells = collections.defaultdict(lambda: [0, 0.0]); per_min = collections.defaultdict(lambda: {'msgs': 0, 'lag': [], 'conn': collections.Counter()})
state = {'reconnects': collections.Counter(), 'frames': 0}
def log(msg): ev.write(f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} {msg}\n"); ev.flush()

async def conn(idx, codes):
    chunk = [s for s in codes if s[:1].isdigit()]
    while datetime.now(timezone.utc) < end_at:
        try:
            async with websockets.connect(WS, additional_headers={'Authorization': f'Bearer {get_token()}'}, ping_interval=None, max_size=2 ** 22) as ws:
                await ws.send(json.dumps([{'id': f'req-{idx}'}, {'type': 'trade:kr', 'codes': chunk}]))
                ack = json.loads(await ws.recv()); log(f"conn#{idx} ack subscribed={len(ack.get('subscribed', []))} rejected={ack.get('rejected')}")
                next_ping = time.time() + 60; idle = 0
                while datetime.now(timezone.utc) < end_at:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        raw = None; idle += 5
                    now = time.time()
                    if raw is not None:
                        idle = 0; fr = json.loads(raw)
                        if fr.get('type') == 'message':
                            d = fr['data']; ts = datetime.fromisoformat(d['timestamp']); sym = fr['topic'].split(':')[-1]
                            bar = (ts.replace(second=0, microsecond=0) + timedelta(minutes=1)).isoformat()
                            c = cells[(sym, bar)]; c[0] += 1; c[1] += float(d['volume'])
                            m = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M'); pm = per_min[m]; pm['msgs'] += 1; pm['conn'][idx] += 1; pm['lag'].append(now - ts.timestamp())
                            state['frames'] += 1
                        elif fr.get('type') == 'error': log(f"conn#{idx} error frame {fr}")
                    if now >= next_ping: await ws.send('PING'); next_ping = now + 60
                    if idle >= 180: log(f"conn#{idx} 180s no frame - reconnect"); break
        except Exception as e:  # noqa: BLE001
            state['reconnects'][idx] += 1; log(f"conn#{idx} reconnect #{state['reconnects'][idx]} {type(e).__name__}: {str(e)[:100]}"); await asyncio.sleep(1)

async def flusher():
    last = None
    while datetime.now(timezone.utc) < end_at + timedelta(seconds=5):
        await asyncio.sleep(10)
        cur = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M')
        for m in sorted(k for k in per_min if k < cur):
            pm = per_min.pop(m); lag = sorted(pm['lag'])
            mins.write(json.dumps({'minute': m, 'msgs': pm['msgs'], 'conn0': pm['conn'][0], 'conn1': pm['conn'][1],
                                   'lag_p50': round(lag[len(lag)//2], 3) if lag else None, 'lag_p95': round(lag[int(len(lag)*.95)], 3) if lag else None,
                                   'lag_max': round(lag[-1], 3) if lag else None, 'reconnects': dict(state['reconnects'])}) + '\n'); mins.flush()

async def main():
    log(f"start symbols={len(symbols)} end={end_at.isoformat()}")
    await asyncio.gather(conn(0, symbols[:100]), conn(1, symbols[100:200]), flusher())
    with open(P + '_cells.jsonl', 'w') as f:
        for (sym, bar), (n, v) in sorted(cells.items()): f.write(json.dumps({'symbol': sym, 'bar': bar, 'n': n, 'volume': v}) + '\n')
    log(f"done frames={state['frames']} cells={len(cells)} reconnects={dict(state['reconnects'])}"); print('DONE', P)

asyncio.run(main())
