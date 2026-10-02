# 2) 웹소켓 체결 녹화: trade:kr 을 N개 종목으로 M분 받아 data/ws_trades_<UTC>.jsonl 에 수신시각과 프레임을 그대로 쓴다.
#    LOSSY 채널에 시퀀스가 없으므로 유실은 나중에 1분봉과 대조해서만 알 수 있다(reconcile.py).
#    실행: docker run --rm --network host -v ~/tossinvest-lab:/lab -w /lab --entrypoint python cdc-realtime-pipeline-binance-collector:latest scripts/ws_record.py 005930,000660 10
import asyncio, json, os, sys, time
sys.path.insert(0, os.path.dirname(__file__))
from common import get_token, ROOT
import websockets

WS = 'wss://openapi-ws.tossinvest.com/ws/v1'
symbols = sys.argv[1].split(','); minutes = float(sys.argv[2]) if len(sys.argv) > 2 else 5
out = os.path.join(ROOT, 'data', f"ws_trades_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.jsonl")


async def main():
    deadline = time.time() + minutes * 60; n = 0; reconnects = 0; last_report = time.time()
    with open(out, 'w') as f:
        while time.time() < deadline:
            tok = get_token()
            try:
                async with websockets.connect(WS, additional_headers={'Authorization': f'Bearer {tok}'}, ping_interval=None, max_size=2 ** 22) as ws:
                    # 시장은 심볼로 판별: 6자리 종목코드(숫자 시작)는 kr, 그 외는 us. 섞어 보내도 채널별로 나눠 선언한다
                    kr = [s for s in symbols if s[:1].isdigit()]; us = [s for s in symbols if not s[:1].isdigit()]
                    decl = [{'id': 'req-1'}] + ([{'type': 'trade:kr', 'codes': kr}] if kr else []) + ([{'type': 'trade:us', 'codes': us}] if us else [])
                    await ws.send(json.dumps(decl))
                    ack = json.loads(await ws.recv()); print(time.strftime('%H:%M:%S'), 'ack', ack)
                    next_ping = time.time() + 60
                    while time.time() < deadline:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            raw = None
                        now = time.time()
                        if raw is not None:
                            f.write(json.dumps({'recv_ts': now, 'raw': raw}) + '\n'); n += 1
                        if now >= next_ping:
                            await ws.send('PING'); next_ping = now + 60
                        if now - last_report >= 60:
                            print(time.strftime('%H:%M:%S'), f'frames={n} reconnects={reconnects}'); last_report = now; f.flush()
            except Exception as e:  # noqa: BLE001
                reconnects += 1; print(time.strftime('%H:%M:%S'), 'reconnect', reconnects, repr(e)[:120]); await asyncio.sleep(1)
    print('done frames', n, 'reconnects', reconnects, '->', out)

asyncio.run(main())
