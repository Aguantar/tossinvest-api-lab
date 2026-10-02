# 3) 1분봉 조회: 종목별 최근 count 개를 data/candles_<UTC>.jsonl 에. MARKET_DATA_CHART 초당 20회 안에서 종목당 1회.
import json, os, sys, time; sys.path.insert(0, os.path.dirname(__file__))
from common import get, ROOT
symbols = sys.argv[1].split(','); count = int(sys.argv[2]) if len(sys.argv) > 2 else 60
out = os.path.join(ROOT, 'data', f"candles_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.jsonl")
with open(out, 'w') as f:
    for s in symbols:
        c, h = get('/api/v1/candles', {'symbol': s, 'interval': '1m', 'count': count, 'adjusted': 'false'})
        for k in (c.get('result') or {}).get('candles', []):
            f.write(json.dumps({'symbol': s, **k}) + '\n')
        print(s, 'candles', len((c.get('result') or {}).get('candles', [])), 'remaining', h.get('X-RateLimit-Remaining')); time.sleep(0.1)
print('->', out)
