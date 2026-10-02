# 랭킹(거래량 상위, 당일)으로 실시간 계층에 넣을 심볼 N개를 고른다. 시세 값은 출력하지 않는다.
import sys, os; sys.path.insert(0, os.path.dirname(__file__))
from common import get
country = sys.argv[1] if len(sys.argv) > 1 else 'US'; n = int(sys.argv[2]) if len(sys.argv) > 2 else 100
r, h = get('/api/v1/rankings', {'type': 'MARKET_TRADING_VOLUME', 'marketCountry': country, 'duration': '1d', 'count': n})
syms = [x['symbol'] for x in (r.get('result') or {}).get('rankings', [])]
print(','.join(syms))
print(f"# rankedAt={(r.get('result') or {}).get('rankedAt')} n={len(syms)} remaining={h.get('X-RateLimit-Remaining')}", file=sys.stderr)
