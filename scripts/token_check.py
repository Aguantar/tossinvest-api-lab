# 1) 토큰 발급 + 토큰만 필요한 조회 3개로 접속 확인. 시세 값은 출력하지 않는다(약관: 데이터는 로컬에만).
import sys, os; sys.path.insert(0, os.path.dirname(__file__))
from common import get_token, get
tok = get_token(force=True)
print('token ok, len', len(tok))
cal, h = get('/api/v1/market-calendar/KR'); print('calendar/KR: keys', list((cal.get('result') or {}).keys())[:8], 'ratelimit', h)
st, h = get('/api/v1/stocks', {'symbols': '005930,000660'}); res = st.get('result'); print('stocks: n', len(res) if isinstance(res, list) else type(res).__name__, 'ratelimit', h)
c, h = get('/api/v1/candles', {'symbol': '005930', 'interval': '1m', 'count': 3}); cs = (c.get('result') or {}).get('candles', [])
print('candles 1m: n', len(cs), 'latest ts', cs[0]['timestamp'] if cs else None, 'ratelimit', h)
