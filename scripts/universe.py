# 유니버스: /stocks/all 을 시장별로 받아 data/universe_<UTC>.json 에 저장하고 크기·유형 분포를 출력한다. STOCK_ALL 그룹은 초당 1회.
import sys, os, json, time, collections; sys.path.insert(0, os.path.dirname(__file__))
from common import get, ROOT
out = {}
for mk in ['KOSPI', 'KOSDAQ', 'KR_ETC', 'NYSE', 'NASDAQ', 'AMEX', 'US_ETC']:
    r, h = get('/api/v1/stocks/all', {'market': mk}); rows = r.get('result') or []
    out[mk] = rows; time.sleep(1.1)
    st = collections.Counter(x.get('securityType') for x in rows); cs = sum(1 for x in rows if x.get('isCommonShare'))
    print(f"{mk:7s} {len(rows):5d}  commonShare={cs:5d}  {dict(st.most_common(5))}")
p = os.path.join(ROOT, 'data', f"universe_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json"); json.dump(out, open(p, 'w'), ensure_ascii=False)
kr = sum(len(out[m]) for m in ['KOSPI', 'KOSDAQ', 'KR_ETC']); us = sum(len(out[m]) for m in ['NYSE', 'NASDAQ', 'AMEX', 'US_ETC'])
print(f"KR total={kr} US total={us} ->", p)
