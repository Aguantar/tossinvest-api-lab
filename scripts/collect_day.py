"""하루치 원천 수집 (2026-10-14, 1주차). 장 마감 뒤 한 번 돌아 그날의 국내 전 종목 1분봉, 일봉 두 벌(수정주가 적용·미적용) 60일, 종목 마스터, 달력을
Parquet(정본)과 Postgres raw 스키마(적재본)에 넣는다. 같은 날을 다시 돌리면 그 날짜 행만 지우고 다시 넣는다(멱등).

왜 이렇게:
  - 장 마감 뒤 1회: 1분봉은 REST 로 완결된 봉을 받으므로 실시간으로 붙어 있을 이유가 없고, 진행 중인 봉은 값이 바뀔 수 있다(FAQ). 순회 12분, 일봉 7.5분.
  - 초당 20 아래로 보내도 버킷 경계에서 0.8% 는 429 가 난다(docs/03). Retry-After 를 지키고 횟수를 manifest 에 남긴다.
  - 봉 수의 정답은 종목마다 다르다(720/660/390, docs/03 §3). 그래서 "빠짐없음" 은 여기서 판정하지 않고 종목별 첫·마지막 봉과 봉 수를 manifest 에 남겨 dbt 가 판정한다.
  - 달력이 거래일이 아니라고 하면 아무것도 받지 않고 run_log 에 skipped 로 남긴다. "안 오는 게 정상" 의 근거.
  - Parquet 이 정본, Postgres 는 적재본. Postgres 가 깨져도 Parquet 에서 다시 넣는다.
  - 시세 값은 로그에 찍지 않는다(약관). 로그에는 건수·시간·오류만.

실행: venv/bin/python scripts/collect_day.py [--date YYYY-MM-DD] [--limit N] [--skip-1d]
  기본 날짜: 지금이 20:05 KST 이후면 오늘, 아니면 달력의 직전 거래일.
환경: PGHOST(기본 10.88.0.1) PGPORT(5433) PGUSER(toss) PGDATABASE(toss) PGPASS_FILE(<ROOT>/.pgpass_toss) PARQUET_ROOT(<ROOT>/data/parquet)
"""
import argparse, csv, io, json, os, queue, sys, threading, time, urllib.error, urllib.parse, urllib.request, uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import BASE, ROOT, get, get_token

import pyarrow as pa
import pyarrow.parquet as pq
import psycopg

KST = timezone(timedelta(hours=9))
RATE, THREADS = 20.0, 6
PARQUET_ROOT = os.environ.get('PARQUET_ROOT', os.path.join(ROOT, 'data', 'parquet'))


def log(msg):
    print(f"{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z {msg}", flush=True)


# ── Postgres ───────────────────────────────────────────────────────────────
def pg_connect():
    pw = open(os.environ.get('PGPASS_FILE', os.path.join(ROOT, '.pgpass_toss'))).read().strip()
    return psycopg.connect(host=os.environ.get('PGHOST', '10.88.0.1'), port=int(os.environ.get('PGPORT', '5433')),
                           user=os.environ.get('PGUSER', 'toss'), dbname=os.environ.get('PGDATABASE', 'toss'), password=pw, connect_timeout=15)


def pg_replace(conn, table, key_col, key_val, columns, rows):
    """key_col = key_val 인 행을 지우고 rows 를 COPY 로 넣는다. 한 트랜잭션."""
    buf = io.StringIO(); w = csv.writer(buf)
    for r in rows: w.writerow(['' if v is None else v for v in r])
    buf.seek(0)
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table} WHERE {key_col} = %s", (key_val,))
        with cur.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '')") as cp:
            cp.write(buf.read())
    conn.commit()


# ── API (속도 제한 공유) ───────────────────────────────────────────────────
class Api:
    def __init__(self):
        self.tok = get_token(); self.lock = threading.Lock(); self.next_slot = time.time()
        self.stats = {'req': 0, 'e429': 0, 'err': 0}

    def _slot(self):
        with self.lock:
            t = max(self.next_slot, time.time()); self.next_slot = t + 1.0 / RATE
        d = t - time.time()
        if d > 0: time.sleep(d)

    def candles(self, params):
        """(candles, nextBefore, retries_429) 또는 (None, None, retries) 실패."""
        url = BASE + '/api/v1/candles?' + urllib.parse.urlencode(params); r429 = 0
        for attempt in range(5):
            self._slot()
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers={'Authorization': f'Bearer {self.tok}'}), timeout=20) as r:
                    d = json.load(r)
                with self.lock: self.stats['req'] += 1
                res = d.get('result') or {}
                return res.get('candles', []), res.get('nextBefore'), r429
            except urllib.error.HTTPError as e:
                with self.lock: self.stats['req'] += 1
                if e.code == 429:
                    r429 += 1
                    with self.lock: self.stats['e429'] += 1
                    time.sleep(float(e.headers.get('Retry-After', '1'))); continue
                if e.code == 401 and attempt == 0:
                    self.tok = get_token(force=True); continue
                with self.lock: self.stats['err'] += 1
                if e.code >= 500: time.sleep(2 * (attempt + 1)); continue
                return None, None, r429
            except Exception:
                with self.lock: self.stats['err'] += 1
                time.sleep(2 * (attempt + 1))
        return None, None, r429


def run_pool(symbols, fn):
    q = queue.Queue()
    for s in symbols: q.put(s)
    out, lock = [], threading.Lock()

    def worker():
        while True:
            try: s = q.get_nowait()
            except queue.Empty: return
            r = fn(s)
            with lock: out.append(r)
    ths = [threading.Thread(target=worker, daemon=True) for _ in range(THREADS)]
    for t in ths: t.start()
    last = time.time()
    while any(t.is_alive() for t in ths):
        time.sleep(5)
        if time.time() - last >= 60:
            last = time.time(); log(f"  progress {len(out)}/{len(symbols)}")
    return out


# ── 단계 ────────────────────────────────────────────────────────────────────
def resolve_date(arg):
    if arg: return arg
    now = datetime.now(KST)
    if now.hour > 20 or (now.hour == 20 and now.minute >= 5): return now.strftime('%Y-%m-%d')
    r, _ = get('/api/v1/market-calendar/KR', {'date': now.strftime('%Y-%m-%d')})
    return (r.get('result') or {}).get('previousBusinessDay', {}).get('date')


def step_calendar(conn, day):
    r, _ = get('/api/v1/market-calendar/KR', {'date': day})
    today = (r.get('result') or {}).get('today') or {}
    sessions = today.get('integrated'); trading = sessions is not None
    pg_replace(conn, 'raw.market_calendar', 'trade_date', day, ['trade_date', 'is_trading', 'sessions', 'fetched_at'],
               [[day, trading, json.dumps(sessions) if sessions else None, datetime.now(timezone.utc).isoformat()]])
    return trading


def step_universe(conn, day):
    rows, syms = [], []
    now = datetime.now(timezone.utc).isoformat()
    for mk in ('KOSPI', 'KOSDAQ', 'KR_ETC'):
        r, _ = get('/api/v1/stocks/all', {'market': mk}); items = r.get('result') or []
        for x in items:
            rows.append([day, mk, x['symbol'], json.dumps(x, ensure_ascii=False), now]); syms.append(x['symbol'])
        time.sleep(1.1)  # STOCK_ALL 그룹 초당 1회
    pg_replace(conn, 'raw.universe_snapshot', 'snapshot_date', day, ['snapshot_date', 'market', 'symbol', 'payload', 'fetched_at'], rows)
    return sorted(set(syms))


def write_parquet(path, table):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.part'; pq.write_table(table, tmp, compression='zstd'); os.replace(tmp, path)
    return os.path.getsize(path)


def step_1m(conn, api, day, symbols):
    fetched = datetime.now(timezone.utc)

    def one(s):
        before = f'{day}T20:00:00+09:00'; bars = []; pages = 0; r429 = 0; err = None
        while pages < 4:
            c, nb, k = api.candles({'symbol': s, 'interval': '1m', 'count': 200, 'before': before, 'adjusted': 'false'}); pages += 1; r429 += k
            if c is None: err = 'request failed'; break
            c = [b for b in c if b['timestamp'].startswith(day)]; bars += c
            if not nb or not nb.startswith(day) or len(c) < 200: break
            before = nb
        return s, bars, pages, r429, err

    results = run_pool(symbols, one)
    rows, manifest = [], []
    for s, bars, pages, r429, err in results:
        ts_list = sorted(b['timestamp'] for b in bars)
        for b in bars:
            rows.append((day, s, b['timestamp'], b['openPrice'], b['highPrice'], b['lowPrice'], b['closePrice'], b['volume'], b.get('currency'), fetched))
        manifest.append([day, s, len(bars), ts_list[0] if ts_list else None, ts_list[-1] if ts_list else None, pages, r429,
                         'error' if err else ('empty' if not bars else 'ok'), err, fetched.isoformat()])
    # Parquet (정본)
    cols = list(zip(*rows)) if rows else [[] for _ in range(10)]
    table = pa.table({
        'trade_date': pa.array([datetime.strptime(x, '%Y-%m-%d').date() for x in cols[0]], pa.date32()),
        'symbol': pa.array(cols[1], pa.string()),
        'ts': pa.array([datetime.fromisoformat(x) for x in cols[2]], pa.timestamp('ms', tz='Asia/Seoul')),
        'open': pa.array([Decimal(x) for x in cols[3]], pa.decimal128(18, 4)),
        'high': pa.array([Decimal(x) for x in cols[4]], pa.decimal128(18, 4)),
        'low': pa.array([Decimal(x) for x in cols[5]], pa.decimal128(18, 4)),
        'close': pa.array([Decimal(x) for x in cols[6]], pa.decimal128(18, 4)),
        'volume': pa.array([Decimal(x) for x in cols[7]], pa.decimal128(20, 4)),
        'currency': pa.array(cols[8], pa.string()),
        'fetched_at': pa.array(list(cols[9]), pa.timestamp('ms', tz='UTC')),
    })
    size = write_parquet(os.path.join(PARQUET_ROOT, 'candles_1m', f'trade_date={day}', 'part-0.parquet'), table)
    # Postgres (적재본)
    pg_replace(conn, 'raw.candles_1m', 'trade_date', day,
               ['trade_date', 'symbol', 'ts', 'open', 'high', 'low', 'close', 'volume', 'currency', 'fetched_at'],
               [[r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9].isoformat()] for r in rows])
    pg_replace(conn, 'raw.sweep_manifest', 'trade_date', day,
               ['trade_date', 'symbol', 'bars', 'first_ts', 'last_ts', 'requests', 'retries_429', 'status', 'error', 'fetched_at'], manifest)
    from collections import Counter
    prof = Counter(m[2] for m in manifest)
    return {'symbols': len(symbols), 'bars': len(rows), 'parquet_bytes': size,
            'bars_720': prof.get(720, 0), 'bars_660': prof.get(660, 0), 'bars_390': prof.get(390, 0), 'bars_0': prof.get(0, 0),
            'bars_other': sum(v for k, v in prof.items() if k not in (720, 660, 390, 0)),
            'errors': sum(1 for m in manifest if m[7] == 'error'), 'retries_429': sum(m[6] for m in manifest)}


def step_1d(conn, api, day, symbols):
    fetched = datetime.now(timezone.utc)

    def one(s):
        out = []; err = None
        for adj in ('true', 'false'):
            c, _, _ = api.candles({'symbol': s, 'interval': '1d', 'count': 60, 'adjusted': adj})
            if c is None: err = 'request failed'; continue
            for b in c:
                out.append((day, s, b['timestamp'][:10], adj == 'true', b['openPrice'], b['highPrice'], b['lowPrice'], b['closePrice'], b['volume'], fetched))
        return s, out, err

    results = run_pool(symbols, one)
    rows = [r for _, out, _ in results for r in out]; errs = sum(1 for _, _, e in results if e)
    cols = list(zip(*rows)) if rows else [[] for _ in range(10)]
    table = pa.table({
        'snapshot_date': pa.array([datetime.strptime(x, '%Y-%m-%d').date() for x in cols[0]], pa.date32()),
        'symbol': pa.array(cols[1], pa.string()),
        'trade_date': pa.array([datetime.strptime(x, '%Y-%m-%d').date() for x in cols[2]], pa.date32()),
        'adjusted': pa.array(cols[3], pa.bool_()),
        'open': pa.array([Decimal(x) for x in cols[4]], pa.decimal128(18, 4)),
        'high': pa.array([Decimal(x) for x in cols[5]], pa.decimal128(18, 4)),
        'low': pa.array([Decimal(x) for x in cols[6]], pa.decimal128(18, 4)),
        'close': pa.array([Decimal(x) for x in cols[7]], pa.decimal128(18, 4)),
        'volume': pa.array([Decimal(x) for x in cols[8]], pa.decimal128(20, 4)),
        'fetched_at': pa.array(list(cols[9]), pa.timestamp('ms', tz='UTC')),
    })
    size = write_parquet(os.path.join(PARQUET_ROOT, 'candles_1d_snapshot', f'snapshot_date={day}', 'part-0.parquet'), table)
    pg_replace(conn, 'raw.candles_1d_snapshot', 'snapshot_date', day,
               ['snapshot_date', 'symbol', 'trade_date', 'adjusted', 'open', 'high', 'low', 'close', 'volume', 'fetched_at'],
               [[r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9].isoformat()] for r in rows])
    return {'symbols': len(symbols), 'rows': len(rows), 'parquet_bytes': size, 'errors': errs}


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--date'); ap.add_argument('--limit', type=int); ap.add_argument('--skip-1d', action='store_true')
    a = ap.parse_args()
    day = resolve_date(a.date); run_id = f"{day}-{uuid.uuid4().hex[:8]}"; t0 = time.time(); started = datetime.now(timezone.utc)
    conn = pg_connect()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO raw.run_log (run_id, job, trade_date, started_at, status) VALUES (%s, 'collect_day', %s, %s, 'running')", (run_id, day, started))
    conn.commit()
    detail = {}; status = 'ok'
    try:
        if not step_calendar(conn, day):
            status = 'skipped'; detail = {'reason': 'not a trading day'}; log(f"{day} 휴장 - 건너뜀")
        else:
            symbols = step_universe(conn, day)
            if a.limit: symbols = symbols[:a.limit]
            log(f"{day} 유니버스 {len(symbols)} 종목"); api = Api()
            detail['candles_1m'] = step_1m(conn, api, day, symbols); log(f"1분봉 {detail['candles_1m']}")
            if not a.skip_1d:
                detail['candles_1d'] = step_1d(conn, api, day, symbols); log(f"일봉 두 벌 {detail['candles_1d']}")
            detail['api'] = api.stats
    except Exception as e:
        status = 'error'; detail['error'] = str(e)[:500]; log(f"ERROR {e}")
    detail['elapsed_s'] = round(time.time() - t0, 1)
    with conn.cursor() as cur:
        cur.execute("UPDATE raw.run_log SET finished_at = %s, status = %s, detail = %s WHERE run_id = %s",
                    (datetime.now(timezone.utc), status, json.dumps(detail, ensure_ascii=False), run_id))
    conn.commit(); conn.close()
    log(f"DONE {day} status={status} elapsed={detail['elapsed_s']}s")
    sys.exit(0 if status in ('ok', 'skipped') else 1)


if __name__ == '__main__':
    main()
