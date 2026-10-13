"""장중 폴링 한 바퀴 (2026-10-14, docs/04 §5). 전 종목 최신 1분봉 200개를 받아 raw.candles_1m_intraday 에 upsert 한다.
같은 봉이 다른 값으로 다시 오면 changes 를 올린다. 마감 뒤 완결본과 비교하는 재료.
  - 유니버스는 Postgres 의 최신 스냅샷을 쓴다(STOCK_ALL 호출 절약).
  - 달력에 세션이 없으면(휴장) 아무것도 안 하고 poll_log 에 skipped.
  - 초당 20 아래, 6스레드. 한 바퀴 4,304 요청 ≈ 3.6분. cron 10분 간격.
  - 시세 값은 로그에 찍지 않는다.
실행: python scripts/poll_intraday.py [--limit N]
"""
import argparse, csv, io, json, os, sys, time, uuid
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import get
from collect_day import Api, run_pool, pg_connect, KST, log


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--limit', type=int); a = ap.parse_args()
    day = datetime.now(KST).strftime('%Y-%m-%d'); run_id = f"{day}-{datetime.now(KST).strftime('%H%M')}-{uuid.uuid4().hex[:6]}"
    started = datetime.now(timezone.utc); t0 = time.time(); conn = pg_connect()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO raw.poll_log (run_id, trade_date, started_at, status) VALUES (%s, %s, %s, 'running')", (run_id, day, started))
        cur.execute("SELECT is_trading FROM raw.market_calendar WHERE trade_date = %s", (day,)); row = cur.fetchone()
    conn.commit()
    if row is None:
        r, _ = get('/api/v1/market-calendar/KR', {'date': day}); sessions = ((r.get('result') or {}).get('today') or {}).get('integrated')
        trading = sessions is not None
        with conn.cursor() as cur:
            cur.execute("INSERT INTO raw.market_calendar (trade_date, is_trading, sessions, fetched_at) VALUES (%s, %s, %s, now()) ON CONFLICT (trade_date) DO NOTHING",
                        (day, trading, json.dumps(sessions) if sessions else None))
        conn.commit()
    else:
        trading = row[0]
    status, detail = 'ok', {}
    try:
        if not trading:
            status = 'skipped'; detail = {'reason': 'not a trading day'}
        else:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol FROM raw.universe_snapshot WHERE snapshot_date = (SELECT max(snapshot_date) FROM raw.universe_snapshot) ORDER BY symbol")
                symbols = [r[0] for r in cur.fetchall()]
            if a.limit: symbols = symbols[:a.limit]
            api = Api(); now = datetime.now(timezone.utc)

            def one(s):
                c, _, _ = api.candles({'symbol': s, 'interval': '1m', 'count': 200, 'adjusted': 'false'})
                if c is None: return s, None
                return s, [b for b in c if b['timestamp'].startswith(day)]

            results = run_pool(symbols, one)
            rows = [(day, s, b['timestamp'], b['openPrice'], b['highPrice'], b['lowPrice'], b['closePrice'], b['volume']) for s, bars in results if bars for b in bars]
            buf = io.StringIO(); w = csv.writer(buf)
            for r in rows: w.writerow(r)
            buf.seek(0)
            with conn.cursor() as cur:
                cur.execute("CREATE TEMP TABLE poll_in (trade_date date, symbol text, ts timestamptz, open numeric, high numeric, low numeric, close numeric, volume numeric) ON COMMIT DROP")
                with cur.copy("COPY poll_in FROM STDIN WITH (FORMAT csv)") as cp: cp.write(buf.read())
                cur.execute("""
                    INSERT INTO raw.candles_1m_intraday (trade_date, symbol, ts, open, high, low, close, volume, first_seen_at, last_seen_at)
                    SELECT trade_date, symbol, ts, open, high, low, close, volume, %(now)s, %(now)s FROM poll_in
                    ON CONFLICT (trade_date, symbol, ts) DO UPDATE SET
                      last_seen_at = EXCLUDED.last_seen_at,
                      changes = raw.candles_1m_intraday.changes + CASE WHEN (raw.candles_1m_intraday.open, raw.candles_1m_intraday.high, raw.candles_1m_intraday.low, raw.candles_1m_intraday.close, raw.candles_1m_intraday.volume)
                                                                   IS DISTINCT FROM (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close, EXCLUDED.volume) THEN 1 ELSE 0 END,
                      last_changed_at = CASE WHEN (raw.candles_1m_intraday.open, raw.candles_1m_intraday.high, raw.candles_1m_intraday.low, raw.candles_1m_intraday.close, raw.candles_1m_intraday.volume)
                                                  IS DISTINCT FROM (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close, EXCLUDED.volume) THEN EXCLUDED.last_seen_at ELSE raw.candles_1m_intraday.last_changed_at END,
                      open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume
                """, {'now': now})
                cur.execute("SELECT count(*), count(*) FILTER (WHERE last_changed_at = %s) FROM raw.candles_1m_intraday WHERE trade_date = %s", (now, day))
                total, changed = cur.fetchone()
            conn.commit()
            detail = {'symbols': len(symbols), 'rows_in': len(rows), 'failed_symbols': sum(1 for _, b in results if b is None),
                      'table_rows': total, 'changed_this_round': changed, 'api': api.stats}
    except Exception as e:
        status = 'error'; detail['error'] = str(e)[:500]; log(f"ERROR {e}")
    detail['elapsed_s'] = round(time.time() - t0, 1)
    with conn.cursor() as cur:
        cur.execute("UPDATE raw.poll_log SET finished_at = now(), status = %s, detail = %s WHERE run_id = %s", (status, json.dumps(detail), run_id))
    conn.commit(); conn.close(); log(f"poll {run_id} {status} {detail}")
    sys.exit(0 if status in ('ok', 'skipped') else 1)


if __name__ == '__main__':
    main()
