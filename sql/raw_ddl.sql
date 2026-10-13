-- 토스증권 원천 층 (2026-10-14, 1주차). 원천은 Parquet 이 정본이고 이 표는 그 적재본. 재적재는 trade_date/snapshot_date 단위 delete+insert.
CREATE SCHEMA IF NOT EXISTS raw;

-- 국내 1분봉. 봉의 ts 는 봉 종료 시각(FAQ). 월 파티션: 하루 240만 행, 연 5.9억 행이라 유지보수(삭제·VACUUM)를 파티션 단위로.
CREATE TABLE IF NOT EXISTS raw.candles_1m (
  trade_date date NOT NULL,
  symbol text NOT NULL,
  ts timestamptz NOT NULL,
  open numeric(18,4), high numeric(18,4), low numeric(18,4), close numeric(18,4),
  volume numeric(20,4),
  currency text,
  fetched_at timestamptz NOT NULL,
  PRIMARY KEY (trade_date, symbol, ts)
) PARTITION BY RANGE (trade_date);

-- 일봉 스냅샷 두 벌. adjusted 가 true/false. snapshot_date 마다 최근 60 거래일을 다시 받아 둔다 - 정정 이력의 원재료.
CREATE TABLE IF NOT EXISTS raw.candles_1d_snapshot (
  snapshot_date date NOT NULL,
  symbol text NOT NULL,
  trade_date date NOT NULL,
  adjusted boolean NOT NULL,
  open numeric(18,4), high numeric(18,4), low numeric(18,4), close numeric(18,4),
  volume numeric(20,4),
  fetched_at timestamptz NOT NULL,
  PRIMARY KEY (snapshot_date, symbol, trade_date, adjusted)
) PARTITION BY RANGE (snapshot_date);

-- 종목 마스터 스냅샷. 상장·폐지·유형 변화의 원재료 (SCD 는 dbt 에서).
CREATE TABLE IF NOT EXISTS raw.universe_snapshot (
  snapshot_date date NOT NULL,
  market text NOT NULL,
  symbol text NOT NULL,
  payload jsonb NOT NULL,
  fetched_at timestamptz NOT NULL,
  PRIMARY KEY (snapshot_date, symbol)
);

-- 달력: 그날이 거래일인가, 세션 시각. "안 오는 게 정상" 의 근거.
CREATE TABLE IF NOT EXISTS raw.market_calendar (
  trade_date date PRIMARY KEY,
  is_trading boolean NOT NULL,
  sessions jsonb,
  fetched_at timestamptz NOT NULL
);

-- 순회 명세: 종목마다 그날 받은 봉 수와 첫·마지막 봉. 빈틈 판정의 기준(세션 프로파일 720/660/390)은 여기서 나온다.
CREATE TABLE IF NOT EXISTS raw.sweep_manifest (
  trade_date date NOT NULL,
  symbol text NOT NULL,
  bars integer NOT NULL,
  first_ts timestamptz, last_ts timestamptz,
  requests integer NOT NULL,
  retries_429 integer NOT NULL DEFAULT 0,
  status text NOT NULL,            -- ok / empty / error
  error text,
  fetched_at timestamptz NOT NULL,
  PRIMARY KEY (trade_date, symbol)
);

-- 실행 기록: 만든 것 ≠ 도는 것. 실행마다 한 행.
CREATE TABLE IF NOT EXISTS raw.run_log (
  run_id text PRIMARY KEY,
  job text NOT NULL,
  trade_date date,
  started_at timestamptz NOT NULL,
  finished_at timestamptz,
  status text NOT NULL,
  detail jsonb
);

-- 파티션: 2026-10 ~ 2027-03 미리. 적재기가 없으면 만든다.
DO $$
DECLARE d date := date '2026-10-01';
BEGIN
  WHILE d < date '2027-04-01' LOOP
    EXECUTE format('CREATE TABLE IF NOT EXISTS raw.candles_1m_%s PARTITION OF raw.candles_1m FOR VALUES FROM (%L) TO (%L)', to_char(d,'YYYYMM'), d, d + interval '1 month');
    EXECUTE format('CREATE TABLE IF NOT EXISTS raw.candles_1d_snapshot_%s PARTITION OF raw.candles_1d_snapshot FOR VALUES FROM (%L) TO (%L)', to_char(d,'YYYYMM'), d, d + interval '1 month');
    d := d + interval '1 month';
  END LOOP;
END $$;

-- 2026-10-14 장중 폴링 (docs/04 §5). 10분마다 전 종목 최신 1분봉 200개를 받아 upsert. 같은 봉이 다시 오면 값이 바뀌었는지 센다(changes).
--   마감 뒤 완결본(candles_1m)과 셀 단위로 비교해 "진행 중인 봉이 나중에 바뀌는가·얼마나·언제" 를 잰다.
CREATE TABLE IF NOT EXISTS raw.candles_1m_intraday (
  trade_date date NOT NULL,
  symbol text NOT NULL,
  ts timestamptz NOT NULL,
  open numeric(18,4), high numeric(18,4), low numeric(18,4), close numeric(18,4),
  volume numeric(20,4),
  first_seen_at timestamptz NOT NULL,
  last_seen_at timestamptz NOT NULL,
  changes integer NOT NULL DEFAULT 0,     -- first_seen 이후 값이 바뀐 횟수
  last_changed_at timestamptz,
  PRIMARY KEY (trade_date, symbol, ts)
);
CREATE TABLE IF NOT EXISTS raw.poll_log (
  run_id text PRIMARY KEY,
  trade_date date NOT NULL,
  started_at timestamptz NOT NULL,
  finished_at timestamptz,
  status text NOT NULL,
  detail jsonb
);

-- 2026-10-14 API 전수 점검 뒤 추가 (docs/04 §6). 원문은 payload jsonb 로 그대로 두고 열은 dbt 에서 고른다.
-- 일별 시계열 5종 (투자자별 매매·공매도·프로그램·신용·대차). count/until 페이지네이션이 있어 과거를 받을 수 있다. updatedAt 이 있으므로 값이 바뀔 수 있어 upsert.
CREATE TABLE IF NOT EXISTS raw.trend_daily (
  endpoint text NOT NULL,            -- investor_trading / short_selling / program_trades / credit_trades / securities_lending
  symbol text NOT NULL,
  trade_date date NOT NULL,
  payload jsonb NOT NULL,
  fetched_at timestamptz NOT NULL,
  PRIMARY KEY (endpoint, symbol, trade_date)
);
-- 종목 기본정보 스냅샷 (상장일·폐지일·상태·발행주식수). /stocks 200건씩
CREATE TABLE IF NOT EXISTS raw.stock_info_snapshot (
  snapshot_date date NOT NULL, symbol text NOT NULL, payload jsonb NOT NULL, fetched_at timestamptz NOT NULL,
  PRIMARY KEY (snapshot_date, symbol)
);
-- 매수 유의·VI 등 경고 스냅샷. 있는 종목만 행. 경고가 없으면 그날 그 종목 행이 없다는 것이 정보다.
CREATE TABLE IF NOT EXISTS raw.warnings_snapshot (
  snapshot_date date NOT NULL, symbol text NOT NULL, seq integer NOT NULL, payload jsonb NOT NULL, fetched_at timestamptz NOT NULL,
  PRIMARY KEY (snapshot_date, symbol, seq)
);
-- 지수(KOSPI·KOSDAQ) 일봉과 투자자별 매매대금
CREATE TABLE IF NOT EXISTS raw.index_daily (
  symbol text NOT NULL, kind text NOT NULL,   -- candle / investor_trading
  trade_date date NOT NULL, payload jsonb NOT NULL, fetched_at timestamptz NOT NULL,
  PRIMARY KEY (symbol, kind, trade_date)
);
-- 랭킹 스냅샷 (6 유형 × 1d, 상위 100)
CREATE TABLE IF NOT EXISTS raw.rankings_snapshot (
  snapshot_date date NOT NULL, type text NOT NULL, duration text NOT NULL, rank integer NOT NULL, symbol text NOT NULL,
  payload jsonb NOT NULL, ranked_at timestamptz, fetched_at timestamptz NOT NULL,
  PRIMARY KEY (snapshot_date, type, duration, rank)
);
-- 환율 (1분 갱신 표시 환율, 하루 1회 기록)
CREATE TABLE IF NOT EXISTS raw.exchange_rate (
  fetched_at timestamptz PRIMARY KEY, payload jsonb NOT NULL
);
