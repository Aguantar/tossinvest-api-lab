# 04. 1주차: 원천 수집기 (2026-10-14)

목표: 국내 전 종목 1분봉이 매 거래일 빈틈없이 쌓이는 것. 그 위에 2주차부터 dbt 층을 얹는다.

## 1. 결정

| 결정 | 왜 |
|---|---|
| 실행 위치 Oracle (ARM 2코어, ES 와 동거) | 코인 프로젝트의 공백("한 대, 클라우드 없음")을 메운다. 하루 1회 20분 잡이라 2코어로 충분. 저장은 백업 볼륨(147G, 여유 70G) |
| 저장 Parquet(정본) + Postgres 16(적재본) | Parquet 은 하루 1분봉 240만 행이 28MB(실측 11.8B/행 zstd). Postgres 는 dbt·Metabase 가 붙는 "진짜 DB". DuckDB 는 혼자 쓰는 파일이라 뺐다(docs/02 §9 정정). Postgres 가 깨져도 Parquet 에서 다시 넣는다 |
| Postgres 는 컨테이너, 127.0.0.1 과 WireGuard(10.88.0.1)에만 바인딩, 메모리 768MB | 외부 노출 0. 미니PC 의 Metabase 가 터널로 붙는다 |
| 장 마감 뒤 1회 (20:30 KST, 평일 cron) | 1분봉은 REST 로 완결된 봉을 받는 것이라 실시간일 이유가 없다. 진행 중인 봉은 값이 바뀔 수 있다(FAQ) |
| 휴장 판정은 달력 API | `integrated` 가 null 이면 안 받고 run_log 에 skipped. "안 오는 게 정상" 의 근거를 API 가 준다 |
| 1분봉 표는 월 파티션 | 연 5.9억 행. 삭제·VACUUM 을 파티션 단위로 |
| 일봉 두 벌(수정주가 적용·미적용) 60일을 매일 스냅샷 | 정정 이력의 원재료. 60일 창에 604 종목이 바뀐다(docs/03 §4). 어제 스냅샷과 오늘 스냅샷의 차이가 기업행위다 |
| 종목 마스터를 매일 스냅샷 (payload jsonb 그대로) | 상장·폐지·유형 변화를 dbt 가 SCD 로 만든다. 원문 필드를 미리 고르지 않는다 |
| 봉 수 판정은 수집기가 하지 않는다 | 정답이 종목마다 720/660/390 으로 다르다(docs/03 §3). 수집기는 종목별 봉 수·첫·마지막 봉을 manifest 에 남기고 판정은 dbt 테스트가 한다 |
| 시세 값은 로그에 찍지 않는다 | 약관. 로그엔 건수·시간·오류만 |
| 허용 IP | Oracle 공인 IP 가 목록에 없어 첫 호출이 403. 사용자가 추가한 뒤 200. 클라우드 IP 변동 시 다시 403 이 날 수 있다(FAQ) - 헬스체크 항목 후보 |

## 2. 만든 것

- `sql/raw_ddl.sql`: raw 스키마 6표 (candles_1m, candles_1d_snapshot, universe_snapshot, market_calendar, sweep_manifest, run_log)
- `scripts/collect_day.py`: 달력 → 유니버스 → 1분봉 순회(6스레드, 초당 20, 429 는 Retry-After) → 일봉 두 벌 → Parquet·Postgres 적재(날짜 단위 delete+insert) → run_log
- Oracle: `~/toss/{venv,scripts,env.sh,.env,.pgpass_toss}`, 컨테이너 `toss-postgres`, Parquet `/mnt/backup/toss/parquet/<표>/<날짜>=.../part-0.parquet`, 로그 `/mnt/backup/toss/logs/`
- cron: `30 11 * * 1-5` (UTC) = 20:30 KST 평일

## 3. 시험 (10-13 분, 20 종목)

| 어디서 | 결과 |
|---|---|
| 미니PC | 1분봉 11,610 (720봉 5 · 660봉 8 · 390봉 7), 일봉 2,400행, 요청 107, 429 1, 오류 0, 15.0초. Parquet 59KB + 32KB |
| Oracle (같은 날짜 재실행) | 같은 수치 14.5초. Postgres 행 수가 그대로(멱등) |
| Postgres 확인 | 1분봉 ts 범위 08:01~20:00 KST, 390봉 종목은 09:01~15:30, run_log 1행 ok |

## 4. 다음

- 10-13 전 종목 첫 실행(Oracle) → 봉 수 분포가 docs/03(720: 601 / 660: 1,890 / 390: 1,810)과 같은 자릿수인지
- 10-14 20:30 cron 첫 자동 실행 → "만든 것 ≠ 도는 것"
- 2주차: dbt(dbt-postgres) staging·세션 프로파일 차원·빈틈 테스트·정정 이력 모델
