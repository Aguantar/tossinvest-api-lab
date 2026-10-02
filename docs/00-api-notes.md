# 00. 토스증권 Open API 조사 노트 (2026-09-24 03:30~03:55 UTC)

읽은 출처 (전부 공개 문서, 원문은 `docs/raw/` 에 보관):

| 문서 | URL | 보관 |
|---|---|---|
| LLM 안내 인덱스 | https://developers.tossinvest.com/llms.txt | (인덱스라 미보관) |
| 개요·빠른 시작·Rate Limits·웹소켓 연동 | https://openapi.tossinvest.com/openapi-docs/overview.md | `raw/overview.md` |
| API 레퍼런스 (Markdown) | https://openapi.tossinvest.com/openapi-docs/latest/api-reference/README.md | `raw/api-reference.md` |
| OpenAPI 3.1 (정본) | https://openapi.tossinvest.com/openapi-docs/latest/openapi.json | `raw/openapi.json` (v1.2.17, 경로 33) |
| AsyncAPI 3.0 웹소켓 (정본) | https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json | `raw/asyncapi.json` |
| FAQ | https://openapi.tossinvest.com/openapi-docs/faq.md | `raw/faq.md` |
| 브라우저 문서 | https://developers.tossinvest.com/docs | JS 렌더링이라 fetch 로는 빈 페이지 |

## 1. 접속

- REST `https://openapi.tossinvest.com`, 웹소켓 `wss://openapi-ws.tossinvest.com/ws/v1`
- 인증: OAuth 2.0 Client Credentials. `POST /oauth2/token` (form: grant_type=client_credentials, client_id, client_secret) → `Authorization: Bearer`. 계좌·자산·주문·조건주문은 `X-Tossinvest-Account` 헤더 추가
- 웹소켓도 같은 Bearer 헤더로 handshake

## 2. REST 엔드포인트 (33)

시세: `GET /api/v1/prices`(symbols 콤마 최대 200) · `/orderbook` · `/trades`(symbol, count 최대 **50**, 과거 페이지네이션 **없음** → 당일 전체 체결 재구성 불가) · `/candles`(symbol, interval `1m`|`1d`, count 최대 **200**, `before` 로 과거 페이지네이션, `adjusted` 수정주가) · `/price-limits`
종목: `/stocks`(symbols 최대 200) · `/stocks/all`(그룹 STOCK_ALL, 초당 1회) · `/stocks/{symbol}/warnings|investor-trading|program-trades|short-selling|credit-trades|securities-lending`
시장: `/exchange-rate` · `/market-calendar/KR`(KRX·NXT 세션) · `/market-calendar/US` · `/rankings` · `/market-indicators/prices|{symbol}/candles|{symbol}/investor-trading`
계좌·주문: `/accounts` · `/holdings` · `/orders`(생성·목록) · `/orders/{id}`(상세·`/cancel`·`/modify`) · `/buying-power` · `/sellable-quantity` · `/commissions` · `/conditional-orders`(SINGLE·OCO·OTO)

응답 예 (candles 1m): `{"timestamp":"2026-03-25T09:32:00+09:00","openPrice":"72000",...,"volume":"15200","currency":"KRW"}`, `nextBefore`. 숫자는 전부 **문자열**.
응답 예 (trades): `{"price":"72000","volume":"120","timestamp":"2026-03-25T09:30:42.000+09:00","currency":"KRW"}` - **체결 ID·시퀀스 없음**.

## 3. Rate Limits (클라이언트 × 그룹, 초당, 토큰 버킷)

AUTH 5 · ACCOUNT 1 · ASSET 5 · STOCK 5 · STOCK_ALL 1 · STOCK_TRADING_TREND 10 · MARKET_INFO 3 · **MARKET_DATA 15** · **MARKET_DATA_CHART 20** · RANKING 5 · MARKET_INDICATOR_PRICE 10 · MARKET_INDICATOR 10 · MARKET_INDICATOR_CHART 5 · ORDER 10(09:00~09:10 KST 도 10) · ORDER_HISTORY 5 · ORDER_INFO 6(피크 3) · CONDITIONAL_ORDER 5 · CONDITIONAL_ORDER_HISTORY 10.
헤더 `X-RateLimit-Limit`(burst) · `X-RateLimit-Remaining` · `X-RateLimit-Reset`(초) · 429 에 `Retry-After`. 한도는 예고 없이 바뀔 수 있음.

## 4. 웹소켓

- 채널 `trade:{kr|us}` · `orderbook:{kr|us}` · `personal:order`. 국내는 **KRX+NXT 통합 시세만**, 거래소 구분 필드 없음
- 구독 선언 = JSON 배열 1개, **full-replace**. ack `subscriptions{subscribed,rejected}`, 데이터 `message{topic,data}`, `error`, `pong`
- 체결 프레임 data: `price, volume, timestamp, currency` - **시퀀스·체결ID·누적거래량 없음**
- 호가 프레임 data: `timestamp(null 가능), currency, asks[{price,volume}], bids[...]`
- 전달 보장: 시세 채널 **LOSSY**("수신이 밀리면 중간 프레임 유실, 유실 감지용 sequence 미제공"), 주문 채널 LOSSLESS(세션 내, 2초 이상 막히면 종료)
- 한도: **계정당 동시 연결 2**(초과 시 가장 오래된 연결 종료) · **연결당 구독 100**(채널×종목 합산) · 선언 5회/초
- 서버는 180초 무수신 시 종료 → 60초 PING. 서버 배포 시 `server-shutdown` error

## 5. FAQ 에서 파이프라인에 중요한 것

- 국내 1분봉 08:01~20:00(프리~애프터), `1m` 의 timestamp 는 **봉 종료 시각**(09:01 봉 = 09:00:00~09:00:59.999). 시가 단일가(09:00) 체결은 09:01 봉, 종가 단일가(15:30)는 15:31 봉
- 거래량 0 봉은 체결 없음(OHLC 는 직전 종가로 채움). 일봉 거래량 ≠ 분봉 합(집계 범위 다름). 진행 중인 봉은 다른 증권사와 다를 수 있음
- 과거 데이터: 국내 2022-11-23, 미국 2021-11-30 이후. 과거 캔들 사후 정정 없음. 기업행위는 `adjusted` 수정비율로만(가격 ×비율 반올림, 거래량 ÷비율 버림). 제공 시작 이전 구간은 `adjusted=false` 로도 원가격 불가. 상장폐지 종목 404
- 미국 시세는 일부 거래소 기준(NBBO 아님), 1분봉 이상값 안 거름
- 주문 사전요건(`prerequisite-required`): 투자자 성향 등록·위험 고지·약관 동의

## 6. 데이터 이용 정책 (FAQ 원문)

> API를 통해 제공받는 정보는 투자자 본인의 매매 목적에 한하여 이용하여야 합니다. 상업적 이용이 제외될 뿐만 아니라 비상업적인 용도라 하더라도 제 3자에게 배포하는 행위는 엄격히 금지됩니다.

해석 (내 것, 법률 자문 아님. 신청 때 동의한 이용약관 전문은 못 봤으므로 그것을 확인할 것):
- 금지가 분명한 것: 받은 정보의 제3자 배포(원본·가공 데이터, 시세가 보이는 캡처·공개 대시보드), 상업적 이용
- 문제 없는 것: 본인 계좌·본인 데이터로 혼자 처리·학습. 본인 계좌 자동매매(주문·조건주문 API 가 그 목적). 코드·구조·트러블슈팅 공개
- 처음엔 "본인의 매매 목적에 한하여" 를 이용 금지로 넓게 읽어 "안 된다" 고 했다가 정정함. 이 문단의 실제 규제 대상은 배포·상업
- 위반이 되는 선: 신호·데이터를 남에게 주는 것(텔레그램·구독·대리 매매 → 약관 + 등록 없는 투자자문·일임), 유료화

## 7. 이 API 로 할 수 있는 파이프라인 문제 (지금 프로젝트와 안 겹치는 것)

1. **시퀀스 없는 LOSSY 스트림의 유실 측정**: 웹소켓 체결을 분 단위로 합쳐 `1m` 캔들 거래량과 셀(종목×분) 대조. 봉 경계 규칙(종료 시각 표기, 단일가 체결의 봉 귀속)을 정확히 맞춰야 0 이 나온다
2. **체결 REST 50건 한도**: 폴링으로 당일 전체를 재구성할 수 없다 → 어디까지 되고 어디부터 안 되는지 실측
3. **기업행위·수정주가**: `adjusted` true/false 두 벌을 매일 받아 수정비율 변화(=기업행위)를 스냅샷으로 잡는 "정정 이력" 데이터셋
4. **장 운영 달력**: KRX·NXT 세션, 휴장, 08:01~20:00 봉 범위. "안 오는 게 정상"을 달력이 결정
5. 자동매매(본인 계좌)는 별건. 하려면 실돈 원칙 변경을 기록하고 최소 수량·손실 한도를 코드에 박는다

## 8. 공개 범위 (처음부터 정함)

저장소에는 코드·문서만. `data/`, `.env`, `*.jsonl`, 토큰은 `.gitignore`. 문서의 숫자는 "API 동작에 대한 측정"(유실률·429 빈도·재연결 시간)까지, 특정 종목의 시세 값은 적지 않는다.
