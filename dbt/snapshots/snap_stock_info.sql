{% snapshot snap_stock_info %}
{# 종목 기본정보 SCD2. 매일 받은 스냅샷의 최신 행을 check 전략으로 추적한다. 이름·시장·유형·상태·발행주식수·NXT·거래정지·폐지일이 바뀌면 새 버전.
   dbt snapshot 을 쓰는 이유: 날짜별 스냅샷 표에서 SCD2 를 직접 SQL 로 만드는 것보다 "언제 바뀌었나" 를 도구가 기록하게 두는 것이 표준이다. #}
{{ config(target_schema='snap', unique_key='symbol', strategy='check',
          check_cols=['name', 'market', 'security_type', 'status', 'shares_outstanding', 'nxt_supported', 'krx_trading_suspended', 'nxt_trading_suspended', 'liquidation_trading', 'delist_date']) }}
select symbol, name, market, security_type, is_common_share, status, list_date, delist_date, shares_outstanding,
       nxt_supported, krx_trading_suspended, nxt_trading_suspended, liquidation_trading, snapshot_date
from {{ ref('stg_stock_info') }}
where snapshot_date = (select max(snapshot_date) from {{ ref('stg_stock_info') }})
{% endsnapshot %}
