-- 세션 프로파일은 그날의 종목 속성으로 설명돼야 한다: nxt_supported 이고 NXT 거래정지가 아니면 720. 10-14 에 604 = 604.
--   그날 속성 스냅샷이 없는 날은 판정하지 않는다 (10-13).
select trade_date, count(*) as mismatched
from {{ ref('dim_symbol_day_profile') }}
where has_same_day_attr and profile_observed is not null and profile_expected is not null and profile_observed <> profile_expected
group by trade_date having count(*) > 0
