-- 세션 프로파일은 종목 속성으로 설명돼야 한다: nxt_supported ↔ 720. 10-14 에 604 = 604.
--   어긋나면 종목 속성이 바뀌었거나(NXT 편입·제외) 수집이 세션 하나를 통째로 놓친 것이다.
select trade_date, count(*) as mismatched
from {{ ref('dim_symbol_day_profile') }}
where profile_observed is not null and profile_expected is not null and profile_observed <> profile_expected
group by trade_date having count(*) > 0
