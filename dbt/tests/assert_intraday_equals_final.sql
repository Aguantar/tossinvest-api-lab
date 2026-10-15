-- 장중 폴링 표 = 마감 완결본, 양쪽에 다 있는 종목에 한해. 10-14 100%, 10-15 종목 집합 변화(신규 2·제외 1) 외 100%.
--   완결본이 없는 날(오늘)과 장중 표가 없는 날은 대조 대상이 아니다. 종목 집합 변화는 dq 표의 final_only/intraday_only 열로 본다.
select * from {{ ref('dq_intraday_vs_final_daily') }}
where final_cells > 0 and intraday_cells > 0 and (differ_cells > 0 or final_only_cells_common > 0 or intraday_only_cells_common > 0)
