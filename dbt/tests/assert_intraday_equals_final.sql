-- 장중 폴링 표 = 마감 완결본. 10-14 첫 결과 100%. 깨지면 "닫힌 봉이 나중에 바뀐다" 는 새 사실이므로 막고 본다.
--   완결본이 없는 날(오늘, 아직 20:30 전)과 장중 표가 없는 날(폴링 전 날짜)은 대조 대상이 아니다. 둘 다 있는 날만.
select * from {{ ref('dq_intraday_vs_final_daily') }}
where final_cells > 0 and intraday_cells > 0 and (differ_cells > 0 or final_only > 0 or intraday_only > 0)
