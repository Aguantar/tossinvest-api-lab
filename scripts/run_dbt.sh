#!/usr/bin/env bash
# 토스 dbt 일 배치 (2026-10-15, docs/05). 마감 순회(20:30, 약 72분) 뒤 21:50 KST. run → snapshot → test → source freshness. 결과는 raw.run_log(job='dbt').
set -uo pipefail
. /home/ubuntu/toss/env.sh; export PGPASSWORD=$(cat /home/ubuntu/toss/.pgpass_toss); cd /home/ubuntu/toss/dbt
RUN_ID="dbt-$(date -u +%Y%m%dT%H%M%S)"; LOG=/mnt/backup/toss/logs/dbt.log
P() { docker exec toss-postgres psql -U toss -d toss -tAc "$1"; }
P "insert into raw.run_log (run_id, job, trade_date, started_at, status) values ('$RUN_ID', 'dbt', (now() at time zone 'Asia/Seoul')::date, now(), 'running')"
status=ok; detail=""
for step in "run" "snapshot" "test" "source freshness"; do
  out=$(/home/ubuntu/toss/venv/bin/dbt $step --profiles-dir . 2>&1); rc=$?
  summary=$(echo "$out" | grep -E "^.*Done\." | tail -1 | sed 's/\x1b\[[0-9;]*m//g' | cut -c12-)
  echo "$(date -u +%FT%TZ) $RUN_ID $step rc=$rc $summary" >> "$LOG"
  detail="$detail$step: rc=$rc $summary; "
  if [ $rc -ne 0 ] && [ "$step" != "source freshness" ]; then status=error; echo "$out" | grep -E "FAIL|ERROR" | head -20 >> "$LOG"; break; fi
done
P "update raw.run_log set finished_at = now(), status = '$status', detail = to_jsonb(\$\$${detail//\'/}\$\$::text) where run_id = '$RUN_ID'"
echo "$(date -u +%FT%TZ) $RUN_ID DONE $status" >> "$LOG"
