#!/usr/bin/env bash
# 부하 구간 대조: 랭킹 상위 N 종목을 한 연결(구독 한도 100)로 M분 녹화 → 1분봉 조회 → 셀 대조. 결과는 data/burst_<UTC>.log.
# cron 에서 돈다: 31 13 24 9 * (미국 정규장 개장 22:30 KST 직후). 끝나면 crontab 에서 지운다.
set -u; cd "$(dirname "$0")/.."
COUNTRY=${1:-US}; N=${2:-100}; MIN=${3:-10}; TS=$(date -u +%Y%m%dT%H%M%SZ); LOG=data/burst_$TS.log
{
echo "[$(date -u +%T)] start country=$COUNTRY n=$N minutes=$MIN"
SYMS=$(python3 scripts/top_symbols.py "$COUNTRY" "$N" 2>>"$LOG") || { echo "rankings 실패"; exit 1; }
echo "[$(date -u +%T)] symbols=$(echo "$SYMS" | tr ',' '\n' | wc -l)"
docker run --rm --network host -e PYTHONUNBUFFERED=1 -v "$PWD":/lab -w /lab --entrypoint python cdc-realtime-pipeline-binance-collector:latest scripts/ws_record.py "$SYMS" "$MIN" 2>&1 | sed -E 's/[A-Za-z0-9._-]{40,}/<masked>/g' | cut -c1-300
F=$(ls -t data/ws_trades_*.jsonl | head -1); echo "[$(date -u +%T)] frames=$(wc -l < "$F") file=$F"
python3 scripts/rest_candles.py "$SYMS" 30 2>&1 | tail -2
C=$(ls -t data/candles_2*.jsonl | head -1)
echo "=== reconcile ==="; python3 scripts/reconcile.py "$F" "$C"
echo "=== frame profile ==="; python3 - "$F" <<'PY'
import json, sys, collections
from datetime import datetime
types=collections.Counter(); topics=collections.Counter(); lag=[]; per_sec=collections.Counter()
for ln in open(sys.argv[1]):
    o=json.loads(ln); fr=json.loads(o['raw']); types[fr.get('type')]+=1
    if fr.get('type')=='message':
        topics[fr['topic']]+=1; ts=datetime.fromisoformat(fr['data']['timestamp']).timestamp(); lag.append(o['recv_ts']-ts); per_sec[int(o['recv_ts'])]+=1
lag.sort(); ps=sorted(per_sec.values())
print("types:", dict(types), "| topics:", len(topics), "| top5 topic counts:", sorted(topics.values(), reverse=True)[:5])
if lag: print(f"lag p50={lag[len(lag)//2]:.3f}s p95={lag[int(len(lag)*0.95)]:.3f}s p99={lag[int(len(lag)*0.99)]:.3f}s max={lag[-1]:.3f}s n={len(lag)}")
if ps: print(f"msgs/sec p50={ps[len(ps)//2]} p95={ps[int(len(ps)*0.95)]} max={ps[-1]} seconds={len(ps)}")
PY
echo "[$(date -u +%T)] done"
} >> "$LOG" 2>&1
