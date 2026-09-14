#!/usr/bin/env bash
# Checks that a failing step ends the run instead of hanging it.
#
# Makes the incubator report an error on every step, starts a run, and expects
# the run to reach "failed". Puts the incubator back to normal afterwards.
set -uo pipefail

EXEC=${EXEC:-http://localhost:5001}
TIMEOUT=${TIMEOUT:-60}
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE/.."

restore() {
  echo
  echo "putting incubator-1 back to normal..."
  INC_FAIL_PCT=0 docker compose up -d incubator-1 >/dev/null 2>&1
  sleep 4
}
trap restore EXIT

echo "making incubator-1 fail every step..."
INC_FAIL_PCT=100 docker compose up -d incubator-1 >/dev/null 2>&1
sleep 5

fail_pct=$(curl -sf "$EXEC/drivers" | jq -r '.[] | select(.device_id=="incubator-1") | .fail_pct')
if [ "$fail_pct" != "100" ]; then
  echo "  FAIL  could not switch failures on (incubator-1 fail_pct=$fail_pct)"
  exit 1
fi

dropping=$(curl -sf "$EXEC/drivers" | jq -r '[.[] | select((.drop_result_pct // 0) > 0) | "\(.device_id)=\(.drop_result_pct)%"] | join(", ")')
if [ -n "$dropping" ]; then
  echo
  echo "  NOTE  result dropping is also on ($dropping). A dropped failure report"
  echo "        looks the same as a hang, so this check cannot tell them apart."
  echo "        Set LH_/INC_/PR_DROP_PCT to 0 for a clean result."
fi

run_id=$(curl -sf -X POST "$EXEC/runs" -H 'Content-Type: application/json' -d '{}' | jq -r '.id // empty')
if [ -z "$run_id" ]; then
  echo "  FAIL  could not create a run -- is the executor up?"
  exit 1
fi
echo "run: $run_id"

curl -sf -X POST "$EXEC/runs/$run_id/start" >/dev/null

start=$(date +%s)
status=""
while [ $(( $(date +%s) - start )) -lt "$TIMEOUT" ]; do
  status=$(curl -sf "$EXEC/runs/$run_id" | jq -r '.run.status')
  case "$status" in completed|failed|aborted) break ;; esac
  sleep 1
done
elapsed=$(( $(date +%s) - start ))

echo
case "$status" in
  failed)
    echo "  PASS  the run ended as 'failed' after ${elapsed}s"
    rc=0
    ;;
  running|"")
    echo "  FAIL  the run is still '${status:-unknown}' after ${elapsed}s -- a failing step hung it"
    echo "        (this driver fails every step, so an unbounded retry never ends)"
    rc=1
    ;;
  *)
    echo "  FAIL  the run ended as '$status', expected 'failed'"
    rc=1
    ;;
esac

echo
curl -sf "$EXEC/runs/$run_id" | jq -r '.steps[] | "  \(.name): \(.status)\(if .error then "  (\(.error))" else "" end)"'
exit $rc
