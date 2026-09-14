#!/usr/bin/env bash
# Checks whether the executor does its job.
#
#   ./scripts/acceptance.sh                    # the default workflow
#   ./scripts/acceptance.sh "Triple Assay"     # a named one, see workflows.yaml
#
# Five checks must pass. Two more numbers are reported but do not fail the run.
set -uo pipefail

EXEC=${EXEC:-http://localhost:5001}
TIMEOUT=${TIMEOUT:-90}
workflow=${1:-}

pass=0; fail=0
ok()   { printf '  PASS  %s\n' "$1"; pass=$((pass+1)); }
bad()  { printf '  FAIL  %s\n' "$1"; fail=$((fail+1)); }
note() { printf '  ....  %s\n' "$1"; }

body='{}'
[ -n "$workflow" ] && body=$(jq -nc --arg n "$workflow" '{workflow_name: $n}')

run_id=$(curl -sf -X POST "$EXEC/runs" -H 'Content-Type: application/json' -d "$body" | jq -r '.id // empty')
if [ -z "$run_id" ]; then
  echo "could not create a run -- is the executor up? (curl $EXEC/health)" >&2
  exit 2
fi
echo "run: $run_id"

dropping=$(curl -sf "$EXEC/drivers" | jq -r '[.[] | select((.drop_result_pct // 0) > 0) | "\(.device_id)=\(.drop_result_pct)%"] | join(", ")')
if [ -n "$dropping" ]; then
  echo
  echo "NOTE: result dropping is switched on ($dropping)."
  echo "      Checks below may fail for that reason. Set LH_/INC_/PR_DROP_PCT to 0."
fi

failing=$(curl -sf "$EXEC/drivers" | jq -r '[.[] | select((.fail_pct // 0) > 0) | "\(.device_id)=\(.fail_pct)%"] | join(", ")')
if [ -n "$failing" ]; then
  echo
  echo "NOTE: step failures are switched on ($failing)."
  echo "      Checks below may fail for that reason. Set LH_/INC_/PR_FAIL_PCT to 0."
fi

refused_before=$(curl -sf "$EXEC/drivers" | jq '[.[] | (.rejected // 0)] | add // 0')

start=$(date +%s)
curl -sf -X POST "$EXEC/runs/$run_id/start" >/dev/null

status=""
while [ $(( $(date +%s) - start )) -lt "$TIMEOUT" ]; do
  status=$(curl -sf "$EXEC/runs/$run_id" | jq -r '.run.status')
  case "$status" in completed|failed|aborted) break ;; esac
  sleep 1
done
elapsed=$(( $(date +%s) - start ))

tl=$(curl -sf "$EXEC/runs/$run_id/timeline")
drv=$(curl -sf "$EXEC/drivers")
# Drivers report everything they have ever run, so narrow to this run's steps.
mine=$(curl -sf "$EXEC/runs/$run_id" | jq -c '[.steps[].id]')
executions=$(echo "$drv" | jq -c --argjson mine "$mine" \
  '[.[] | (.executed // [])] | add // [] | map(select(. as $id | $mine | index($id)))')

echo
echo "checks"

# 1. the run finished
if [ "$status" = "completed" ]; then
  ok "run completed (${elapsed}s)"
else
  bad "run did not complete -- status '${status:-unknown}' after ${elapsed}s"
fi

# 2. every step completed
notdone=$(echo "$tl" | jq -r '[.steps[] | select(.status != "completed") | .name] | join(", ")')
if [ -z "$notdone" ]; then
  ok "all steps completed"
else
  bad "steps not completed: $notdone"
fi

# 3. no step started before its dependencies finished
violations=$(echo "$tl" | jq -r '
  (.steps | map({key: .name, value: .}) | from_entries) as $by
  | [ .steps[]
      | . as $s
      | select($s.start_offset_ms != null)
      | $s.depends_on[]?
      | . as $d
      | select(($by[$d].end_offset_ms // null) == null
               or ($by[$d].end_offset_ms > $s.start_offset_ms))
      | "\($s.name) started before \($d) finished" ]
  | join("; ")')
started=$(echo "$tl" | jq '[.steps[] | select(.start_offset_ms != null)] | length')
if [ "${started:-0}" -eq 0 ]; then
  note "dependency order: nothing ran, nothing to check"
elif [ -z "$violations" ]; then
  ok "dependency order respected"
else
  bad "$violations"
fi

# 4. independent work actually overlapped
overlaps=$(echo "$tl" | jq '
  [ .steps as $all
    | range(0; ($all|length)) as $i
    | range(($i+1); ($all|length)) as $j
    | $all[$i] as $a | $all[$j] as $b
    | select($a.device_id != $b.device_id)
    | select($a.start_offset_ms != null and $b.start_offset_ms != null)
    | select($a.end_offset_ms != null and $b.end_offset_ms != null)
    | select($a.start_offset_ms < $b.end_offset_ms and $b.start_offset_ms < $a.end_offset_ms)
    | 1 ] | length')
if [ "${overlaps:-0}" -gt 0 ]; then
  ok "independent steps ran at the same time ($overlaps overlapping pair(s))"
else
  bad "nothing ran in parallel -- every step waited for the previous one"
fi

# 5. no step was executed more than once (ground truth from the drivers)
dupes=$(echo "$executions" | jq -r 'group_by(.) | map(select(length > 1) | .[0]) | join(", ")')
executed_total=$(echo "$executions" | jq 'length')
if [ "${executed_total:-0}" -eq 0 ]; then
  note "duplicate execution: no steps executed, nothing to check"
elif [ -z "$dupes" ]; then
  ok "no step executed more than once"
else
  bad "steps executed more than once: $dupes"
fi

echo
echo "for information"
note "total run time: ${elapsed}s"
refused_after=$(echo "$drv" | jq '[.[] | (.rejected // 0)] | add // 0')
note "commands refused by drivers during this run: $(( refused_after - refused_before ))"
dropped=$(echo "$drv" | jq '[.[] | (.dropped // 0)] | add // 0')
[ "${dropped:-0}" -gt 0 ] && note "results dropped by drivers (lifetime): $dropped"

echo
if [ "$fail" -eq 0 ] && [ "$pass" -eq 5 ]; then
  echo "All 5 checks passed."
elif [ "$fail" -eq 0 ]; then
  echo "$pass/5 checks passed, none failed -- the rest could not be checked yet."
else
  echo "$pass/5 checks passed, $fail failed."
fi
echo "Timeline: ./scripts/timeline.sh $run_id"
[ "$fail" -eq 0 ]
