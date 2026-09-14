#!/usr/bin/env bash
# Show when each step actually occupied its device.
set -uo pipefail

EXEC=${EXEC:-http://localhost:5001}
run_id=${1:?usage: timeline.sh <run_id>}

json=$(curl -sf "$EXEC/runs/$run_id/timeline") || { echo "could not fetch timeline" >&2; exit 1; }

echo "$json" | jq -r '
  "run   : \(.run_id)",
  "status: \(.status)",
  "total : \(if .total_duration_ms then "\(.total_duration_ms/1000)s" else "-" end)"'
echo

echo "$json" | jq -r '
  (.total_duration_ms // ([.steps[].end_offset_ms // 0] | max) // 1) as $total
  | .steps
  | sort_by(.start_offset_ms // 9999999)
  | .[]
  | [ .name, .device_id, .status, (.dispatch_count|tostring),
      (.start_offset_ms // -1 | tostring), (.end_offset_ms // -1 | tostring),
      ($total|tostring) ]
  | @tsv' |
awk -F'\t' '
BEGIN { W=52 }
{
  name=$1; dev=$2; st=$3; dc=$4; s=$5+0; e=$6+0; total=$7+0
  if (total <= 0) total = 1
  bar=""
  if (s >= 0 && e >= 0) {
    a=int(s*W/total); b=int(e*W/total); if (b<=a) b=a+1
    for(i=0;i<a;i++) bar=bar " "
    for(i=a;i<b;i++) bar=bar "#"
  } else if (s >= 0) {
    a=int(s*W/total)
    for(i=0;i<a;i++) bar=bar " "
    bar=bar ">>"
  } else {
    bar="(not dispatched)"
  }
  flag = (dc+0 > 1) ? sprintf("  <-- %d dispatch attempts", dc) : ""
  printf "%-19s %-17s %-10s |%-*s|%s\n", name, dev, st, W, bar, flag
}'
echo
echo "Each # is time the step was occupying its device."
