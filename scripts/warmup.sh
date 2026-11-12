#!/usr/bin/env bash
# Pre-demo warm-up. Run ~10 minutes before showing the demo, from the bundle root.
#
# Starts both warehouses and the app, waits until the app actually answers HTTP
# (platform status RUNNING is not proof), and touches the tables both views read
# so the first click does not measure warehouse start-up or disk-cache fill.
#
#   scripts/warmup.sh PROFILE
set -uo pipefail

P="${1:?usage: scripts/warmup.sh PROFILE}"
APP=lakehouse-rt-demo
say() { printf '%s\n' "$*"; }

V=$(databricks bundle summary -p "$P" -o json | jq -c '.variables | map_values(.value)')
DBSQL=$(jq -r .dbsql_warehouse_id <<<"$V")
RT=$(jq -r .rt_warehouse_id <<<"$V")
S="$(jq -r .catalog <<<"$V").$(jq -r .schema <<<"$V")"

say "[1/4] warehouses"
for W in "$DBSQL" "$RT"; do
  ST=$(databricks warehouses get "$W" -p "$P" -o json 2>/dev/null | jq -r '.state // "?"')
  if [[ "$ST" != "RUNNING" ]]; then
    say "  starting $W (was $ST)"
    databricks warehouses start "$W" -p "$P" --no-wait >/dev/null 2>&1
  else
    say "  $W already RUNNING"
  fi
done

say "[2/4] app"
ST=$(databricks apps get "$APP" -p "$P" -o json 2>/dev/null | jq -r '.compute_status.state // "?"')
if [[ "$ST" != "ACTIVE" ]]; then
  say "  starting app (compute was $ST)"
  databricks apps start "$APP" -p "$P" >/dev/null 2>&1
fi
URL=$(databricks apps get "$APP" -p "$P" -o json | jq -r '.url')

say "[3/4] HTTP reachability"
TOK=$(databricks auth token -p "$P" 2>/dev/null | jq -r '.access_token // empty')
for i in $(seq 1 12); do
  CODE=$(curl -s -L --max-time 40 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $TOK" "$URL/" 2>/dev/null)
  [[ "$CODE" == "200" ]] && { say "  HTTP 200, app is serving"; break; }
  say "  HTTP $CODE (attempt $i/12)"
  sleep 15
done

say "[4/4] warming the tables on both warehouses"
for Q in \
  "SELECT COUNT(*) FROM $S.gold_dispute_case_fact WHERE NOT is_closed" \
  "SELECT COUNT(*) FROM $S.gold_merchant_dispute_monthly" \
  "SELECT COUNT(*) FROM $S.gold_dispute_event_stream WHERE merchant_id = 113961" \
  "SELECT SUM(chargeback_amount) FROM $S.gold_dispute_case_fact WHERE merchant_id = 113961"; do
  for W in "$DBSQL" "$RT"; do
    databricks api post /api/2.0/sql/statements -p "$P" --json "$(jq -n --arg q "$Q" --arg w "$W" \
      '{statement:$q,warehouse_id:$w,wait_timeout:"50s"}')" >/dev/null 2>&1
  done
done

cat <<MSG

Ready:
  Architecture   $URL/
  Head to head   $URL/?view=headtohead
  Scaled load    $URL/?view=scale
MSG
