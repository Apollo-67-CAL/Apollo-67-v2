#!/usr/bin/env bash
set -eo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
TOKEN="${ADMIN_TOKEN:-}"

if [[ -n "${TOKEN}" ]]; then
  declare -a AUTH_HEADER
  AUTH_HEADER=( -H "X-Admin-Token: ${TOKEN}" )
else
  declare -a AUTH_HEADER
  AUTH_HEADER=()
fi

echo "1) init db"
curl -s "${BASE_URL}/debug/init-db" | python3 -m json.tool

echo "2) get sentiment"
curl -s "${AUTH_HEADER[@]}" "${BASE_URL}/admin/api/sentiment" | python3 -m json.tool

echo "3) update sentiment"
curl -s -X PUT "${AUTH_HEADER[@]}" -H 'Content-Type: application/json' \
  "${BASE_URL}/admin/api/sentiment" \
  -d '{
    "overall": {"enabled": true, "weight": 1.0, "recency_minutes": 1440, "bullish_threshold": 0.2, "bearish_threshold": -0.2, "payload": {}},
    "institutional": {"enabled": true, "weight": 0.35, "recency_minutes": 1440, "bullish_threshold": 0.2, "bearish_threshold": -0.2, "payload": {}},
    "news": {"enabled": true, "weight": 0.35, "recency_minutes": 720, "bullish_threshold": 0.2, "bearish_threshold": -0.2, "payload": {}},
    "social": {"enabled": true, "weight": 0.30, "recency_minutes": 240, "bullish_threshold": 0.2, "bearish_threshold": -0.2, "payload": {}}
  }' | python3 -m json.tool

echo "4) audit"
curl -s "${AUTH_HEADER[@]}" "${BASE_URL}/admin/api/sentiment/audit?limit=10" | python3 -m json.tool

echo "5) list tactics"
curl -s "${AUTH_HEADER[@]}" "${BASE_URL}/admin/api/tactics" | python3 -m json.tool

echo "6) create tactic"
TACTIC_NAME="Breakout_$RANDOM"
TACTIC_ID=$(curl -s -X POST "${AUTH_HEADER[@]}" -H 'Content-Type: application/json' \
  "${BASE_URL}/admin/api/tactics" \
  -d "{\"name\":\"${TACTIC_NAME}\",\"description\":\"Starter tactic\",\"tags\":[\"trend\",\"breakout\"],\"parameters\":{\"lookback\":20,\"risk_reward\":2.0},\"enabled\":true}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["id"])')
echo "TACTIC_ID=${TACTIC_ID}"

echo "7) update tactic"
curl -s -X PUT "${AUTH_HEADER[@]}" -H 'Content-Type: application/json' \
  "${BASE_URL}/admin/api/tactics/${TACTIC_ID}" \
  -d "{\"name\":\"${TACTIC_NAME}\",\"description\":\"Updated starter tactic\",\"tags\":[\"trend\"],\"parameters\":{\"lookback\":30,\"risk_reward\":2.5},\"enabled\":false}" | python3 -m json.tool

echo "8) soft delete tactic"
curl -s -X DELETE "${AUTH_HEADER[@]}" "${BASE_URL}/admin/api/tactics/${TACTIC_ID}" | python3 -m json.tool
