# quick curl to API

#!/usr/bin/env bash
set -euo pipefail
curl -sS -X POST http://localhost:8000/event \
  -H "content-type: application/json" \
  -d '{"source":"api","value":42,"category":"demo"}' | jq .
