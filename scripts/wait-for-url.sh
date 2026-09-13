# tiny helper used by healthchecks (optional)

#!/usr/bin/env bash
# wait-for-url.sh http://localhost:8000/health 60
url="$1"; timeout="${2:-60}"
for i in $(seq 1 "$timeout"); do
  if curl -fsS "$url" >/dev/null 2>&1; then exit 0; fi
  sleep 1
done
echo "Timed out waiting for $url" >&2
exit 1
