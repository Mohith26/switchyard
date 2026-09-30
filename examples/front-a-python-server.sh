#!/usr/bin/env bash
# Put Switchyard in front of a server that knows nothing about it: a ten-line
# Python HTTP server. Shows caching, failover when a cache node dies, and a
# zero-downtime restart under load. Needs Go 1.23+ and python3; Linux or macOS.
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o bin/switchyard ./cmd/switchyard
D=$(mktemp -d)
cat > "$D/backend.py" <<'PY'
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        body = ("hello from a plain Python server: %s\n" % self.path).encode()
        self.send_response(200)
        if self.path.startswith("/static/"):
            self.send_header("Cache-Control", "public, max-age=60")   # cacheable
        self.send_header("Content-Length", str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def log_message(self, *a): pass
ThreadingHTTPServer(("127.0.0.1", 7301), H).serve_forever()
PY
cat > "$D/edge.json" <<'JSON'
{"listen":"127.0.0.1:8301","cache_nodes":["127.0.0.1:9301","127.0.0.1:9302"],"hashing":"ring","health_interval":"200ms","failover":true}
JSON
python3 "$D/backend.py" & BP=$!
bin/switchyard cache -listen 127.0.0.1:9301 -name c1 -origins 127.0.0.1:7301 2>/dev/null & C1=$!
bin/switchyard cache -listen 127.0.0.1:9302 -name c2 -origins 127.0.0.1:7301 2>/dev/null & C2=$!
sleep 0.5
bin/switchyard edge -config "$D/edge.json" -pidfile "$D/edge.pid" & 
sleep 0.8
trap 'kill -TERM $(cat "$D/edge.pid") $C1 $C2 $BP 2>/dev/null || true' EXIT

echo "== cacheable asset: expect MISS then HIT"
for i in 1 2 3; do curl -s -D - -o /dev/null 127.0.0.1:8301/static/app.js | grep -i '^x-cache:'; done
echo "== no Cache-Control from the backend: never cached"
for i in 1 2; do curl -s -D - -o /dev/null 127.0.0.1:8301/api/me | grep -i '^x-cache:'; done

echo "== kill the cache node that owns the asset; the edge fails over"
owner=$(curl -s -D - -o /dev/null 127.0.0.1:8301/static/app.js | grep -i '^x-switchyard-node:' | tr -d '\r' | awk '{print $2}')
if [ "$owner" = "127.0.0.1:9301" ]; then kill $C1; else kill $C2; fi
sleep 0.3
curl -s -o /dev/null -w "status after the crash: %{http_code}\n" 127.0.0.1:8301/static/app.js

echo "== zero-downtime restart during 400 requests"
old=$(cat "$D/edge.pid")
(for i in $(seq 1 400); do curl -s -o /dev/null -w "%{http_code}\n" 127.0.0.1:8301/static/app.js; done > "$D/codes") & L=$!
sleep 0.2; kill -USR2 "$old"; wait $L; sleep 1.5
echo "old pid $old, new pid $(cat "$D/edge.pid"); status codes:"; sort "$D/codes" | uniq -c
