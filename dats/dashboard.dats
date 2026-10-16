$schema: https://github.com/wow-look-at-my/dats/schema.json

shared:
	files:
		serve.sh: |
			# Runs a single check script against a server this starts and
			# stops. usage: serve.sh <config.json> <check.sh>
			# The config listens on port 0, so the kernel picks a free port and
			# the server log names it. Concurrent runs never share a server.
			# The check reads the ports from $PORT and $DASHBOARD_PORT, and the
			# server log from $SERVER_LOG.
			set -euo pipefail
			config="$1"
			check="$2"
			log="$(mktemp)"
			"${SERVER:-./build/go-s3-server}" --config "$config" > "$log" 2>&1 &
			server=$!
			trap 'kill "$server" 2>/dev/null || true; wait "$server" 2>/dev/null || true' EXIT
			wait_port() {
				local p="$1" path="$2" ok=""
				for _ in $(seq 1 100); do
					if curl -so /dev/null "http://127.0.0.1:$p$path"; then ok=yes; break; fi
					if ! kill -0 "$server" 2>/dev/null; then break; fi
					sleep 0.1
				done
				if [ -z "$ok" ]; then
					echo "the server never answered on port $p"
					while IFS= read -r line; do echo "server: $line"; done < "$log"
					return 1
				fi
			}
			# The server binds the dashboard before the cache port, so the
			# dashboard line is in the log when the cache line is.
			port_re='listening on [^ ]*:([0-9]+) bucket='
			dash_re='dashboard listening on [^ ]*:([0-9]+);'
			PORT=""
			DASHBOARD_PORT=""
			for _ in $(seq 1 100); do
				while IFS= read -r line; do
					if [[ "$line" =~ $port_re ]]; then PORT="${BASH_REMATCH[1]}"; fi
					if [[ "$line" =~ $dash_re ]]; then DASHBOARD_PORT="${BASH_REMATCH[1]}"; fi
				done < "$log"
				if [ -n "$PORT" ] || ! kill -0 "$server" 2>/dev/null; then break; fi
				sleep 0.1
			done
			if [ -z "$PORT" ]; then
				echo "the server log never named its port"
				while IFS= read -r line; do echo "server: $line"; done < "$log"
				exit 1
			fi
			export PORT DASHBOARD_PORT
			export SERVER_LOG="$log"
			wait_port "$PORT" /_health
			# The dashboard is another listener, so the cache port answering says nothing about it.
			if [ -n "$DASHBOARD_PORT" ]; then
				wait_port "$DASHBOARD_PORT" /
			fi
			# A failing check gets the server's log too. Without this a check
			# that cannot reach a server which HAD answered reports only its own
			# exit status. And the a single process that knows why says nothing.
			status=0
			bash "$check" || status=$?
			# On stdout, because that is what a failing test reports back.
			if [ "$status" -ne 0 ]; then
				echo "the check exited $status"
				kill -0 "$server" 2>/dev/null || echo "the server had already exited"
				while IFS= read -r line; do echo "server: $line"; done < "$log"
			fi
			exit "$status"

tests:
	- desc: the dashboard script parses as the module the page loads it as
	  exit: 0
	  cmd: bash -c 'f="$(mktemp --suffix=.mjs)"; cp dashboard.js "$f"; node --check "$f" && echo parses'
	  outputs:
		stdout:
			- "parses"

	- desc: the dashboard answers on its own port with no credentials, while the cache port still demands them
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:0","dashboard_listen":"127.0.0.1:0","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				dash=http://127.0.0.1:$DASHBOARD_PORT
				# The dashboard binds in its own goroutine, so the cache port
				# answering does not mean this port is up yet. Say so when it
				# never opens, rather than ending the script with no output.
				up=""
				for _ in $(seq 1 100); do
					if curl -so /dev/null "$dash/_health"; then up=yes; break; fi
					sleep 0.1
				done
				if [ -z "$up" ]; then
					echo "the dashboard never opened $DASHBOARD_PORT"
					exit 1
				fi
				echo "cache-anonymous $(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/test-cache/anon/v1test000000000001")"
				echo "page $(curl -s -o /dev/null -w '%{http_code}' "$dash/")"
				echo "css $(curl -s -o /dev/null -w '%{http_code}' "$dash/dashboard.css")"
				echo "js $(curl -s -o /dev/null -w '%{http_code}' "$dash/dashboard.js")"
				echo "stats $(curl -s -o /dev/null -w '%{http_code}' "$dash/api/stats")"
				curl -sf "$dash/" | grep -o '<title>[^<]*</title>'
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
	  outputs:
		stdout:
			- "cache-anonymous 403"
			- "page 200"
			- "css 200"
			- "js 200"
			- "stats 200"
			- "<title>go build cache</title>"

	- desc: the stats snapshot reports the traffic the cache port actually served
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:0","dashboard_listen":"127.0.0.1:0","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:$PORT/test-cache
				auth=testuser:testpass
				# A cacheprog-keyed body goes through the read guards, which
				# reject this plain text. Store a single to fill the index, and
				# read a plain key back for the hit.
				curl -sf -u "$auth" -X PUT --data-binary 'x' "$base/go-buildcache/v1$(printf 'a%.0s' $(seq 64))" > /dev/null
				curl -sf -u "$auth" -X PUT --data-binary 'dashboard body' "$base/plain/v1test000000000001" > /dev/null
				curl -sf -u "$auth" "$base/plain/v1test000000000001" > /dev/null
				stats="$(mktemp)"
				code=$(curl -s -o "$stats" -w '%{http_code}' "http://127.0.0.1:$DASHBOARD_PORT/api/stats")
				if [ "$code" != "200" ]; then
					echo "the stats endpoint answered $code"
					exit 1
				fi
				grep -o '"bucket": "test-cache"' "$stats"
				grep -A2 '"outcome": "hit"' "$stats" | grep -o '"value": 1'
				grep -o '"s3_index_hashes"' "$stats"
				echo "no-password $(grep -c testpass "$stats" || true)"
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
	  outputs:
		stdout:
			- '"bucket": "test-cache"'
			- '"value": 1'
			- '"s3_index_hashes"'
			- "no-password 0"

	- desc: an empty dashboard_listen opens no dashboard listener
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:0","dashboard_listen":"","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				# The server logs each listener it binds, so its log is the list
				# of open ports.
				echo "dashboard-port ${DASHBOARD_PORT:-none}"
				grep -o 'dashboard disabled' "$SERVER_LOG"
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
	  outputs:
		stdout:
			- "dashboard-port none"
			- "dashboard disabled"
		"!stdout":
			- "dashboard listening on"

	- desc: the bandwidth endpoint reports the served bytes as module bands, a remainder, and a separate index series
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:0","dashboard_listen":"127.0.0.1:0","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:$PORT/test-cache
				auth=testuser:testpass
				# Two modules' bodies. The objects carry no module metadata, so
				# the attribution is the header the client sends about itself,
				# which is the derivation the access log's projects already use.
				curl -sf -u "$auth" -X PUT -H 'X-Cache-Module: example.com/one' --data-binary 'aaaa' "$base/plain/v1one0000000000001" > /dev/null
				curl -sf -u "$auth" -H 'X-Cache-Module: example.com/one' "$base/plain/v1one0000000000001" > /dev/null
				curl -sf -u "$auth" -X PUT -H 'X-Cache-Module: example.com/two' --data-binary 'bbbbbbbb' "$base/plain/v1two0000000000001" > /dev/null
				curl -sf -u "$auth" -H 'X-Cache-Module: example.com/two' "$base/plain/v1two0000000000001" > /dev/null
				# An index blob, pulled whole, is the series that belongs to no
				# module: a whole fleet reads the same one.
				curl -sf -u "$auth" -X PUT --data-binary 'x' "$base/go-buildcache/v1$(printf 'a%.0s' $(seq 64))" > /dev/null
				curl -sf -u "$auth" "$base/_index" -o /dev/null
				series="$(mktemp)"
				curl -sf "http://127.0.0.1:$DASHBOARD_PORT/api/bandwidth" -o "$series"
				# The window is fixed, so the chart's axis does not move.
				grep -q '"bucket_seconds": 1' "$series" && echo "bucket-seconds 1" || echo "bucket-seconds other"
				grep -q '"retention_seconds": 300' "$series" && echo "retention-seconds 300" || echo "retention-seconds other"
				# One point per bucket, filled in whether or not it served anything.
				echo "points $(grep -c '"t":' "$series" || true)"
				grep -q '"(other)"' "$series" && echo "remainder band" || echo "remainder missing"
				# Each module's band is exactly the body the GET sent it, so the
				# index bytes were not folded into it.
				one="$(sed -n 's/.*"example.com\/one": \([0-9][0-9]*\).*/\1/p' "$series" | sort -n | tail -1 || true)"
				two="$(sed -n 's/.*"example.com\/two": \([0-9][0-9]*\).*/\1/p' "$series" | sort -n | tail -1 || true)"
				echo "module-one ${one:-0} bytes"
				echo "module-two ${two:-0} bytes"
				index="$(sed -n 's/.*"index": \([0-9][0-9]*\).*/\1/p' "$series" | sort -n | tail -1 || true)"
				test "${index:-0}" -gt 0 && echo "index-series bytes" || echo "index-series empty"
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
	  outputs:
		stdout:
			- "bucket-seconds 1"
			- "retention-seconds 300"
			- "points 300"
			- "remainder band"
			- "module-one 4 bytes"
			- "module-two 8 bytes"
			- "index-series bytes"
