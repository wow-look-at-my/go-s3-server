$schema: https://github.com/wow-look-at-my/dats/schema.json

shared:
	files:
		serve.sh: |
			# Runs one check script against a server this starts and stops.
			# usage: serve.sh <config.json> <port> <check.sh>
			set -euo pipefail
			config="$1"
			port="$2"
			check="$3"
			log="$(mktemp)"
			"${SERVER:-./build/go-s3-server}" --config "$config" > "$log" 2>&1 &
			server=$!
			trap 'kill "$server" 2>/dev/null || true; wait "$server" 2>/dev/null || true' EXIT
			ready=""
			for _ in $(seq 1 100); do
				if curl -so /dev/null "http://127.0.0.1:$port/_health"; then ready=yes; break; fi
				if ! kill -0 "$server" 2>/dev/null; then break; fi
				sleep 0.1
			done
			if [ -z "$ready" ]; then
				echo "the server never answered on port $port" >&2
				sed 's/^/server: /' "$log" >&2
				exit 1
			fi
			bash "$check"

tests:
	- desc: the dashboard answers on its own port with no credentials, while the cache port still demands them
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19030","dashboard_listen":"127.0.0.1:19130","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				dash=http://127.0.0.1:19130
				echo "cache-anonymous $(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:19030/test-cache/anon/v1test000000000001)"
				echo "page $(curl -s -o /dev/null -w '%{http_code}' "$dash/")"
				echo "css $(curl -s -o /dev/null -w '%{http_code}' "$dash/dashboard.css")"
				echo "js $(curl -s -o /dev/null -w '%{http_code}' "$dash/dashboard.js")"
				echo "stats $(curl -s -o /dev/null -w '%{http_code}' "$dash/api/stats")"
				curl -sf "$dash/" | grep -o '<title>[^<]*</title>'
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19030 {inputs.check.sh}
	  outputs:
		stdout:
			- "cache-anonymous 403"
			- "page 200"
			- "css 200"
			- "js 200"
			- "stats 200"
			- "<title>go cache server</title>"

	- desc: the stats snapshot reports the traffic the cache port actually served
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19031","dashboard_listen":"127.0.0.1:19131","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19031/test-cache
				auth=testuser:testpass
				key="go-buildcache/v1$(printf 'a%.0s' $(seq 64))"
				curl -sf -u "$auth" -X PUT --data-binary 'dashboard body' "$base/$key" > /dev/null
				curl -sf -u "$auth" "$base/$key" > /dev/null
				stats="$(mktemp)"
				curl -sf http://127.0.0.1:19131/api/stats -o "$stats"
				grep -o '"bucket": "test-cache"' "$stats"
				grep -o '"hit": 1' "$stats"
				grep -o '"s3_index_hashes"' "$stats"
				echo "no-password $(grep -c testpass "$stats" || true)"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19031 {inputs.check.sh}
	  outputs:
		stdout:
			- '"bucket": "test-cache"'
			- '"hit": 1'
			- '"s3_index_hashes"'
			- "no-password 0"

	- desc: an empty dashboard_listen leaves no dashboard port open
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19032","dashboard_listen":"","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://127.0.0.1:19132/ || true)
				echo "dashboard-port ${code:-000}"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19032 {inputs.check.sh}
	  outputs:
		stdout:
			- "dashboard-port 000"
