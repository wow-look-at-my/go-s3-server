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
	# The client and the server are one protocol, so the only honest check of it is a real
	# build against a real server. A build whose local cache was wiped must get its outputs
	# back over the wire; anything less and the shared tier is decoration.
	- desc: a build with a cold local cache gets its outputs back from the server
	  exit: 0
	  timeout: 900
	  inputs:
		env:
			DATA_DIR: "{outputs.data}"
			PROJECT_GO_MOD: "{inputs.project/go.mod}"
		files:
			config.json: '{"listen":"127.0.0.1:19040","metrics_listen":"127.0.0.1:19041","bucket":"test-cache","data_dir":"{outputs.data}","write_once":{"action":"allow"},"disable_auth":true}'
			# An old directive on purpose: the fixture must build under whatever go the
			# sandbox has, and it asks nothing of the language.
			project/go.mod: |
				module example.com/test

				go 1.21
			project/main.go: |
				package main

				import "fmt"

				func main() { fmt.Println(greet("world")) }

				func greet(name string) string { return "hello, " + name }
			check.sh: |
				set -euo pipefail

				# The sandbox does not carry the caller's cache directory, so an inherited
				# GOROOT can name a toolchain that is not here. Let go find its own root,
				# and keep it from fetching another one.
				unset GOROOT
				export GOTOOLCHAIN=local

				# Its own cache roots, so the wipe below cannot reach the host's and the run
				# cannot be answered by whatever the host already had.
				root="$(mktemp -d)"
				export GOPATH="$root/gopath"
				export GOCACHE="$root/gocache"
				export XDG_CACHE_HOME="$root/xdg"
				export GOFLAGS=-mod=mod
				export GO_BUILDCACHE_CONFIG=$(printf '%s' '{"endpoint":"http://127.0.0.1:19040","bucket":"test-cache","username":"anyone","password":"unchecked"}' | base64 -w0)

				# The cache protocol is the whole subject, so the build speaks it directly:
				# cmd/go hands every action to the client, which talks to the server started
				# above. Nothing else about the build matters here.
				export GOCACHEPROG="go-toolchain cacheprog"

				# What the server says it handed out. Counting here rather than reading the
				# client's log keeps the verdict on this repository's own contract: the
				# client renames its counters (s3 -> web already happened once).
				served() {
					curl -sf http://127.0.0.1:19041/metrics |
						awk '/^s3_batch_keys_total\{kind="streamed"\}/ || /^s3_get_requests_total\{outcome="hit"\}/ { n += $2 } END { print n + 0 }'
				}

				build() {
					mkdir -p "$1"
					cp "$(dirname "$PROJECT_GO_MOD")"/* "$1"/
					(cd "$1" && go build -o /dev/null ./...)
				}

				# The cache objects the server holds, which is not every file under the
				# data directory: the server writes its own version stamp there before a
				# client ever connects, and counting that reports success on an empty
				# cache.
				stored() {
					find "$DATA_DIR/go-buildcache" -type f 2> /dev/null | wc -l
				}

				# Each step ends in its own exit code, and dats prints the code it got even
				# when it prints nothing else -- which is how this suite runs inside a
				# build. Never `step && echo`: bash exempts everything left of && from
				# `set -e`, so a failed step would carry on and report a later stage.
				command -v go > /dev/null || exit 21
				command -v go-toolchain > /dev/null || exit 22
				echo "found-tools yes"

				go version > /dev/null || exit 23
				go env GOROOT > /dev/null || exit 24
				echo "go-runs yes"

				build "$root/first" || exit 25
				echo "cold-build-ran yes"

				test "$(stored)" -gt 0 || exit 26
				echo "cold-build-populated-the-server yes"
				before=$(served)

				# The second build is the one under test, and it is a second MACHINE, not a
				# rerun: the same sources in a directory that has never been built, with no
				# local cache left. Anything it does not recompile came over the wire.
				rm -rf "$XDG_CACHE_HOME/go-toolchain/buildcache" "$GOCACHE"
				build "$root/second" || exit 27
				echo "warm-build-ran yes"
				after=$(served)

				echo "served-before $before"
				echo "served-after $after"
				test "$after" -gt "$before" || exit 28
				echo "server-served-the-warm-build yes"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19040 {inputs.check.sh}
	  outputs:
		stdout:
			- "found-tools yes"
			- "go-runs yes"
			- "cold-build-ran yes"
			- "cold-build-populated-the-server yes"
			- "warm-build-ran yes"
			- "server-served-the-warm-build yes"
