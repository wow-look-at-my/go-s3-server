# Starts the container image and requires it to serve. It lives here rather
# than in dats/ because it needs docker, which the sandbox go-toolchain runs
# dats/ under does not have: CI invokes this one with --no-sandbox.
#
# The sibling service published an image whose entrypoint could not start at
# all, and the first execution of that entrypoint was production. So publishing
# is gated on this suite, and the last test is a negative control: the same
# image with an exec-form entrypoint must NOT start, or these checks would pass
# for an image that merely happens to work.

# The build is SETUP, not a test. dats runs tests concurrently, so a build
# written as test 1 races every test that needs the image, and they fail with
# "no such image" rather than on anything they assert. A setup failure fails
# the whole run, so the build is still gated.
setup: |
	set -eu
	test -f build/go-s3-server || { echo "build/go-s3-server is missing; the build job produces it" >&2; exit 1; }
	docker build -t go-s3-server:smoke . >/dev/null

tests:
	- desc: the setup produced an image
	  cmd: |
		set -eu
		docker image inspect -f 'PRESENT' go-s3-server:smoke
	  outputs:
		stdout:
			- "PRESENT"

	- desc: the image declares no VOLUME, so a missing bind mount fails visibly
	  cmd: |
		set -eu
		vols=$(docker image inspect -f '{{ len .Config.Volumes }}' go-s3-server:smoke)
		echo "volumes=$vols"
	  outputs:
		stdout:
			- "volumes=0"

	- desc: a container from it starts and serves the health probe
	  cmd: |
		set -eu
		# 2>&1 inside the capture so a daemon refusal lands in $cid and gets
		# printed. Without it a failing run reports only its exit status.
		if ! cid=$(docker run -d -p 18099:8080 -e CACHE_USERNAME=smoke -e CACHE_PASSWORD=smoke go-s3-server:smoke 2>&1); then
		echo "docker run failed: $cid" >&2
		exit 1
		fi
		trap 'docker rm -f "$cid" >/dev/null 2>&1 || true' EXIT
		for _ in $(seq 1 30); do
		if curl -fsS -o /dev/null http://127.0.0.1:18099/_health; then echo "SERVING"; exit 0; fi
		sleep 1
		done
		echo "the container never served /_health; logs follow" >&2
		docker logs "$cid" >&2
		exit 1
	  outputs:
		stdout:
			- "SERVING"

	- desc: the update-check probes answer, so docker-updater can roll it
	  cmd: |
		set -eu
		if ! cid=$(docker run -d -p 18100:8080 -e CACHE_USERNAME=smoke -e CACHE_PASSWORD=smoke go-s3-server:smoke 2>&1); then
		echo "docker run failed: $cid" >&2
		exit 1
		fi
		trap 'docker rm -f "$cid" >/dev/null 2>&1 || true' EXIT
		for _ in $(seq 1 30); do curl -fsS -o /dev/null http://127.0.0.1:18100/_health && break; sleep 1; done
		curl -fsS -o /dev/null http://127.0.0.1:18100/.well-known/docker-updater/health
		curl -fsS -o /dev/null http://127.0.0.1:18100/.well-known/docker-updater/pre-update
		echo "PROBES ok"
	  outputs:
		stdout:
			- "PROBES ok"

	- desc: the auth gate refuses an unauthenticated bucket read
	  cmd: |
		set -eu
		if ! cid=$(docker run -d -p 18101:8080 -e CACHE_USERNAME=smoke -e CACHE_PASSWORD=smoke go-s3-server:smoke 2>&1); then
		echo "docker run failed: $cid" >&2
		exit 1
		fi
		trap 'docker rm -f "$cid" >/dev/null 2>&1 || true' EXIT
		for _ in $(seq 1 30); do curl -fsS -o /dev/null http://127.0.0.1:18101/_health && break; sleep 1; done
		anon=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:18101/gobuildcache/_index)
		auth=$(curl -s -o /dev/null -w '%{http_code}' -u smoke:smoke http://127.0.0.1:18101/gobuildcache/_index)
		echo "anon=$anon auth=$auth"
	  outputs:
		stdout:
			- "anon=403 auth=200"

	# The negative control. An exec-form entrypoint execve()s the APE, which the
	# kernel refuses, so this variant must fail to start. If it starts, the tests
	# above prove nothing about why the real entrypoint works.
	- desc: an exec-form entrypoint cannot start the same binary
	  cmd: |
		set -eu
		printf 'FROM go-s3-server:smoke\nENTRYPOINT ["/usr/local/bin/go-s3-server"]\n' > Dockerfile.execform
		if ! out=$(docker build -f Dockerfile.execform -t go-s3-server:execform . 2>&1); then
		echo "building the exec-form variant failed: $out" >&2
		exit 1
		fi
		if docker run --rm go-s3-server:execform --help >/dev/null 2>&1; then
		echo "an exec-form entrypoint started the APE, so this suite cannot detect a broken one" >&2
		exit 1
		fi
		echo "EXEC FORM REFUSED"
	  outputs:
		stdout:
			- "EXEC FORM REFUSED"
