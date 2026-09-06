# The binary is built by go-toolchain in CI and downloaded into build/ by the
# publish-ghcr reusable workflow before this image is built -- it is NOT compiled
# here. go-toolchain writes one file, named for the module: build/go-s3-server.
#
# An APE needs a SHELL to start. The kernel refuses the file, which begins with
# the DOS/shell magic rather than an ELF header, so a shell runs its boot script
# and that stages a runnable copy under /tmp.
# see https://github.com/wow-look-at-my/gosmopolitan/blob/master/docs/APE-STAGING.md
#
# distroless ships no shell, so busybox supplies one. busybox also prepares the
# two writable directories, because distroless has no shell to run mkdir in: the
# cache data_dir, and /tmp, which APE staging writes to.
FROM busybox:musl AS shell
# The applet links are made by hand, RELATIVE. `busybox --install -s` writes
# each one as an absolute path to where it ran, so every link would dangle once
# this directory is copied to /bin below.
RUN mkdir -p /out/bin /out/data /out/tmp \
 && cp /bin/busybox /out/bin/busybox \
 && cd /out/bin \
 && for a in $(./busybox --list); do [ "$a" = busybox ] || ln -sf busybox "$a"; done \
 && chown 65532:65532 /out/data /out/tmp \
 && touch /out/tmp/.keep

FROM gcr.io/distroless/static-debian12:nonroot

ARG VERSION=dev

LABEL org.opencontainers.image.source="https://github.com/wow-look-at-my/go-s3-server"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.description="Shared Go build cache server for go-toolchain"

COPY --from=shell /out/bin/ /bin/
COPY --chmod=755 build/go-s3-server /usr/local/bin/go-s3-server
COPY docker/config.json /etc/go-s3-server/config.json
COPY --from=shell --chown=65532:65532 /out/data /var/lib/go-s3-server
COPY --from=shell --chown=65532:65532 /out/tmp /tmp

# The cache bodies live at /var/lib/go-s3-server. The deploy BIND-MOUNTS a host
# directory there:
#
#   docker run -v /srv/go-s3-server:/var/lib/go-s3-server ...
#
# There is deliberately no VOLUME instruction. It buys nothing when the deploy
# mounts the path, and when the deploy forgets, it hides the mistake behind an
# anonymous volume that a container recreate orphans -- which is the cache loss
# it looks like it is preventing. Without a mount the cache is gone on restart,
# and every client pays a full rebuild. That must fail visibly.

# 8080 serves the cache protocol; 9090 serves Prometheus metrics. Declaring the
# serving port is also what lets docker-updater find the /.well-known probes
# without the deploy naming a port for it.
EXPOSE 8080
EXPOSE 9090

STOPSIGNAL SIGTERM

USER nonroot

# Through the shell ON PURPOSE. An exec-form entrypoint execve()s the file
# directly, and the kernel cannot exec an APE. The boot script execs the staged
# copy, so the server still replaces the shell as PID 1 and SIGTERM reaches it
# -- which is what the 280s drain in main.go depends on.
#
# The baked config reads its credentials from CACHE_USERNAME and CACHE_PASSWORD,
# so an image carries no secret. Override --config to supply another one.
ENTRYPOINT ["/bin/sh", "/usr/local/bin/go-s3-server", "--config", "/etc/go-s3-server/config.json"]
