# The server is an APE (Actually Portable Executable): its header is a shell
# script rather than an ELF, so the kernel cannot exec it directly. Without
# either a registered binfmt handler or a shell to interpret that header, the
# container start fails as
#
#     exec /go-s3-server: no such file or directory
#
# -- ENOENT reported against the ENTRYPOINT path, never against the shell that
# is actually missing. That misdirection is why a scratch image carrying the
# bare APE reads as a missing binary instead of a missing interpreter.
#
# The busybox MUST be static. The final image is scratch and has no /lib, so a
# busybox that names an ELF interpreter cannot start either, and fails with the
# exact same misleading ENOENT. busybox:stable-musl is static-pie; Alpine's is a
# PIE against /lib/ld-musl-x86_64.so.1 and will not do.
FROM busybox:stable-musl AS shell
RUN mkdir -p /shell && cp /bin/busybox /shell/busybox \
    && for a in $(/shell/busybox --list); do ln -sf busybox "/shell/$a"; done
# scratch ships no filesystem at all, not even /tmp. Go's os.TempDir answers
# /tmp unconditionally, so anything reaching for a temp file needs it to exist.
RUN mkdir -p /skel/tmp && chmod 1777 /skel/tmp

FROM scratch

ARG VERSION=dev

LABEL org.opencontainers.image.source="https://github.com/wow-look-at-my/go-s3-server"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.description="Minimal S3-compatible cache server"

# /tmp, mode 1777, from the skeleton above.
COPY --from=shell /skel/ /
COPY --from=gcr.io/distroless/static-debian12 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=shell /shell /bin

# The APE sits under /usr/local/lib and a shebang launcher takes its place at
# the entrypoint path. A launcher the kernel CAN exec keeps the shell an
# implementation detail of this image: a container recreated from an older
# container's stored config still names /go-s3-server, and still starts,
# instead of exiting on a bare exec of a non-ELF.
COPY --chmod=755 build/go-s3-server /usr/local/lib/go-s3-server/go-s3-server
COPY --chmod=755 scripts/image-launcher.sh /go-s3-server

ENV PATH=/usr/local/bin:/bin
ENV TMPDIR=/tmp

# This command runs in the final rootfs, so it proves the shipped shell loads
# with no /lib present, and that both halves of the entrypoint are executable.
# A shell that cannot start makes every container start fail, and the build is
# the last place that failure is cheap.
RUN ["/bin/sh", "-c", "test -x /go-s3-server && test -x /usr/local/lib/go-s3-server/go-s3-server"]

# 9000 serves the S3 API and the unauthenticated probes
# (/_health and /.well-known/docker-updater/{health,pre-update}); 9090 serves
# Prometheus metrics. Two exposed ports means docker-updater's endpoint
# discovery cannot guess which one speaks the contract, so a deployment sets
# docker-updater.well-known.port=9000 to name it.
EXPOSE 9000
EXPOSE 9090

# The drain in main.go hangs off SIGTERM: it flips /_health to 503 and then
# gives in-flight GET/PUT streams up to 280s to finish. A container stopped
# with anything else loses that entirely.
STOPSIGNAL SIGTERM

ENTRYPOINT ["/go-s3-server"]
