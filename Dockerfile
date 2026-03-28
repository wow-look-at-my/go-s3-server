FROM scratch
ARG TARGETARCH
COPY go-s3-server_linux_${TARGETARCH} /go-s3-server
ENTRYPOINT ["/go-s3-server"]
