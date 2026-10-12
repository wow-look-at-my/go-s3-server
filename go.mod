module github.com/wow-look-at-my/go-s3-server

go 1.26.0

require (
	github.com/pierrec/lz4/v4 v4.1.27
	github.com/prometheus/client_golang v1.25.0
	github.com/spf13/cobra v1.10.2
	github.com/stretchr/testify v1.12.1
	github.com/wow-look-at-my/go-containers v0.0.0 // go-toolchain:auto-branch
	github.com/wow-look-at-my/go-ipc v0.0.0 // go-toolchain:auto-branch
	github.com/wow-look-at-my/go-s3-server/cacheclient v0.0.0 // go-toolchain:auto-branch
	golang.org/x/sys v0.49.0
)

// The client lives in this repository, so its tests run against the tree, never
// a published version: protocol and server change together in one commit.
replace github.com/wow-look-at-my/go-s3-server/cacheclient => ./cacheclient

// cobra names v3.0.4, whose go.mod requires gopkg.in/check.v1. Without this the
// build reads gopkg.in, and a gopkg.in outage fails go mod tidy.
exclude go.yaml.in/yaml/v3 v3.0.4

require (
	github.com/klauspost/compress v1.20.1
	github.com/prometheus/client_model v0.6.3
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/common v0.72.1 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	github.com/wow-look-at-my/go-mmap v0.0.0 // indirect
	github.com/wow-look-at-my/go-shm v0.0.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
