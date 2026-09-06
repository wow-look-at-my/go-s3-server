// Command go-s3-server is the shared build cache server for go-toolchain.
//
// The root of the repository holds this entry point and nothing else. The
// server itself is internal/cacheserver, one package, so a reader opening the
// repository sees the program's shape instead of a directory of source files.
package main

import "github.com/wow-look-at-my/go-s3-server/internal/cacheserver"

func main() { cacheserver.Main() }
