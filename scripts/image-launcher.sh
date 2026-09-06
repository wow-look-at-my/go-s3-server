#!/bin/sh
# The image installs this at /go-s3-server, and the APE beside it under
# /usr/local/lib. The kernel cannot exec an APE: the file's header is a shell
# script, not an ELF, and the image registers no binfmt handler. A shebang
# script IS execable, so every spelling of the entrypoint reaches the binary,
# including the one an older container recorded in its stored config.
exec /bin/sh /usr/local/lib/go-s3-server/go-s3-server "$@"
