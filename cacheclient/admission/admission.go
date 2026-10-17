// Package admission holds the server's admission limits that the client also sizes itself by.
package admission

import "time"

// Wait is how long the server queues a request for a work slot before it sheds it.
const Wait = 10 * time.Second
