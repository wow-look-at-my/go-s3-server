//go:build js || wasip1

package cacheclient

import "errors"

// canSpawn is false: js and wasip1 start no process, so Close drains.
const canSpawn = false

func spawnUploader(string) error {
	return errors.New("this platform starts no process")
}
