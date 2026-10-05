// Command snaport downloads AWS EBS snapshots/AMIs via the EBS Direct
// APIs into sparse, verified, compressed local images.
package main

import (
	"os"

	"snaport/internal/cli"
)

func main() {
	os.Exit(cli.Execute())
}
