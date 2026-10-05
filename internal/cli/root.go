// Package cli wires snaport's cobra commands.
package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Version is set at build time via -ldflags "-X snaport/internal/cli.Version=x.y.z".
var Version = "dev"

// Region and Profile are persistent flags shared by commands that talk
// to AWS.
var (
	region  string
	profile string
)

var rootCmd = &cobra.Command{
	Use:   "snaport",
	Short: "Download AWS EBS snapshots/AMIs via the EBS Direct APIs into sparse, compressed local images",
	Long: `snaport downloads EC2 EBS snapshots (standalone or as part of an AMI)
using the official EBS Direct APIs (ListSnapshotBlocks/GetSnapshotBlock)
and writes them as NTFS sparse raw images: only allocated blocks consume
disk space. Images are checksum-verified block by block, restore-tested,
and compressed with zstd. Interrupted downloads resume safely.`,
	SilenceUsage:  true,
	SilenceErrors: true,
	Version:       Version,
}

func init() {
	rootCmd.PersistentFlags().StringVar(&region, "region", "", "AWS region (or AWS_REGION env / profile default)")
	rootCmd.PersistentFlags().StringVar(&profile, "profile", "", "AWS shared config profile")
	rootCmd.AddCommand(downloadCmd, verifyCmd, selftestCmd)
}

// Execute runs the CLI root command; it returns the process exit code.
func Execute() int {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	return 0
}
