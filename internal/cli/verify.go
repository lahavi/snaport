package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"snaport/internal/img"
	"snaport/internal/manifest"
)

var verifyFlags struct {
	manifestPath string
}

var verifyCmd = &cobra.Command{
	Use:   "verify <image.img|image.img.zst|ami-directory>",
	Short: "Verify a downloaded image (or AMI directory) against its manifest",
	Long: `Re-verify a snaport artifact:

  - raw image (.img): every block checksum plus the whole-image SHA-256;
  - compressed image (.img.zst): full decode restore-test, decoded hash
    compared with the manifest;
  - AMI directory: verifies every volume recorded in ami-manifest.json.`,
	Args: cobra.ExactArgs(1),
	RunE: runVerify,
}

func init() {
	verifyCmd.Flags().StringVar(&verifyFlags.manifestPath, "manifest", "", "manifest path (default: inferred next to the image)")
}

func runVerify(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	target := args[0]

	st, err := os.Stat(target)
	if err != nil {
		return err
	}
	if st.IsDir() {
		return verifyAMIDir(ctx, target)
	}
	return verifyOne(ctx, target, verifyFlags.manifestPath)
}

func verifyOne(ctx context.Context, path, manifestPath string) error {
	if manifestPath == "" {
		manifestPath = inferManifestPath(path)
	}
	m, err := manifest.Load(manifestPath)
	if err != nil {
		return fmt.Errorf("loading manifest: %w", err)
	}

	switch {
	case strings.HasSuffix(path, ".img.zst"):
		if m.ImageSHA256 == "" {
			return fmt.Errorf("manifest %s has no recorded image hash; cannot restore-test", manifestPath)
		}
		fmt.Printf("restore-testing %s ...\n", filepath.Base(path))
		rt, err := img.RestoreTest(ctx, path, m.ImageSHA256, m.LogicalSize)
		if err != nil {
			return err
		}
		if !rt.RestoreTestOK {
			return fmt.Errorf("restore test failed for %s", path)
		}
		fmt.Printf("PASS %s: decoded %s, sha256 matches %s\n",
			path, humanBytes(float64(rt.DecodedBytes)), m.ImageSHA256)
		return nil

	case strings.HasSuffix(path, ".img"):
		fmt.Printf("verifying %s (%d blocks) ...\n", filepath.Base(path), m.BlockCount)
		vr, err := img.Verify(ctx, path, m, nil)
		if err != nil {
			return err
		}
		if vr.BlocksChecked != m.BlockCount {
			return fmt.Errorf("verified %d blocks, manifest has %d", vr.BlocksChecked, m.BlockCount)
		}
		if m.ImageSHA256 != "" && vr.ImageSHA256 != m.ImageSHA256 {
			return fmt.Errorf("image hash %s does not match manifest %s", vr.ImageSHA256, m.ImageSHA256)
		}
		fmt.Printf("PASS %s: %d/%d blocks, sha256 %s\n", path, vr.BlocksChecked, m.BlockCount, vr.ImageSHA256)
		return nil

	default:
		return fmt.Errorf("cannot verify %s: expected a .img or .img.zst file", path)
	}
}

// inferManifestPath maps foo.img / foo.img.zst to foo.manifest.json.
func inferManifestPath(path string) string {
	for _, suffix := range []string{".img.zst", ".img"} {
		if strings.HasSuffix(path, suffix) {
			return strings.TrimSuffix(path, suffix) + ".manifest.json"
		}
	}
	return path + ".manifest.json"
}

func verifyAMIDir(ctx context.Context, dir string) error {
	topPath := filepath.Join(dir, "ami-manifest.json")
	top, err := manifest.Load(topPath)
	if err != nil {
		return fmt.Errorf("loading %s: %w (is this an AMI download directory?)", topPath, err)
	}
	if top.Kind != manifest.KindAMI {
		return fmt.Errorf("%s is not an AMI manifest", topPath)
	}
	for _, vol := range top.Volumes {
		image := filepath.Join(dir, vol.ImageFile)
		if _, err := os.Stat(image + ".zst"); err == nil {
			image += ".zst"
		} else if _, err := os.Stat(image); err != nil {
			return fmt.Errorf("volume %s: neither %s.zst nor %s found", vol.Device, image, image)
		}
		manifestPath := filepath.Join(dir, vol.Manifest)
		fmt.Printf("== %s (%s, %s)\n", vol.SnapshotID, vol.Device, filepath.Base(image))
		if err := verifyOne(ctx, image, manifestPath); err != nil {
			return fmt.Errorf("volume %s: %w", vol.Device, err)
		}
	}
	return nil
}
