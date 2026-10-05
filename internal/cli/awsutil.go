package cli

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

// loadAWSConfig resolves credentials and region from the standard chain
// (env vars, shared config/profile, container/instance roles), applying
// the --region and --profile overrides.
func loadAWSConfig(ctx context.Context) (aws.Config, error) {
	var opts []func(*config.LoadOptions) error
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}
	if profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(profile))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return cfg, fmt.Errorf("loading AWS configuration: %w", err)
	}
	if cfg.Region == "" {
		return cfg, fmt.Errorf("no AWS region configured: pass --region, set AWS_REGION, or add a region to your profile")
	}
	return cfg, nil
}
