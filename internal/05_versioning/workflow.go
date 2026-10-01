package versioning

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// LegacyActivity represents the original step
func LegacyActivity(ctx context.Context, input string) (string, error) {
	activity.GetLogger(ctx).Info("LegacyActivity executed", "input", input)
	return "LEGACY:" + input, nil
}

// ModernFraudCheckActivity represents a new step introduced in version 1
func ModernFraudCheckActivity(ctx context.Context, input string) (bool, error) {
	activity.GetLogger(ctx).Info("ModernFraudCheckActivity executed", "input", input)
	return true, nil // Passed fraud check
}

type VersionedWorkflowResult struct {
	VersionUsed  int    `json:"version_used"`
	Output       string `json:"output"`
	FraudChecked bool   `json:"fraud_checked"`
}

// EvolvingWorkflow demonstrates safe workflow code evolution using workflow.GetVersion.
//
// In Temporal, you NEVER abruptly modify workflow branches or insert new activities
// without versioning, because active workflows replaying from history will fail with
// non-deterministic panics!
func EvolvingWorkflow(ctx workflow.Context, input string) (*VersionedWorkflowResult, error) {
	logger := workflow.GetLogger(ctx)

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Check version for "fraud-check-feature"
	// - For historical runs that already passed this point: GetVersion returns workflow.DefaultVersion (-1)
	// - For new executions: GetVersion records version 1 in history and returns 1
	version := workflow.GetVersion(ctx, "fraud-check-feature", workflow.DefaultVersion, 1)

	var fraudChecked bool
	if version == 1 {
		logger.Info("Executing new V1 path with fraud check")
		err := workflow.ExecuteActivity(ctx, ModernFraudCheckActivity, input).Get(ctx, &fraudChecked)
		if err != nil {
			return nil, err
		}
	} else {
		logger.Info("Executing legacy path without fraud check")
	}

	// Step 2: Execute common legacy activity
	var output string
	err := workflow.ExecuteActivity(ctx, LegacyActivity, input).Get(ctx, &output)
	if err != nil {
		return nil, err
	}

	return &VersionedWorkflowResult{
		VersionUsed:  int(version),
		Output:       output,
		FraudChecked: fraudChecked,
	}, nil
}
