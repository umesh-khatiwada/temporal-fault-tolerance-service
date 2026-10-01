package childworkflows

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// ItemProcessorChildWorkflow is a child workflow that handles a single item.
// Using child workflows isolates failures and keeps parent workflow history compact.
func ItemProcessorChildWorkflow(ctx workflow.Context, item BatchItem) (*ProcessItemResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("ChildWorkflow started for item", "itemID", item.ItemID)

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	var res ProcessItemResult
	err := workflow.ExecuteActivity(ctx, ProcessSingleItemActivity, item).Get(ctx, &res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

type BatchParentResult struct {
	TotalProcessed int                  `json:"total_processed"`
	Items          []*ProcessItemResult `json:"items"`
}

// BatchParentWorkflow coordinates multiple Child Workflows concurrently.
func BatchParentWorkflow(ctx workflow.Context, items []BatchItem) (*BatchParentResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("BatchParentWorkflow started", "totalItems", len(items))

	childFutures := make([]workflow.ChildWorkflowFuture, len(items))

	// Launch all Child Workflows concurrently
	for i, item := range items {
		childOptions := workflow.ChildWorkflowOptions{
			WorkflowID: fmt.Sprintf("child-item-%s", item.ItemID),
		}
		childCtx := workflow.WithChildOptions(ctx, childOptions)

		// ExecuteChildWorkflow returns a ChildWorkflowFuture immediately (non-blocking)
		childFutures[i] = workflow.ExecuteChildWorkflow(childCtx, ItemProcessorChildWorkflow, item)
	}

	// Await completion of all child workflows
	results := make([]*ProcessItemResult, len(items))
	for i, future := range childFutures {
		var itemResult ProcessItemResult
		err := future.Get(ctx, &itemResult)
		if err != nil {
			logger.Error("Child workflow failed", "index", i, "error", err)
			return nil, fmt.Errorf("child workflow %d failed: %w", i, err)
		}
		results[i] = &itemResult
	}

	logger.Info("BatchParentWorkflow completed successfully", "processedCount", len(results))
	return &BatchParentResult{
		TotalProcessed: len(results),
		Items:          results,
	}, nil
}

type RecurringState struct {
	CurrentIteration int `json:"current_iteration"`
	MaxIterations    int `json:"max_iterations"`
}

// RecurringMonitorWorkflow demonstrates CONTINUE-AS-NEW.
// In Temporal, every action appends to the Workflow Event History.
// For workflows meant to run indefinitely (polling, recurring cleanup, bots),
// calling workflow.NewContinueAsNewError resets the Event History to keep memory & performance optimal!
func RecurringMonitorWorkflow(ctx workflow.Context, state RecurringState) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("RecurringMonitorWorkflow iteration started", "iteration", state.CurrentIteration)

	// Simulate work for this cycle
	// Durable sleep (does NOT block OS thread, server wakes up worker when timer elapses)
	err := workflow.Sleep(ctx, 1*time.Second)
	if err != nil {
		return "", err
	}

	state.CurrentIteration++

	if state.CurrentIteration >= state.MaxIterations {
		logger.Info("Reached max iterations, terminating cleanly", "total", state.CurrentIteration)
		return fmt.Sprintf("Completed %d iterations", state.CurrentIteration), nil
	}

	// Reset event history and continue execution as a new run!
	logger.Info("History threshold reached: Continuing as new run", "nextIteration", state.CurrentIteration)
	return "", workflow.NewContinueAsNewError(ctx, RecurringMonitorWorkflow, state)
}
