package basictransfer

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// TransferWorkflow orchestrates a two-step fund transfer.
//
// GOLDEN RULES OF TEMPORAL WORKFLOWS:
// 1. MUST BE DETERMINISTIC: Executing the same code against the same history MUST take the same code path.
// 2. NO direct I/O: Never make HTTP requests, DB queries, or file writes inside a Workflow. Use Activities!
// 3. NO standard concurrency primitives: Do NOT use standard `go routine`, `time.Sleep`, or `time.Now()`.
//    Use `workflow.Go()`, `workflow.Sleep()`, and `workflow.Now(ctx)`.
func TransferWorkflow(ctx workflow.Context, req TransferRequest) (*TransferResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("TransferWorkflow started", "refID", req.ReferenceID, "amount", req.Amount)

	// Configure Activity execution parameters:
	// - StartToCloseTimeout: Maximum time a single attempt of this Activity can run.
	// - ScheduleToCloseTimeout: Maximum total time for the Activity including waiting in queue & retries.
	// - RetryPolicy: Controls exponential backoff.
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    1 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    3,
			NonRetryableErrorTypes: []string{
				"ErrInvalidAmount",
				"ErrInsufficientFunds",
			},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Withdraw funds from source account
	var withdrawTxID string
	err := workflow.ExecuteActivity(ctx, WithdrawActivity, req).Get(ctx, &withdrawTxID)
	if err != nil {
		logger.Error("WithdrawActivity failed", "error", err)
		return nil, err
	}

	// Step 2: Deposit funds into destination account
	var depositTxID string
	err = workflow.ExecuteActivity(ctx, DepositActivity, req).Get(ctx, &depositTxID)
	if err != nil {
		logger.Error("DepositActivity failed", "error", err)
		// NOTE: In Module 03, we explore the Saga pattern to automatically compensate (refund)
		// the withdrawal if deposit fails.
		return nil, err
	}

	result := &TransferResult{
		TransactionID: withdrawTxID + "|" + depositTxID,
		Status:        "COMPLETED",
		CompletedAt:   workflow.Now(ctx), // Deterministic timestamp provided by Temporal history
	}

	logger.Info("TransferWorkflow completed successfully", "txID", result.TransactionID)
	return result, nil
}
