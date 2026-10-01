package basictransfer

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// TransferRequest contains the payload for the fund transfer.
type TransferRequest struct {
	FromAccountID string  `json:"from_account_id"`
	ToAccountID   string  `json:"to_account_id"`
	Amount        float64 `json:"amount"`
	ReferenceID   string  `json:"reference_id"`
}

// TransferResult contains the result of the completed transfer.
type TransferResult struct {
	TransactionID string    `json:"transaction_id"`
	Status        string    `json:"status"`
	CompletedAt   time.Time `json:"completed_at"`
}

// WithdrawActivity simulates deducting money from an account.
// Activities are where all non-deterministic logic (I/O, database calls, HTTP requests) MUST reside.
func WithdrawActivity(ctx context.Context, req TransferRequest) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Starting WithdrawActivity", "fromAccount", req.FromAccountID, "amount", req.Amount)

	// Business validation: Non-retryable error.
	// If the account has insufficient funds, retrying will NOT fix it.
	// Marking it NonRetryable tells Temporal to abort immediately rather than exhausting retry attempts.
	if req.Amount <= 0 {
		return "", temporal.NewNonRetryableApplicationError(
			"transfer amount must be greater than zero",
			"ErrInvalidAmount",
			nil,
		)
	}

	if req.FromAccountID == "ACC_EMPTY" {
		return "", temporal.NewNonRetryableApplicationError(
			"account has insufficient funds",
			"ErrInsufficientFunds",
			nil,
		)
	}

	// Simulating heartbeat for long-running or distributed activity steps:
	// If worker crashes, Temporal uses heartbeats to detect liveness before the full timeout expires.
	activity.RecordHeartbeat(ctx, "validating ledger")

	// Simulate ledger debit
	txID := fmt.Sprintf("TX-WTH-%s-%s", req.FromAccountID, req.ReferenceID)
	logger.Info("WithdrawActivity succeeded", "txID", txID)
	return txID, nil
}

// DepositActivity simulates crediting money to the destination account.
func DepositActivity(ctx context.Context, req TransferRequest) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Starting DepositActivity", "toAccount", req.ToAccountID, "amount", req.Amount)

	// Simulate simulated temporary transient network error for testing retries
	if req.ToAccountID == "ACC_TRANSIENT_FAIL" {
		// A regular Go error is treated by Temporal as RETRYABLE.
		// Temporal will automatically retry according to the Workflow's RetryPolicy.
		return "", fmt.Errorf("transient network timeout communicating with banking gateway")
	}

	txID := fmt.Sprintf("TX-DEP-%s-%s", req.ToAccountID, req.ReferenceID)
	logger.Info("DepositActivity succeeded", "txID", txID)
	return txID, nil
}
