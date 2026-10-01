package signalsqueries

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// Workflow Signal, Query, and Update identifiers
const (
	SignalNameApproval   = "signal_order_approval"
	QueryNameOrderState  = "query_order_state"
	UpdateNameAddress    = "update_shipping_address"
	TaskQueueOrders      = "ORDER_PROCESSING_TASK_QUEUE"
)

// OrderRequest input for the workflow
type OrderRequest struct {
	OrderID         string        `json:"order_id"`
	CustomerEmail   string        `json:"customer_email"`
	ShippingAddress string        `json:"shipping_address"`
	Amount          float64       `json:"amount"`
	ApprovalTimeout time.Duration `json:"approval_timeout"` // e.g. 24 hours in prod, seconds in test
}

// ApprovalSignal payload sent by external manager/customer
type ApprovalSignal struct {
	Approved  bool   `json:"approved"`
	Approver  string `json:"approver"`
	Reason    string `json:"reason"`
}

// OrderState exposed via Query and updated in real-time
type OrderState struct {
	OrderID         string    `json:"order_id"`
	Status          string    `json:"status"` // PENDING_APPROVAL, APPROVED, REJECTED, EXPIRED, SHIPPED
	ShippingAddress string    `json:"shipping_address"`
	Approved        bool      `json:"approved"`
	Approver        string    `json:"approver"`
	TrackingNumber  string    `json:"tracking_number"`
	LastUpdated     time.Time `json:"last_updated"`
}

// OrderProcessingWorkflow demonstrates Signals, Queries, Updates, and Timers.
func OrderProcessingWorkflow(ctx workflow.Context, req OrderRequest) (*OrderState, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting OrderProcessingWorkflow", "orderID", req.OrderID)

	// Local mutable state of this workflow instance
	state := &OrderState{
		OrderID:         req.OrderID,
		Status:          "PENDING_APPROVAL",
		ShippingAddress: req.ShippingAddress,
		LastUpdated:     workflow.Now(ctx),
	}

	// 1. REGISTER QUERY HANDLER:
	// Queries allow clients to synchronously inspect workflow state without adding events to history.
	// Query handlers MUST be read-only and deterministic.
	err := workflow.SetQueryHandler(ctx, QueryNameOrderState, func() (*OrderState, error) {
		return state, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to register query handler: %w", err)
	}

	// 2. REGISTER UPDATE HANDLER:
	// Updates allow synchronous mutation of workflow state with immediate return value to the caller.
	// We provide an optional validator to reject invalid inputs BEFORE mutating state.
	err = workflow.SetUpdateHandlerWithOptions(
		ctx,
		UpdateNameAddress,
		func(ctx workflow.Context, newAddress string) (string, error) {
			state.ShippingAddress = newAddress
			state.LastUpdated = workflow.Now(ctx)
			logger.Info("Shipping address updated via Update", "newAddress", newAddress)
			return fmt.Sprintf("Address updated to: %s", newAddress), nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, newAddress string) error {
				if len(newAddress) < 5 {
					return errors.New("address must be at least 5 characters long")
				}
				if state.Status == "SHIPPED" {
					return errors.New("cannot change address after order has already shipped")
				}
				return nil
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register update handler: %w", err)
	}

	// 3. LISTEN FOR ASYNCHRONOUS SIGNALS OR TIMEOUT USING SELECTOR:
	// A Selector allows a workflow to await multiple asynchronous channels (Signals, Timers, Futures) deterministically.
	signalChan := workflow.GetSignalChannel(ctx, SignalNameApproval)
	selector := workflow.NewSelector(ctx)

	var receivedApproval *ApprovalSignal
	selector.AddReceive(signalChan, func(c workflow.ReceiveChannel, more bool) {
		var sig ApprovalSignal
		c.Receive(ctx, &sig)
		receivedApproval = &sig
		logger.Info("Received approval signal", "approved", sig.Approved, "approver", sig.Approver)
	})

	// Add durable timer for deadline
	approvalTimeout := req.ApprovalTimeout
	if approvalTimeout <= 0 {
		approvalTimeout = 24 * time.Hour
	}
	timerFuture := workflow.NewTimer(ctx, approvalTimeout)
	timerExpired := false
	selector.AddFuture(timerFuture, func(f workflow.Future) {
		timerExpired = true
		logger.Warn("Approval window expired without decision")
	})

	// Block until either Signal is received OR Timer fires
	selector.Select(ctx)

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	activityCtx := workflow.WithActivityOptions(ctx, activityOptions)

	// 4. ACT ON RESULT
	if timerExpired {
		state.Status = "EXPIRED"
		state.LastUpdated = workflow.Now(ctx)
		_ = workflow.ExecuteActivity(activityCtx, SendNotificationActivity, NotificationRequest{
			RecipientEmail: req.CustomerEmail,
			Subject:        "Order Cancelled",
			Message:        "Order was automatically cancelled due to approval timeout.",
		}).Get(activityCtx, nil)
		return state, nil
	}

	if !receivedApproval.Approved {
		state.Status = "REJECTED"
		state.Approver = receivedApproval.Approver
		state.LastUpdated = workflow.Now(ctx)
		_ = workflow.ExecuteActivity(activityCtx, SendNotificationActivity, NotificationRequest{
			RecipientEmail: req.CustomerEmail,
			Subject:        "Order Rejected",
			Message:        fmt.Sprintf("Order rejected: %s", receivedApproval.Reason),
		}).Get(activityCtx, nil)
		return state, nil
	}

	// Order Approved!
	state.Status = "APPROVED"
	state.Approved = true
	state.Approver = receivedApproval.Approver
	state.LastUpdated = workflow.Now(ctx)

	// Step 5: Fulfill order
	var fulfillment FulfillmentResult
	err = workflow.ExecuteActivity(activityCtx, FulfillOrderActivity, OrderFulfillmentRequest{
		OrderID:         req.OrderID,
		ShippingAddress: state.ShippingAddress,
	}).Get(activityCtx, &fulfillment)
	if err != nil {
		return nil, err
	}

	state.Status = "SHIPPED"
	state.TrackingNumber = fulfillment.TrackingNumber
	state.LastUpdated = workflow.Now(ctx)

	// Step 6: Notify customer
	_ = workflow.ExecuteActivity(activityCtx, SendNotificationActivity, NotificationRequest{
		RecipientEmail: req.CustomerEmail,
		Subject:        "Order Dispatched",
		Message:        fmt.Sprintf("Your order %s is shipped with tracking: %s", req.OrderID, state.TrackingNumber),
	}).Get(activityCtx, nil)

	logger.Info("OrderProcessingWorkflow completed successfully", "orderID", req.OrderID)
	return state, nil
}
