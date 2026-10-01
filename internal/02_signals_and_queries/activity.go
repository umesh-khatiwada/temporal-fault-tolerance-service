package signalsqueries

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

type NotificationRequest struct {
	RecipientEmail string `json:"recipient_email"`
	Subject        string `json:"subject"`
	Message        string `json:"message"`
}

// SendNotificationActivity sends an email/webhook notification.
func SendNotificationActivity(ctx context.Context, req NotificationRequest) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Sending notification", "email", req.RecipientEmail, "subject", req.Subject)
	// Simulate outbound HTTP or SMTP call
	return nil
}

type OrderFulfillmentRequest struct {
	OrderID         string `json:"order_id"`
	ShippingAddress string `json:"shipping_address"`
}

type FulfillmentResult struct {
	TrackingNumber string    `json:"tracking_number"`
	ShippedAt      time.Time `json:"shipped_at"`
}

// FulfillOrderActivity executes the physical dispatch of the order.
func FulfillOrderActivity(ctx context.Context, req OrderFulfillmentRequest) (*FulfillmentResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Fulfilling order", "orderID", req.OrderID, "address", req.ShippingAddress)

	return &FulfillmentResult{
		TrackingNumber: fmt.Sprintf("TRACK-%s-%d", req.OrderID, time.Now().UnixNano()%10000),
		ShippedAt:      time.Now().UTC(),
	}, nil
}
