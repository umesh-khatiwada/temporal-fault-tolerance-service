package childworkflows

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"
)

type BatchItem struct {
	ItemID string `json:"item_id"`
	Data   string `json:"data"`
}

type ProcessItemResult struct {
	ItemID  string `json:"item_id"`
	Success bool   `json:"success"`
	Output  string `json:"output"`
}

// ProcessSingleItemActivity performs work on an individual item.
func ProcessSingleItemActivity(ctx context.Context, item BatchItem) (*ProcessItemResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Processing batch item in activity", "itemID", item.ItemID)

	return &ProcessItemResult{
		ItemID:  item.ItemID,
		Success: true,
		Output:  fmt.Sprintf("PROCESSED-%s", item.ItemID),
	}, nil
}
