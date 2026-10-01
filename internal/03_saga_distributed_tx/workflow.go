package sagadistributedtx

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type TripBookingResult struct {
	TripID            string   `json:"trip_id"`
	FlightBooking     string   `json:"flight_booking,omitempty"`
	HotelBooking      string   `json:"hotel_booking,omitempty"`
	CarBooking        string   `json:"car_booking,omitempty"`
	Status            string   `json:"status"` // SUCCESS, COMPENSATED, FAILED
	CompensationsDone []string `json:"compensations_done,omitempty"`
}

// TripBookingSagaWorkflow coordinates booking a complete itinerary.
// If any step fails, it executes compensating activities in reverse order (LIFO).
func TripBookingSagaWorkflow(ctx workflow.Context, req TripRequest) (*TripBookingResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting TripBookingSagaWorkflow", "tripID", req.TripID)

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    1 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumAttempts:    2,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	result := &TripBookingResult{
		TripID:            req.TripID,
		Status:            "PENDING",
		CompensationsDone: make([]string, 0),
	}

	// Compensation stack: list of rollback functions to invoke if failure occurs
	var compensations []func(ctx workflow.Context) error

	// Helper to execute compensations in reverse order
	compensate := func(originalErr error) (*TripBookingResult, error) {
		logger.Warn("Workflow failure detected. Executing Saga compensations...", "error", originalErr)
		result.Status = "COMPENSATED"

		// CRITICAL TEMPORAL PATTERN:
		// When compensating due to a failure or workflow cancellation, the main `ctx` might be
		// cancelled or expired. We MUST create a DisconnectedContext so compensation activities
		// have a fresh timeout and can execute without getting blocked by parent context cancellation!
		disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
		compCtx := workflow.WithActivityOptions(disconnectedCtx, activityOptions)

		// Execute compensations in LIFO order (reverse of execution)
		for i := len(compensations) - 1; i >= 0; i-- {
			compErr := compensations[i](compCtx)
			if compErr != nil {
				logger.Error("Compensation activity failed", "index", i, "error", compErr)
				// In production, you would alert ops or escalate to human review
			}
		}

		return result, fmt.Errorf("trip booking failed and was compensated: %w", originalErr)
	}

	// STEP 1: Book Flight
	var flightConf string
	err := workflow.ExecuteActivity(ctx, BookFlightActivity, req.TripID, req.FlightCode).Get(ctx, &flightConf)
	if err != nil {
		return compensate(err)
	}
	result.FlightBooking = flightConf
	// Register compensation for flight
	compensations = append(compensations, func(cCtx workflow.Context) error {
		result.CompensationsDone = append(result.CompensationsDone, "CancelFlight:"+flightConf)
		return workflow.ExecuteActivity(cCtx, CancelFlightActivity, req.TripID, flightConf).Get(cCtx, nil)
	})

	// STEP 2: Reserve Hotel
	var hotelConf string
	err = workflow.ExecuteActivity(ctx, ReserveHotelActivity, req.TripID, req.HotelCode).Get(ctx, &hotelConf)
	if err != nil {
		return compensate(err)
	}
	result.HotelBooking = hotelConf
	// Register compensation for hotel
	compensations = append(compensations, func(cCtx workflow.Context) error {
		result.CompensationsDone = append(result.CompensationsDone, "CancelHotel:"+hotelConf)
		return workflow.ExecuteActivity(cCtx, CancelHotelActivity, req.TripID, hotelConf).Get(cCtx, nil)
	})

	// STEP 3: Rent Car (e.g. this might fail if inventory is out)
	var carConf string
	err = workflow.ExecuteActivity(ctx, RentCarActivity, req.TripID, req.CarCode).Get(ctx, &carConf)
	if err != nil {
		// Triggers rollback of Hotel and Flight!
		return compensate(err)
	}
	result.CarBooking = carConf
	// Register compensation for car
	compensations = append(compensations, func(cCtx workflow.Context) error {
		result.CompensationsDone = append(result.CompensationsDone, "CancelCar:"+carConf)
		return workflow.ExecuteActivity(cCtx, CancelCarActivity, req.TripID, carConf).Get(cCtx, nil)
	})

	result.Status = "SUCCESS"
	logger.Info("TripBookingSagaWorkflow completed successfully", "tripID", req.TripID)
	return result, nil
}
