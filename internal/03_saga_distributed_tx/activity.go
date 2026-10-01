package sagadistributedtx

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"
)

type TripRequest struct {
	TripID     string  `json:"trip_id"`
	UserID     string  `json:"user_id"`
	FlightCode string  `json:"flight_code"`
	HotelCode  string  `json:"hotel_code"`
	CarCode    string  `json:"car_code"`
	Budget     float64 `json:"budget"`
}

// 1. Forward Activity: Book Flight
func BookFlightActivity(ctx context.Context, tripID string, flightCode string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Booking flight", "tripID", tripID, "flightCode", flightCode)
	return fmt.Sprintf("CONF-FLIGHT-%s", flightCode), nil
}

// 1b. Compensating Activity: Cancel Flight
func CancelFlightActivity(ctx context.Context, tripID string, confirmationID string) error {
	logger := activity.GetLogger(ctx)
	logger.Warn("COMPENSATION: Cancelling flight", "tripID", tripID, "confID", confirmationID)
	return nil
}

// 2. Forward Activity: Reserve Hotel
func ReserveHotelActivity(ctx context.Context, tripID string, hotelCode string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Reserving hotel", "tripID", tripID, "hotelCode", hotelCode)
	return fmt.Sprintf("CONF-HOTEL-%s", hotelCode), nil
}

// 2b. Compensating Activity: Cancel Hotel
func CancelHotelActivity(ctx context.Context, tripID string, confirmationID string) error {
	logger := activity.GetLogger(ctx)
	logger.Warn("COMPENSATION: Cancelling hotel", "tripID", tripID, "confID", confirmationID)
	return nil
}

// 3. Forward Activity: Rent Car
func RentCarActivity(ctx context.Context, tripID string, carCode string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Renting car", "tripID", tripID, "carCode", carCode)

	// Simulate failure condition when car is unavailable
	if carCode == "CAR_UNAVAILABLE" {
		return "", fmt.Errorf("no cars available for code %s (inventory exhausted)", carCode)
	}

	return fmt.Sprintf("CONF-CAR-%s", carCode), nil
}

// 3b. Compensating Activity: Cancel Car
func CancelCarActivity(ctx context.Context, tripID string, confirmationID string) error {
	logger := activity.GetLogger(ctx)
	logger.Warn("COMPENSATION: Cancelling car rental", "tripID", tripID, "confID", confirmationID)
	return nil
}
