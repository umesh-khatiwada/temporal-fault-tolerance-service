package sagadistributedtx

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

type SagaTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *SagaTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(TripBookingSagaWorkflow)
	s.env.RegisterActivity(BookFlightActivity)
	s.env.RegisterActivity(CancelFlightActivity)
	s.env.RegisterActivity(ReserveHotelActivity)
	s.env.RegisterActivity(CancelHotelActivity)
	s.env.RegisterActivity(RentCarActivity)
	s.env.RegisterActivity(CancelCarActivity)
}

func (s *SagaTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
}

// Test_HappyPath_AllServicesBooked verifies successful booking of all 3 resources.
func (s *SagaTestSuite) Test_HappyPath_AllServicesBooked() {
	req := TripRequest{
		TripID:     "TRIP-101",
		UserID:     "user-john",
		FlightCode: "FL-UA-942",
		HotelCode:  "HT-HILTON-NYC",
		CarCode    : "CAR-TESLA-3",
		Budget:     1500.0,
	}

	s.env.ExecuteWorkflow(TripBookingSagaWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var res TripBookingResult
	err := s.env.GetWorkflowResult(&res)
	s.NoError(err)
	s.Equal("SUCCESS", res.Status)
	s.Equal("CONF-FLIGHT-FL-UA-942", res.FlightBooking)
	s.Equal("CONF-HOTEL-HT-HILTON-NYC", res.HotelBooking)
	s.Equal("CONF-CAR-CAR-TESLA-3", res.CarBooking)
	s.Empty(res.CompensationsDone)
}

// Test_CarFails_CompensationsTriggered verifies that when RentCar fails, Hotel and Flight are rolled back.
func (s *SagaTestSuite) Test_CarFails_CompensationsTriggered() {
	req := TripRequest{
		TripID:     "TRIP-FAILED-CAR",
		UserID:     "user-jane",
		FlightCode: "FL-DL-100",
		HotelCode:  "HT-MARRIOTT-LA",
		CarCode:    "CAR_UNAVAILABLE", // Triggers failure in RentCarActivity
		Budget:     1200.0,
	}

	s.env.ExecuteWorkflow(TripBookingSagaWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	// The workflow should return an error indicating compensation occurred
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "trip booking failed and was compensated")
}

func TestSagaTestSuite(t *testing.T) {
	suite.Run(t, new(SagaTestSuite))
}
