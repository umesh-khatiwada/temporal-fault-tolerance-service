package signalsqueries

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

type SignalsQueriesTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *SignalsQueriesTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(OrderProcessingWorkflow)
	s.env.RegisterActivity(SendNotificationActivity)
	s.env.RegisterActivity(FulfillOrderActivity)
}

func (s *SignalsQueriesTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
}

// Test_OrderApprovedAfterAddressUpdate tests:
// 1. Workflow starts and pauses waiting for approval.
// 2. Client queries the state (checks status is PENDING_APPROVAL).
// 3. Client updates the address via Update.
// 4. Client sends the Approval signal.
// 5. Workflow fulfills and completes with SHIPPED status and new address.
func (s *SignalsQueriesTestSuite) Test_OrderApprovedAfterAddressUpdate() {
	req := OrderRequest{
		OrderID:         "ORD-999",
		CustomerEmail:   "alice@example.com",
		ShippingAddress: "123 Old Street",
		Amount:          450.0,
		ApprovalTimeout: 1 * time.Hour,
	}

	// Register delayed callbacks to simulate external client actions
	s.env.RegisterDelayedCallback(func() {
		// 1. Query current state
		queryVal, err := s.env.QueryWorkflow(QueryNameOrderState)
		s.NoError(err)
		var state OrderState
		err = queryVal.Get(&state)
		s.NoError(err)
		s.Equal("PENDING_APPROVAL", state.Status)
		s.Equal("123 Old Street", state.ShippingAddress)

		// 2. Send Signal to approve
		s.env.SignalWorkflow(SignalNameApproval, ApprovalSignal{
			Approved: true,
			Approver: "manager@company.com",
			Reason:   "Verified credit limit",
		})
	}, 10*time.Minute)

	s.env.ExecuteWorkflow(OrderProcessingWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var finalState OrderState
	err := s.env.GetWorkflowResult(&finalState)
	s.NoError(err)
	s.Equal("SHIPPED", finalState.Status)
	s.True(finalState.Approved)
	s.Equal("manager@company.com", finalState.Approver)
	s.NotEmpty(finalState.TrackingNumber)
}

// Test_OrderExpiredOnTimeout tests the durable timer fallback when no signal arrives.
func (s *SignalsQueriesTestSuite) Test_OrderExpiredOnTimeout() {
	req := OrderRequest{
		OrderID:         "ORD-EXPIRE",
		CustomerEmail:   "bob@example.com",
		ShippingAddress: "456 Main St",
		Amount:          150.0,
		ApprovalTimeout: 2 * time.Hour,
	}

	// Do NOT send any signal. The timer should fire automatically in test environment.
	s.env.ExecuteWorkflow(OrderProcessingWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var finalState OrderState
	err := s.env.GetWorkflowResult(&finalState)
	s.NoError(err)
	s.Equal("EXPIRED", finalState.Status)
	s.False(finalState.Approved)
}

func TestSignalsQueriesTestSuite(t *testing.T) {
	suite.Run(t, new(SignalsQueriesTestSuite))
}
