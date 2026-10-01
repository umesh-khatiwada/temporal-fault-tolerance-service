package basictransfer

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

type UnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *UnitTestSuite) SetupTest() {
	// Initialize an in-memory Temporal workflow execution environment.
	// NO local server, NO docker, NO background process needed!
	s.env = s.NewTestWorkflowEnvironment()
}

func (s *UnitTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
}

// Test_SuccessfulTransfer tests the happy path with real activity execution.
func (s *UnitTestSuite) Test_SuccessfulTransfer() {
	s.env.RegisterWorkflow(TransferWorkflow)
	s.env.RegisterActivity(WithdrawActivity)
	s.env.RegisterActivity(DepositActivity)

	req := TransferRequest{
		FromAccountID: "ACC_ALICE",
		ToAccountID:   "ACC_BOB",
		Amount:        250.0,
		ReferenceID:   "REF_12345",
	}

	s.env.ExecuteWorkflow(TransferWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result TransferResult
	err := s.env.GetWorkflowResult(&result)
	s.NoError(err)
	s.Equal("COMPLETED", result.Status)
	s.Contains(result.TransactionID, "TX-WTH-ACC_ALICE")
	s.Contains(result.TransactionID, "TX-DEP-ACC_BOB")
}

// Test_InsufficientFunds_NonRetryableFailure tests that business logic failures abort immediately without retrying.
func (s *UnitTestSuite) Test_InsufficientFunds_NonRetryableFailure() {
	s.env.RegisterWorkflow(TransferWorkflow)
	s.env.RegisterActivity(WithdrawActivity)
	s.env.RegisterActivity(DepositActivity)

	req := TransferRequest{
		FromAccountID: "ACC_EMPTY", // Triggers NonRetryableApplicationError
		ToAccountID:   "ACC_BOB",
		Amount:        100.0,
		ReferenceID:   "REF_FAIL",
	}

	s.env.ExecuteWorkflow(TransferWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "account has insufficient funds")
}

// Test_ActivityRetryOnTransientError tests that Temporal automatically retries on transient errors.
func (s *UnitTestSuite) Test_ActivityRetryOnTransientError() {
	s.env.RegisterWorkflow(TransferWorkflow)
	s.env.RegisterActivity(WithdrawActivity)

	// Mock DepositActivity to fail once with a transient error, then succeed on retry!
	s.env.OnActivity(DepositActivity, mock.Anything, mock.Anything).
		Return("", errors.New("simulated network glitch")).
		Once()

	s.env.OnActivity(DepositActivity, mock.Anything, mock.Anything).
		Return("TX-DEP-MOCK-OK", nil).
		Once()

	req := TransferRequest{
		FromAccountID: "ACC_ALICE",
		ToAccountID:   "ACC_BOB",
		Amount:        75.0,
		ReferenceID:   "REF_RETRY",
	}

	s.env.ExecuteWorkflow(TransferWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result TransferResult
	err := s.env.GetWorkflowResult(&result)
	s.NoError(err)
	s.Equal("COMPLETED", result.Status)
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}
