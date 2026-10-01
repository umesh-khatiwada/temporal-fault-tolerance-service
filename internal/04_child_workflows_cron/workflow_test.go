package childworkflows

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type ChildWorkflowsTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *ChildWorkflowsTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(BatchParentWorkflow)
	s.env.RegisterWorkflow(ItemProcessorChildWorkflow)
	s.env.RegisterWorkflow(RecurringMonitorWorkflow)
	s.env.RegisterActivity(ProcessSingleItemActivity)
}

func (s *ChildWorkflowsTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
}

// Test_ConcurrentChildWorkflows verifies that parent workflow executes child workflows concurrently.
func (s *ChildWorkflowsTestSuite) Test_ConcurrentChildWorkflows() {
	items := []BatchItem{
		{ItemID: "ITEM-A", Data: "payload-a"},
		{ItemID: "ITEM-B", Data: "payload-b"},
		{ItemID: "ITEM-C", Data: "payload-c"},
	}

	s.env.ExecuteWorkflow(BatchParentWorkflow, items)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result BatchParentResult
	err := s.env.GetWorkflowResult(&result)
	s.NoError(err)
	s.Equal(3, result.TotalProcessed)
	s.Equal("PROCESSED-ITEM-A", result.Items[0].Output)
	s.Equal("PROCESSED-ITEM-B", result.Items[1].Output)
	s.Equal("PROCESSED-ITEM-C", result.Items[2].Output)
}

// Test_ContinueAsNewExecution verifies that workflow iterates and triggers continue-as-new.
func (s *ChildWorkflowsTestSuite) Test_ContinueAsNewExecution() {
	initialState := RecurringState{
		CurrentIteration: 0,
		MaxIterations:    3,
	}

	s.env.ExecuteWorkflow(RecurringMonitorWorkflow, initialState)

	s.True(s.env.IsWorkflowCompleted())
	// In the test environment, the first iteration returns ContinueAsNew error which the test engine records
	err := s.env.GetWorkflowError()
	s.Error(err)
	s.True(workflow.IsContinueAsNewError(err) || s.Contains(err.Error(), "continue as new"))
}

func TestChildWorkflowsTestSuite(t *testing.T) {
	suite.Run(t, new(ChildWorkflowsTestSuite))
}
