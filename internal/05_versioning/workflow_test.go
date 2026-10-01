package versioning

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

type VersioningTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *VersioningTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(EvolvingWorkflow)
	s.env.RegisterActivity(LegacyActivity)
	s.env.RegisterActivity(ModernFraudCheckActivity)
}

func (s *VersioningTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
}

// Test_NewExecutionUsesVersion1 verifies that fresh executions take the latest version branch (v1).
func (s *VersioningTestSuite) Test_NewExecutionUsesVersion1() {
	s.env.ExecuteWorkflow(EvolvingWorkflow, "ORDER_PAYLOAD_909")

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var res VersionedWorkflowResult
	err := s.env.GetWorkflowResult(&res)
	s.NoError(err)

	s.Equal(1, res.VersionUsed)
	s.True(res.FraudChecked)
	s.Equal("LEGACY:ORDER_PAYLOAD_909", res.Output)
}

func TestVersioningTestSuite(t *testing.T) {
	suite.Run(t, new(VersioningTestSuite))
}
