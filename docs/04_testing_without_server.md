# Module 04: Zero-Server Testing with `testsuite`

> **Official Documentation Reference:** [https://docs.temporal.io/dev-guide/go-sdk/testing](https://docs.temporal.io/dev-guide/go-sdk/testing)

---

## 1. Why `testsuite` is a Game Changer

In many distributed systems, testing requires running heavyweight Docker containers (LocalStack, Kafka, MySQL, Redis) that are slow, brittle, and resource-heavy.

The Temporal Go SDK includes a first-class, **in-process workflow engine mock**:
`go.temporal.io/sdk/testsuite`

### What it provides:
- **Zero Server Setup**: Runs completely in-memory inside the standard `go test` binary.
- **Time Skipping**: A workflow that sleeps for 30 days (`workflow.Sleep(ctx, 30*24*time.Hour)`) completes in **0.002 seconds** in testsuite because virtual time is skipped automatically!
- **Determinism Verification**: Detects determinism violations immediately during unit testing.
- **Activity Mocking**: Easily mock downstream microservices to simulate transient network timeouts, 500 errors, or crashes.

---

## 2. Test Architecture

```go
type UnitTestSuite struct {
    suite.Suite
    testsuite.WorkflowTestSuite
    env *testsuite.TestWorkflowEnvironment
}

func (s *UnitTestSuite) SetupTest() {
    // Spawns an in-memory execution environment
    s.env = s.NewTestWorkflowEnvironment()
}

func (s *UnitTestSuite) TearDownTest() {
    // Asserts all mock expectations were satisfied
    s.env.AssertExpectations(s.T())
}
```

---

## 3. Common Testing Patterns

### 1. Mocking Downstream Activities to Test Retries:
```go
// Simulate a network failure on attempt 1, followed by success on attempt 2:
s.env.OnActivity(DepositActivity, mock.Anything, mock.Anything).
    Return("", errors.New("simulated network connection timeout")).
    Once()

s.env.OnActivity(DepositActivity, mock.Anything, mock.Anything).
    Return("TX-SUCCESS-123", nil).
    Once()

s.env.ExecuteWorkflow(TransferWorkflow, req)
s.True(s.env.IsWorkflowCompleted())
s.NoError(s.env.GetWorkflowError())
```

### 2. Testing Signals and Delayed Callbacks:
```go
// Schedule a Signal to be sent 10 minutes into workflow virtual time:
s.env.RegisterDelayedCallback(func() {
    s.env.SignalWorkflow("signal_order_approval", ApprovalSignal{
        Approved: true,
        Approver: "supervisor@company.com",
    })
}, 10*time.Minute)

s.env.ExecuteWorkflow(OrderProcessingWorkflow, req)
```

### 3. Testing Queries on Running Workflows:
```go
s.env.RegisterDelayedCallback(func() {
    queryVal, err := s.env.QueryWorkflow("query_order_state")
    s.NoError(err)

    var state OrderState
    _ = queryVal.Get(&state)
    s.Equal("PENDING_APPROVAL", state.Status)
}, 5*time.Minute)
```

---

## 4. Running the Tests

To run all tests across all modules in this repository:

```bash
go test -v ./...
```

Notice the execution time: all five modules execute in **under 0.25 seconds** combined!
