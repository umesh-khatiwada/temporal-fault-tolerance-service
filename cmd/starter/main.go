package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/umesh-pc/temporal-learning/config"
	basictransfer "github.com/umesh-pc/temporal-learning/internal/01_basic_transfer"
	signalsqueries "github.com/umesh-pc/temporal-learning/internal/02_signals_and_queries"
	sagadistributedtx "github.com/umesh-pc/temporal-learning/internal/03_saga_distributed_tx"
	childworkflows "github.com/umesh-pc/temporal-learning/internal/04_child_workflows_cron"
	"github.com/umesh-pc/temporal-learning/internal/05_versioning"
	"go.temporal.io/sdk/client"
)

const DefaultTaskQueue = "TEMPORAL_LEARNING_TASK_QUEUE"

func printHelp() {
	fmt.Print(`
Temporal Learning Service CLI
-----------------------------
Usage:
  go run ./cmd/starter <command> [arguments]

Commands:
  transfer                       Trigger Module 01 Basic Fund Transfer
  order                          Trigger Module 02 Order Processing Workflow
  signal <workflow-id> <yes|no>  Send Approval Signal to running Order Workflow
  query <workflow-id>            Query state of running Order Workflow
  update <workflow-id> <address> Send Update to mutate address on running Order Workflow
  saga [--fail]                  Trigger Module 03 Trip Booking Saga (pass --fail to simulate rollback)
  batch                          Trigger Module 04 Concurrent Child Workflows
  version                        Trigger Module 05 Versioned Workflow

Environment Variables for Temporal Service / Cloud:
  TEMPORAL_HOST_PORT             Temporal server address (e.g. "my-org.tmprl.cloud:7233" or "localhost:7233")
  TEMPORAL_NAMESPACE             Temporal namespace (e.g. "my-org.default" or "default")
  TEMPORAL_API_KEY               API Key for Temporal Cloud (if using API key auth)
  TEMPORAL_TLS_CERT_PATH         Path to client mTLS certificate (.pem/.crt)
  TEMPORAL_TLS_KEY_PATH          Path to client mTLS private key (.key)
  TEMPORAL_TASK_QUEUE            Task queue name (default: "TEMPORAL_LEARNING_TASK_QUEUE")
`)
}

func main() {
	if len(os.Args) < 2 {
		printHelp()
		return
	}

	cmd := os.Args[1]
	taskQueue := os.Getenv("TEMPORAL_TASK_QUEUE")
	if taskQueue == "" {
		taskQueue = DefaultTaskQueue
	}

	cfg := config.LoadFromEnv()
	c, err := config.NewTemporalClient(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to Temporal Service at %s: %v\nCheck docs/02_connecting_to_cloud.md", cfg.HostPort, err)
	}
	defer c.Close()

	ctx := context.Background()

	switch cmd {
	case "transfer":
		workflowID := fmt.Sprintf("transfer-%d", time.Now().Unix())
		req := basictransfer.TransferRequest{
			FromAccountID: "ACC_ALICE",
			ToAccountID:   "ACC_BOB",
			Amount:        250.75,
			ReferenceID:   fmt.Sprintf("REF-%d", time.Now().Unix()),
		}

		workflowOptions := client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: taskQueue,
		}

		log.Printf("Starting TransferWorkflow (ID: %s)...", workflowID)
		we, err := c.ExecuteWorkflow(ctx, workflowOptions, basictransfer.TransferWorkflow, req)
		if err != nil {
			log.Fatalf("Failed to execute workflow: %v", err)
		}

		var res basictransfer.TransferResult
		err = we.Get(ctx, &res)
		if err != nil {
			log.Fatalf("Workflow finished with error: %v", err)
		}
		log.Printf("Workflow Result: %+v", res)

	case "order":
		workflowID := fmt.Sprintf("order-%d", time.Now().Unix())
		req := signalsqueries.OrderRequest{
			OrderID:         workflowID,
			CustomerEmail:   "customer@example.com",
			ShippingAddress: "123 Market Street, San Francisco, CA",
			Amount:          499.00,
			ApprovalTimeout: 10 * time.Minute,
		}

		workflowOptions := client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: taskQueue,
		}

		log.Printf("Starting OrderProcessingWorkflow (ID: %s)...", workflowID)
		we, err := c.ExecuteWorkflow(ctx, workflowOptions, signalsqueries.OrderProcessingWorkflow, req)
		if err != nil {
			log.Fatalf("Failed to start workflow: %v", err)
		}

		log.Printf("Workflow running! RunID: %s", we.GetRunID())
		log.Printf("To inspect status: go run ./cmd/starter query %s", workflowID)
		log.Printf("To approve:        go run ./cmd/starter signal %s yes", workflowID)
		log.Printf("To reject:         go run ./cmd/starter signal %s no", workflowID)

	case "signal":
		if len(os.Args) < 4 {
			log.Fatalf("Usage: go run ./cmd/starter signal <workflow-id> <yes|no>")
		}
		workflowID := os.Args[2]
		decision := os.Args[3] == "yes"

		sig := signalsqueries.ApprovalSignal{
			Approved: decision,
			Approver: "lead_admin@example.com",
			Reason:   "CLI manual decision",
		}

		err := c.SignalWorkflow(ctx, workflowID, "", signalsqueries.SignalNameApproval, sig)
		if err != nil {
			log.Fatalf("Failed to send signal: %v", err)
		}
		log.Printf("Signal successfully delivered to Workflow ID: %s (Approved: %v)", workflowID, decision)

	case "query":
		if len(os.Args) < 3 {
			log.Fatalf("Usage: go run ./cmd/starter query <workflow-id>")
		}
		workflowID := os.Args[2]

		queryVal, err := c.QueryWorkflow(ctx, workflowID, "", signalsqueries.QueryNameOrderState)
		if err != nil {
			log.Fatalf("Failed to query workflow: %v", err)
		}
		var state signalsqueries.OrderState
		err = queryVal.Get(&state)
		if err != nil {
			log.Fatalf("Failed to parse query result: %v", err)
		}
		log.Printf("Current Workflow State: %+v", state)

	case "update":
		if len(os.Args) < 4 {
			log.Fatalf("Usage: go run ./cmd/starter update <workflow-id> <new-address>")
		}
		workflowID := os.Args[2]
		newAddress := os.Args[3]

		handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
			WorkflowID:   workflowID,
			UpdateName:   signalsqueries.UpdateNameAddress,
			Args:         []interface{}{newAddress},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		})
		if err != nil {
			log.Fatalf("Update failed: %v", err)
		}

		var resp string
		err = handle.Get(ctx, &resp)
		if err != nil {
			log.Fatalf("Update error response: %v", err)
		}
		log.Printf("Update Response: %s", resp)

	case "saga":
		failCar := len(os.Args) > 2 && os.Args[2] == "--fail"
		carCode := "CAR-BMW-X5"
		if failCar {
			carCode = "CAR_UNAVAILABLE" // triggers compensation rollback
		}

		workflowID := fmt.Sprintf("trip-saga-%d", time.Now().Unix())
		req := sagadistributedtx.TripRequest{
			TripID:     workflowID,
			UserID:     "traveler-42",
			FlightCode: "FLIGHT-AA-100",
			HotelCode:  "HOTEL-HYATT-CHICAGO",
			CarCode:    carCode,
			Budget:     2000.0,
		}

		workflowOptions := client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: taskQueue,
		}

		log.Printf("Starting TripBookingSagaWorkflow (ID: %s, simulateFail: %v)...", workflowID, failCar)
		we, err := c.ExecuteWorkflow(ctx, workflowOptions, sagadistributedtx.TripBookingSagaWorkflow, req)
		if err != nil {
			log.Fatalf("Failed to start saga: %v", err)
		}

		var res sagadistributedtx.TripBookingResult
		err = we.Get(ctx, &res)
		if err != nil {
			log.Printf("Workflow returned expected compensation error: %v", err)
			log.Printf("Compensations executed: %+v", res)
		} else {
			log.Printf("Saga succeeded! Result: %+v", res)
		}

	case "batch":
		workflowID := fmt.Sprintf("batch-parent-%d", time.Now().Unix())
		items := []childworkflows.BatchItem{
			{ItemID: "PART-1", Data: "Telemetry data 1"},
			{ItemID: "PART-2", Data: "Telemetry data 2"},
			{ItemID: "PART-3", Data: "Telemetry data 3"},
		}

		workflowOptions := client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: taskQueue,
		}

		log.Printf("Starting BatchParentWorkflow with %d child workflows...", len(items))
		we, err := c.ExecuteWorkflow(ctx, workflowOptions, childworkflows.BatchParentWorkflow, items)
		if err != nil {
			log.Fatalf("Failed to start batch workflow: %v", err)
		}

		var res childworkflows.BatchParentResult
		err = we.Get(ctx, &res)
		if err != nil {
			log.Fatalf("Batch failed: %v", err)
		}
		log.Printf("Batch completed! Total Processed: %d", res.TotalProcessed)
		for _, item := range res.Items {
			log.Printf(" - Item %s: %s", item.ItemID, item.Output)
		}

	case "version":
		workflowID := fmt.Sprintf("versioned-%d", time.Now().Unix())
		workflowOptions := client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: taskQueue,
		}

		log.Printf("Starting EvolvingWorkflow (ID: %s)...", workflowID)
		we, err := c.ExecuteWorkflow(ctx, workflowOptions, versioning.EvolvingWorkflow, "PAYLOAD_DATA")
		if err != nil {
			log.Fatalf("Failed to execute versioned workflow: %v", err)
		}

		var res versioning.VersionedWorkflowResult
		err = we.Get(ctx, &res)
		if err != nil {
			log.Fatalf("Failed: %v", err)
		}
		log.Printf("Versioned Workflow completed: VersionUsed=%d, FraudChecked=%v, Output=%s",
			res.VersionUsed, res.FraudChecked, res.Output)

	default:
		fmt.Printf("Unknown command '%s'\n", cmd)
		printHelp()
	}
}
