package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/umesh-pc/temporal-learning/config"
	basictransfer "github.com/umesh-pc/temporal-learning/internal/01_basic_transfer"
	signalsqueries "github.com/umesh-pc/temporal-learning/internal/02_signals_and_queries"
	sagadistributedtx "github.com/umesh-pc/temporal-learning/internal/03_saga_distributed_tx"
	childworkflows "github.com/umesh-pc/temporal-learning/internal/04_child_workflows_cron"
	"github.com/umesh-pc/temporal-learning/internal/05_versioning"
	"go.temporal.io/sdk/worker"
)

const DefaultTaskQueue = "TEMPORAL_LEARNING_TASK_QUEUE"

func main() {
	log.Println("=== Starting Temporal Learning Worker ===")

	// 1. Load connection config from environment variables (Cloud / Remote Service)
	cfg := config.LoadFromEnv()
	log.Printf("Connecting to Temporal Service at: %s (Namespace: %s)", cfg.HostPort, cfg.Namespace)

	// 2. Initialize Temporal Client
	c, err := config.NewTemporalClient(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize Temporal client: %v\nCheck docs/02_connecting_to_cloud.md for environment variable instructions.", err)
	}
	defer c.Close()

	taskQueue := os.Getenv("TEMPORAL_TASK_QUEUE")
	if taskQueue == "" {
		taskQueue = DefaultTaskQueue
	}

	// 3. Create a Worker listening on the Task Queue
	w := worker.New(c, taskQueue, worker.Options{})

	// 4. Register Module 01: Basic Transfer
	w.RegisterWorkflow(basictransfer.TransferWorkflow)
	w.RegisterActivity(basictransfer.WithdrawActivity)
	w.RegisterActivity(basictransfer.DepositActivity)

	// 5. Register Module 02: Signals & Queries & Updates
	w.RegisterWorkflow(signalsqueries.OrderProcessingWorkflow)
	w.RegisterActivity(signalsqueries.SendNotificationActivity)
	w.RegisterActivity(signalsqueries.FulfillOrderActivity)

	// 6. Register Module 03: Saga Distributed Transactions
	w.RegisterWorkflow(sagadistributedtx.TripBookingSagaWorkflow)
	w.RegisterActivity(sagadistributedtx.BookFlightActivity)
	w.RegisterActivity(sagadistributedtx.CancelFlightActivity)
	w.RegisterActivity(sagadistributedtx.ReserveHotelActivity)
	w.RegisterActivity(sagadistributedtx.CancelHotelActivity)
	w.RegisterActivity(sagadistributedtx.RentCarActivity)
	w.RegisterActivity(sagadistributedtx.CancelCarActivity)

	// 7. Register Module 04: Child Workflows & Concurrency
	w.RegisterWorkflow(childworkflows.BatchParentWorkflow)
	w.RegisterWorkflow(childworkflows.ItemProcessorChildWorkflow)
	w.RegisterWorkflow(childworkflows.RecurringMonitorWorkflow)
	w.RegisterActivity(childworkflows.ProcessSingleItemActivity)

	// 8. Register Module 05: Versioning
	w.RegisterWorkflow(versioning.EvolvingWorkflow)
	w.RegisterActivity(versioning.LegacyActivity)
	w.RegisterActivity(versioning.ModernFraudCheckActivity)

	// 9. Start listening to the Task Queue
	log.Printf("Worker successfully registered all workflows & activities on TaskQueue: '%s'", taskQueue)
	log.Println("Worker is polling for tasks from Temporal Service... Press Ctrl+C to stop.")

	err = w.Start()
	if err != nil {
		log.Fatalf("Unable to start worker: %v", err)
	}

	// Wait for interrupt signal to gracefully shut down
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("\nShutdown signal received. Stopping worker gracefully...")
	w.Stop()
	log.Println("Worker stopped cleanly.")
}
