package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/praetordev/crypto"
	"github.com/praetordev/db"
	"github.com/praetordev/env"
	"github.com/praetordev/eventbus"
	"github.com/praetordev/metrics"
	"github.com/praetordev/objectstore"
	"github.com/praetordev/plog"
	core "github.com/praetordev/scheduler/core"
)

func main() {
	plog.Configure("scheduler")
	log.Println("Starting Scheduler Service...")

	// Fail fast on a missing/invalid encryption key (used to decrypt Galaxy
	// credential tokens when building manifests).
	if err := crypto.ValidateSecrets(false); err != nil {
		log.Fatalf("secrets misconfigured: %v", err)
	}

	// 1. Connect to DB
	database, err := db.Connect(env.String("DATABASE_URL", db.DefaultDSN))
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}

	// 2. Init NATS
	bus, err := eventbus.NewBus(env.String("NATS_URL", eventbus.DefaultURL))
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer bus.Close()

	// Expose Prometheus /metrics.
	metrics.Serve("")

	// 3. Init Scheduler
	// Poll every 5 seconds
	sched := core.NewScheduler(database, 5*time.Second, bus)
	// Base URL embedded in the manifest for the pushed host-runner to report back.
	sched.APIURL = env.String("API_URL", "")

	// Retention pruning (opt-OUT). JOB_RETENTION_DAYS defaults to 90: terminal jobs
	// finished longer ago than that are deleted, along with their events and log
	// blobs, so state doesn't grow without bound by default (issue #17). Set
	// JOB_RETENTION_DAYS=0 to disable pruning and keep everything.
	if days := env.Int("JOB_RETENTION_DAYS", 90); days > 0 {
		sched.RetentionDays = days
		if ls, err := objectstore.NewJetStreamLogStore(bus.JS, ""); err == nil {
			sched.Logs = ls
		} else {
			log.Printf("retention: object store unavailable, blobs won't be pruned: %v", err)
		}
		log.Printf("retention: pruning terminal jobs finished > %d day(s) ago", days)
	} else {
		log.Printf("retention: pruning disabled (JOB_RETENTION_DAYS=0); job history and log blobs are kept indefinitely")
	}

	// 3. Start loop in background; ctx cancellation is the stop signal.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sched.Start(ctx)

	// 4. Wait for SIGINT/SIGTERM
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	// 5. Graceful shutdown
	log.Println("Shutting down...")
	cancel()
}
