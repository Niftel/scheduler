package main

import (
	"context"
	"errors"
	"log"
	"net/http"
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
	sched.SecretsIntegration = env.String("PRAETOR_SECRETS_URL", "") != ""
	claimServer, secretsClient, err := newClaimServer(database, claimServerConfig{
		SecretsURL:             env.String("PRAETOR_SECRETS_URL", ""),
		SecretsCAFile:          env.String("PRAETOR_SECRETS_CA_FILE", ""),
		SecretsCertificateFile: env.String("PRAETOR_SECRETS_CERT_FILE", ""),
		SecretsPrivateKeyFile:  env.String("PRAETOR_SECRETS_KEY_FILE", ""),
		TrustDomain:            env.String("PRAETOR_SECRETS_TRUST_DOMAIN", ""),
		Address:                env.String("PRAETOR_CLAIM_ADDR", ":8443"),
		ServerCertificateFile:  env.String("PRAETOR_CLAIM_CERT_FILE", ""),
		ServerPrivateKeyFile:   env.String("PRAETOR_CLAIM_KEY_FILE", ""),
		ExecutorCAFile:         env.String("PRAETOR_CLAIM_CLIENT_CA_FILE", ""),
	})
	if err != nil {
		log.Fatalf("claim listener misconfigured: %v", err)
	}

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
	if secretsClient != nil {
		go core.NewBindingCanceller(database, secretsClient).Start(ctx, 5*time.Second)
	}
	claimErrors := make(chan error, 1)
	if claimServer != nil {
		go func() {
			log.Printf("secure executor claim listener started on %s", claimServer.Addr)
			if err := claimServer.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
				claimErrors <- err
			}
		}()
	}

	// 4. Wait for SIGINT/SIGTERM
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	var claimFailure error
	select {
	case <-sigs:
	case err := <-claimErrors:
		claimFailure = err
	}

	// 5. Graceful shutdown
	log.Println("Shutting down...")
	cancel()
	if claimServer != nil {
		shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := claimServer.Shutdown(shutdownContext); err != nil {
			log.Printf("secure executor claim listener shutdown failed: %v", err)
		}
	}
	if claimFailure != nil {
		log.Fatalf("secure executor claim listener failed: %v", claimFailure)
	}
}
