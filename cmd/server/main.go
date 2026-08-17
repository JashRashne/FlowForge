package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flowforge/flowforge/internal/api"
	"github.com/flowforge/flowforge/internal/outbox"
	"github.com/flowforge/flowforge/internal/repository"
	"github.com/flowforge/flowforge/internal/scheduler"
	"github.com/flowforge/flowforge/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func main() {
	// Configure structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	slog.Info("Starting FlowForge Distributed Workflow Engine...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configuration
	dbConnStr := getEnv("DATABASE_URL", "postgres://flowforge:flowforge_password@localhost:5433/flowforge?sslmode=disable")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	httpPort := getEnv("PORT", "8080")

	// 1. Connect to PostgreSQL
	pool, err := pgxpool.New(ctx, dbConnStr)
	if err != nil {
		slog.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("PostgreSQL ping failed", "error", err)
		os.Exit(1)
	}
	slog.Info("Connected to PostgreSQL", "url", dbConnStr)

	// 2. Connect to Redis / Valkey
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Error("Redis ping failed", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()
	slog.Info("Connected to Redis", "addr", redisAddr)

	// 3. Initialize Repositories and Components
	repo := repository.NewPostgresRepository(pool)
	wsHub := api.NewWebSocketHub()
	relay := outbox.NewRelay(pool, rdb, 20)
	sched := scheduler.NewScheduler(pool, repo)

	// 4. Start Background Services:
	// a) WebSocket Redis Stream Listener
	go wsHub.ListenRedisEvents(ctx, rdb)

	// b) Transactional Outbox Event Relay (polls every 100ms)
	go func() {
		slog.Info("Event Relay started")
		if err := relay.Start(ctx, 100*time.Millisecond); err != nil && ctx.Err() == nil {
			slog.Error("Event Relay error", "error", err)
		}
	}()

	// c) Scheduler Loop (ticks every 200ms)
	go func() {
		slog.Info("Scheduler loop started")
		if err := sched.Start(ctx, 200*time.Millisecond); err != nil && ctx.Err() == nil {
			slog.Error("Scheduler loop error", "error", err)
		}
	}()

	// d) Start 3 Concurrent Worker Processes
	for i := 1; i <= 3; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		w := worker.NewWorker(workerID, "flowforge-workers", repo, rdb, nil, 30*time.Second)
		go func(w *worker.Worker, id string) {
			slog.Info("Worker process started", "worker_id", id)
			if err := w.Start(ctx); err != nil && ctx.Err() == nil {
				slog.Error("Worker error", "worker_id", id, "error", err)
			}
		}(w, workerID)
	}

	// 5. Start HTTP & WebSocket Server
	server := api.NewServer(pool, repo, rdb, wsHub)
	httpServer := &http.Server{
		Addr:         ":" + httpPort,
		Handler:      server.Router(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		slog.Info("FlowForge API Server listening", "port", httpPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	// 6. Graceful Shutdown on OS Signals (SIGINT, SIGTERM)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	slog.Info("Shutdown signal received, gracefully draining services...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}

	slog.Info("FlowForge shutdown complete.")
}
