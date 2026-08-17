package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/flowforge/flowforge/internal/dag"
	"github.com/flowforge/flowforge/internal/repository"
	"github.com/flowforge/flowforge/pkg/domain"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Server coordinates HTTP and WebSocket routes for FlowForge.
type Server struct {
	router      *chi.Mux
	pool        *pgxpool.Pool
	repo        *repository.PostgresRepository
	redisClient *redis.Client
	wsHub       *WebSocketHub
}

// NewServer initializes the HTTP router, middleware, and route handlers.
func NewServer(
	pool *pgxpool.Pool,
	repo *repository.PostgresRepository,
	redisClient *redis.Client,
	wsHub *WebSocketHub,
) *Server {
	s := &Server{
		router:      chi.NewRouter(),
		pool:        pool,
		repo:        repo,
		redisClient: redisClient,
		wsHub:       wsHub,
	}
	s.setupRoutes()
	return s
}

// Router returns the configured chi router for testing or HTTP serving.
func (s *Server) Router() http.Handler {
	return s.router
}

func (s *Server) setupRoutes() {
	r := s.router

	// Middleware
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	// CORS for frontend connection
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-Trace-ID"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	// Health check
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	// WebSockets
	r.Get("/ws/events", s.wsHub.HandleWebSocket)

	// Workflows API
	r.Route("/workflows", func(r chi.Router) {
		r.Post("/", s.handleCreateWorkflow)
		r.Get("/{id}", s.handleGetWorkflow)
		r.Post("/{id}/runs", s.handleStartWorkflowRun)
	})

	// Runs API
	r.Route("/runs", func(r chi.Router) {
		r.Get("/{id}", s.handleGetRun)
		r.Get("/{id}/tasks", s.handleGetRunTasks)
	})

	// Workers & Chaos API
	r.Get("/workers", s.handleListWorkers)
	r.Post("/chaos/kill-worker", s.handleKillWorker)
}

func (s *Server) handleCreateWorkflow(w http.ResponseWriter, r *http.Request) {
	var def domain.WorkflowDefinition
	if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	if _, err := uuid.Parse(def.ID); err != nil {
		def.ID = uuid.NewString()
	}
	if def.CreatedAt.IsZero() {
		def.CreatedAt = time.Now().UTC()
	}

	// 1. Validate DAG acyclicity
	graph, err := dag.BuildGraph(def)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadRequest)
		return
	}
	if _, err := graph.TopologicalSort(); err != nil {
		http.Error(w, `{"error":"cycle detected in workflow DAG"}`, http.StatusBadRequest)
		return
	}

	// 2. Persist to Postgres
	if err := s.repo.CreateWorkflow(r.Context(), def); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"id": def.ID, "status": "created"})
}

func (s *Server) handleGetWorkflow(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	wf, err := s.repo.GetWorkflow(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNodeNotFound) {
			http.Error(w, `{"error":"workflow not found"}`, http.StatusNotFound)
			return
		}
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(wf)
}

func (s *Server) handleStartWorkflowRun(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	traceID := r.Header.Get("X-Trace-ID")
	if traceID == "" {
		traceID = uuid.NewString()
	}

	runID, err := s.repo.StartWorkflowRun(r.Context(), id, traceID)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"run_id":   runID,
		"status":   "RUNNING",
		"trace_id": traceID,
	})
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")

	var state string
	var startedAt time.Time
	var finishedAt *time.Time

	err := s.pool.QueryRow(r.Context(), `
		SELECT state, started_at, finished_at
		FROM workflow_runs
		WHERE id = $1::uuid;
	`, runID).Scan(&state, &startedAt, &finishedAt)

	if err != nil {
		http.Error(w, `{"error":"workflow run not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":          runID,
		"state":       state,
		"started_at":  startedAt,
		"finished_at": finishedAt,
	})
}

func (s *Server) handleGetRunTasks(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")

	rows, err := s.pool.Query(r.Context(), `
		SELECT id, node_id, state, attempt, lease_owner, output_ref, created_at, finished_at
		FROM task_runs
		WHERE workflow_run_id = $1::uuid
		ORDER BY created_at ASC;
	`, runID)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type taskSummary struct {
		ID         string     `json:"id"`
		NodeID     string     `json:"node_id"`
		State      string     `json:"state"`
		Attempt    int        `json:"attempt"`
		LeaseOwner *string    `json:"lease_owner,omitempty"`
		OutputRef  *string    `json:"output_ref,omitempty"`
		CreatedAt  time.Time  `json:"created_at"`
		FinishedAt *time.Time `json:"finished_at,omitempty"`
	}

	var tasks []taskSummary
	for rows.Next() {
		var t taskSummary
		if err := rows.Scan(&t.ID, &t.NodeID, &t.State, &t.Attempt, &t.LeaseOwner, &t.OutputRef, &t.CreatedAt, &t.FinishedAt); err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
			return
		}
		tasks = append(tasks, t)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tasks)
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	keys, err := s.redisClient.Keys(ctx, "worker:*").Result()
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	type workerStatus struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}

	var workers []workerStatus
	for _, key := range keys {
		workerID := strings.TrimPrefix(key, "worker:")
		workers = append(workers, workerStatus{
			ID:     workerID,
			Status: "healthy",
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(workers)
}

func (s *Server) handleKillWorker(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkerID string `json:"worker_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.WorkerID == "" {
		http.Error(w, `{"error":"invalid worker_id"}`, http.StatusBadRequest)
		return
	}

	// Delete heartbeat key in Redis immediately
	key := "worker:" + body.WorkerID
	_ = s.redisClient.Del(r.Context(), key).Err()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"killed_worker": body.WorkerID,
		"status":        "heartbeat_stopped",
	})
}
