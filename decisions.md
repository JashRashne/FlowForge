# FlowForge: Architecture & First-Principles Decision Log (`decisions.md`)

This document serves as the living **Architecture Decision Record (ADR)** for FlowForge. It captures the first-principles reasoning, trade-offs, and design choices behind every major component.

---

## 1. Backend Language Selection: Go
- **Context**: Choosing the primary systems programming language for the control plane and execution plane.
- **Decision**: Use **Go (1.24+)**.
- **First-Principles Rationale**:
  1. **Concurrency Primitives**: Go goroutines (~2KB overhead) and channels allow high concurrency for worker pools, heartbeats, and ticker scheduling loops without thread-pool exhaustion.
  2. **Standard Cancellation**: `context.Context` (`context.WithTimeout`, `context.WithCancel`) standardizes timeout propagation and cancellation across HTTP requests, DB queries, and worker execution adapters.
  3. **Industry Standard**: Go is the foundational language of cloud-native infrastructure (Kubernetes, Temporal, Docker, etcd, CockroachDB).
  4. **Single Binary Deployment**: Zero-dependency compilation makes running multi-worker processes locally extremely lightweight.

---

## 2. Workflow Representation: Directed Acyclic Graph (DAG)
- **Context**: Modeling workflow topologies with serial steps, parallel fan-out/fan-in, and dependency chains.
- **Decision**: Model workflows as strict **DAGs** and validate them via **Topological Sorting (Kahn's Algorithm)** before execution.
- **First-Principles Rationale**:
  1. **Acyclicity Invariant**: Circular dependencies ($A \rightarrow B \rightarrow A$) produce infinite deadlocks in a workflow engine.
  2. **In-Degree Scheduling**: A node is runnable if and only if its `InDegree == 0` (all predecessor parent nodes have reached `SUCCEEDED`).
  3. **Complexity**: Kahn's algorithm runs in $O(V + E)$ time, allowing instant validation at workflow submission time.

---

## 3. State Machine & Transition Rules
- **Context**: Coordinating concurrent state changes across the Scheduler, Workers, and Event Relay.
- **Decision**: Enforce a formal, unidirectional state machine on `TaskRun`:
  - `BLOCKED` $\rightarrow$ `READY` $\rightarrow$ `LEASED` $\rightarrow$ `RUNNING` $\rightarrow$ `SUCCEEDED`
  - `RUNNING` $\rightarrow$ `RETRY_WAIT` $\rightarrow$ `READY` (Transient failure with retries remaining)
  - `RUNNING` $\rightarrow$ `FAILED` $\rightarrow$ `DLQ` (Max retries exceeded or poison pill)
  - `LEASED`/`RUNNING` $\rightarrow$ `READY` (Lease expired; recovered by scheduler)
- **First-Principles Rationale**:
  1. **Strict Invariants**: Terminal states (`SUCCEEDED`, `DLQ`) can never be modified.
  2. **Defensive Scheduling**: No task can be leased unless it explicitly reached `READY`.

---

## 4. Delivery Semantics & Lease Fencing
- **Context**: Handling network partitions, slow workers, and message duplicates without corrupting workflow state.
- **Decision**: Implement **At-Least-Once Delivery** combined with **Atomic Lease Fencing** instead of assuming "exactly-once" transport.
- **First-Principles Rationale**:
  1. **The FLP Impossibility / Unreliable Networks**: In asynchronous networks, message loss and latency can cause duplicate deliveries.
  2. **Fencing Tokens**: Every lease grant generates a cryptographically unique `lease_token` (UUID). A worker committing results must supply this token in a conditional SQL `UPDATE`:
     ```sql
     UPDATE task_runs
     SET state = 'SUCCEEDED', output_ref = $1, finished_at = NOW(), version = version + 1
     WHERE id = $2 AND state IN ('LEASED', 'RUNNING') AND lease_token = $3;
     ```
  3. **Stale Worker Protection**: If Worker A hangs past its lease duration and Worker B is granted a new lease with a new token, Worker A's eventual commit affects **0 rows** and is rejected, preventing zombie overwrites.

---

## 5. Persistence Architecture & Transactional Outbox
- **Context**: Keeping PostgreSQL database state in sync with Redis/Valkey message queues without distributed 2PC transactions.
- **Decision**: Use **PostgreSQL as the single source of truth** and implement the **Transactional Outbox Pattern**.
- **First-Principles Rationale**:
  1. **Dual-Write Problem**: Calling `db.Commit()` and `redis.Publish()` sequentially can fail mid-way, causing lost state-change events.
  2. **Atomic Outbox**: Inside the same DB transaction that updates `task_runs`, an event row is inserted into `outbox_events`.
  3. **Event Relay**: A dedicated, idempotent relay loop reads unsent outbox rows (`sent_at IS NULL`), publishes them to Redis streams, and updates `sent_at = NOW()`.
  4. **Ephemeral Queue**: Redis streams carry transient deliveries and heartbeats. If Redis crashes or restarts, zero durable workflow truth is lost.

---

## 6. Standardized Event Envelope
- **Context**: Inter-service communication across Outbox, Redis Streams, and WebSockets.
- **Decision**: Standardize all domain events on an immutable `EventEnvelope` containing `event_id`, `event_type`, `workflow_run_id`, `task_run_id`, `attempt`, `occurred_at`, `trace_id`, and `payload`.
- **First-Principles Rationale**:
  1. **Idempotent Consumers**: `event_id` provides a deduplication key.
  2. **Distributed Tracing**: `trace_id` propagates OpenTelemetry context across process boundaries.
  3. **Latency Observability**: `occurred_at` enables exact measurement of queue wait times and p95 task latencies.
