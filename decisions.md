# FlowForge: Architecture & First-Principles Decision Log (`decisions.md`)

This document serves as the living **Architecture Decision Record (ADR)** and comprehensive First-Principles Engineering Guide for **FlowForge**. It captures the deep theoretical foundations, concrete failure scenarios, algorithms, and architectural tradeoffs behind every component we build.

---

## 📖 Core Philosophy
> *"The project should be impressive because it fails well."*
> 
> Instead of building another standard CRUD application, FlowForge is designed around the reality of distributed systems: **networks drop packets, workers crash mid-execution, messages arrive out-of-order or duplicated, and downstream systems hang.** The strength of FlowForge lies in its ability to guarantee safety invariants and automatic self-healing under chaos.

---

## Table of Contents
1. [ADR-01: Systems Programming Language Selection (Go)](#adr-01-systems-programming-language-selection-go)
2. [ADR-02: Workflows as Directed Acyclic Graphs (DAGs) & Kahn's Algorithm](#adr-02-workflows-as-directed-acyclic-graphs-dags--kahns-algorithm)
3. [ADR-03: Formal Task State Machine & Invariant Enforcement](#adr-03-formal-task-state-machine--invariant-enforcement)
4. [ADR-04: Delivery Semantics, Zombie Workers & Atomic Lease Fencing](#adr-04-delivery-semantics-zombie-workers--atomic-lease-fencing)
5. [ADR-05: Storage Separation — Durable Truth (Postgres) vs Ephemeral Coordination (Redis)](#adr-05-storage-separation--durable-truth-postgres-vs-ephemeral-coordination-redis)
6. [ADR-06: Solving the Dual-Write Problem with Transactional Outbox](#adr-06-solving-the-dual-write-problem-with-transactional-outbox)
7. [ADR-07: Unified Event Envelope & Distributed Tracing](#adr-07-unified-event-envelope--distributed-tracing)
8. [ADR-08: Partial Indexing Strategy for High-Throughput Scheduler Loops](#adr-08-partial-indexing-strategy-for-high-throughput-scheduler-loops)
9. [ADR-09: Transactional Workflow Run Initialization & Outbox Fan-out](#adr-09-transactional-workflow-run-initialization--outbox-fan-out)

---

## ADR-01: Systems Programming Language Selection (Go)

### The Context
Distributed workflow engines (like Temporal, Kubernetes, and etcd) require high concurrency, explicit failure handling, non-blocking I/O, precise timeout controls, and minimal memory overhead.

### The Decision
Build the FlowForge backend entirely in **Go (1.24+)**.

### First-Principles Rationale
1. **Goroutines vs OS Threads**:
   - Traditional OS threads (Java, C++, Python multi-threading) allocate ~1MB to ~2MB of stack space per thread. Managing 5,000 concurrent tasks would consume 5–10 GB of RAM and trigger severe OS context-switching overhead.
   - Go's green threads (**goroutines**) start with a tiny **2 KB stack** that grows dynamically on demand. Scheduling is handled in user-space by Go's $M:N$ work-stealing runtime scheduler (`runtime.gogo`), enabling tens of thousands of concurrent worker tasks on a standard laptop using negligible RAM.
2. **First-Class Cancellation via `context.Context`**:
   - In distributed systems, if a client disconnects, a parent workflow aborts, or a task times out, we must prevent "wasted computation" (resource leaks).
   - Go's `context.Context` (`context.WithTimeout`, `context.WithCancel`) standardizes cancellation propagation across HTTP handlers, SQL database queries, Redis stream listeners, and worker execution subprocesses.
3. **Explicit Error Handling (No Hidden Exceptions)**:
   - Go does not have unchecked runtime exceptions. Functions return explicit errors `(result, error)`. This forces every step of our distributed coordination logic to explicitly handle and categorize failures (transient vs fatal vs retryable).
4. **Single Static Binary Deployment**:
   - Go compiles down to a single standalone machine-code binary with zero external runtime dependencies (unlike JVM or Python runtimes), making containerization and local Docker multi-node simulation lightning fast.

---

## ADR-02: Workflows as Directed Acyclic Graphs (DAGs) & Kahn's Algorithm

### The Context
Real-world workflows consist of interdependent tasks: some can run simultaneously (fan-out), while others require multiple upstream tasks to finish before they can begin (fan-in).

```
         [ Upload Dataset ] (Root Node: In-Degree = 0)
                 │
                 ▼
         [ Extract & Validate ]
               /   │   \
              ▼    ▼    ▼  (Parallel Fan-Out)
       [Analyze] [Index] [Summarize]
              \    │    /
               ▼   ▼   ▼  (Fan-In: In-Degree = 3)
         [ Generate Report ] (Waits for all 3 to finish)
```

### The Decision
Represent workflows as strict **Directed Acyclic Graphs (DAGs)**. Validate all graph submissions using **Kahn's Algorithm for Topological Sorting** before accepting them into the system.

### First-Principles Breakdown

#### 1. Why "Directed"?
Every dependency has a clear unidirectional precedence constraint: $A \rightarrow B$ means "$A$ must successfully finish before $B$ can start".

#### 2. Why "Acyclic"?
If a graph contains a cycle ($A \rightarrow B \rightarrow C \rightarrow A$), $A$ waits for $C$, $C$ waits for $B$, and $B$ waits for $A$. This creates an **unresolvable distributed deadlock** where tasks wait indefinitely.

#### 3. How Kahn's Algorithm Operates:
1. **Compute `InDegree` for every node**: Count how many direct predecessor dependencies each node has.
   - Nodes with `InDegree == 0` have no dependencies and can run immediately (**Root Tasks**).
2. **Initialize a Queue** containing all nodes with `InDegree == 0`.
3. **Iterative Reduction**:
   - Pop a node $U$ from the queue and record it in the topological execution sequence.
   - For every downstream dependent child $V$ ($U \rightarrow V$):
     - Decrement $V$'s remaining `InDegree` by 1 (simulating that predecessor $U$ completed).
     - If $V$'s `InDegree` reaches `0` (all its required parents have finished), push $V$ into the queue.
4. **Cycle Detection Invariant**:
   - If `len(result) == len(total_nodes)`: The graph is **guaranteed acyclic and valid**.
   - If `len(result) < len(total_nodes)`: A **cycle exists**, and the scheduler rejects the workflow immediately with `ErrCycleDetected`.

---

## ADR-03: Formal Task State Machine & Invariant Enforcement

### The Context
In an asynchronous system, multiple actors (Scheduler loop, Worker pool, Event Relay, Manual Admin actions) attempt to update task states concurrently. Without strict state machine validation, race conditions corrupt the workflow lifecycle.

### The Decision
Enforce a formal, deterministic state machine with strict transition guards on every `TaskRun`.

```
                    ┌─────────────────────────┐
                    │         BLOCKED         │ (Waiting for predecessors to SUCCEED)
                    └────────────┬────────────┘
                                 │ All parents succeeded (In-Degree reaches 0)
                                 ▼
                    ┌─────────────────────────┐
                    │          READY          │ (Eligible for worker pickup in stream)
                    └────────────┬────────────┘
                                 │ Worker atomically acquires lease
                                 ▼
                    ┌─────────────────────────┐
                    │         LEASED          │ (Worker holds unique fencing token)
                    └────────────┬────────────┘
                                 │ Worker starts adapter execution
                                 ▼
                    ┌─────────────────────────┐
                    │         RUNNING         │ (Active execution + heartbeat ticker)
                    └─────┬──────────┬────────┘
                          │          │
        Success           │          │ Transient Error (retries remaining)
        ┌─────────────────┘          └─────────────────┐
        ▼                                              ▼
┌───────────────┐                             ┌─────────────────┐
│   SUCCEEDED   │ (Terminal)                  │   RETRY_WAIT    │ (Exponential backoff)
└───────────────┘                             └────────┬────────┘
        ▲                                              │ Backoff timer elapses
        │                                              ▼
        │                                     ┌─────────────────┐
        │                                     │      READY      │
        │                                     └─────────────────┘
        │ Fatal Error / Max Retries Exceeded
        ▼
┌───────────────┐        Poison Pill
│    FAILED     │ ──────────────────────► ┌─────────────────┐
└───────────────┘                         │       DLQ       │ (Dead Letter Queue)
                                          └─────────────────┘
```

### Invariant Rules:
1. **Terminal State Immutability**: Once a task enters `SUCCEEDED` or `DLQ`, it can **never** transition again.
2. **No Leapfrogging**: A task cannot skip from `BLOCKED` directly to `RUNNING` or `SUCCEEDED`.
3. **Lease Expiry Recovery**: If a worker holding a `LEASED` or `RUNNING` task crashes, the Scheduler detects the expired lease and moves the task back to `READY`.

---

## ADR-04: Delivery Semantics, Zombie Workers & Atomic Lease Fencing

### The Context: The "Zombie Worker" Dilemma
In distributed systems, networks are asynchronous. A worker may not be dead; it might just be experiencing a **long garbage collection pause, CPU throttle, or temporary network partition**.

#### The Catastrophic Failure Scenario:
```
Time 0s: Worker A leases Task #1 with a 10-second lease (Lease Token = Token-A).
Time 2s: Worker A encounters a 15-second network hang / GC pause.
Time 10s: Lease expires in Postgres.
Time 11s: Scheduler reclaims Task #1, marks it READY, and queues it.
Time 12s: Worker B leases Task #1 (Lease Token = Token-B), runs it, and commits SUCCEEDED.
Time 17s: Worker A suddenly wakes up! Unaware that it was replaced, Worker A attempts
          to write its stale output to Task #1.
```
Without protection, **Worker A overwrites Worker B's correct result with stale data**, corrupting downstream tasks!

### The Decision: Atomic Lease Fencing & Optimistic Concurrency
1. **Never promise "Exactly-Once" Delivery**: Network transport is **At-Least-Once**. Messages may be redelivered.
2. **Enforce Effectively-Once Execution via SQL Fencing Tokens**:
   - When a lease is granted, Postgres generates a cryptographically unique `lease_token` (UUID).
   - When a worker finishes, it must commit using a conditional SQL query:
     ```sql
     UPDATE task_runs
     SET state = 'SUCCEEDED',
         output_ref = $1,
         finished_at = NOW(),
         version = version + 1
     WHERE id = $2
       AND state IN ('LEASED', 'RUNNING')
       AND lease_token = $3;  -- FENCING GUARD
     ```
3. **Outcome**:
   - Worker B committed using `Token-B` $\rightarrow$ Updated 1 row.
   - When Worker A wakes up and sends `Token-A` $\rightarrow$ SQL matches **0 rows**.
   - The Go repository checks `cmdTag.RowsAffected() == 0` and returns `domain.ErrStaleLeaseCommit`. The stale commit is rejected safely!

---

## ADR-05: Storage Separation — Durable Truth (Postgres) vs Ephemeral Coordination (Redis)

### The Context
Should we store workflow state in Redis for speed, or in PostgreSQL for safety?

### The Decision
Strictly decouple **Durable Business Truth** from **Ephemeral Transport & Coordination**.

| Layer | Technology | Role & Guarantees |
| :--- | :--- | :--- |
| **Durable State of Truth** | **PostgreSQL 16** | Holds all workflow definitions, task state transitions, run history, and audit logs. ACID transactions ensure consistency. |
| **Ephemeral Coordination** | **Redis / Valkey 7.2** | Low-latency stream distribution (`stream:tasks`), WebSocket pub/sub fan-out (`stream:events`), and worker heartbeats with TTLs (`worker:{id}`). |

### First-Principles Rationale:
- If Redis crashes, experiences memory pressure, or loses power, **zero workflow state is lost**. The system reconnects to PostgreSQL, replays the outbox, and resumes normal operations.
- Redis is configured with `maxmemory-policy: noeviction` so memory limits fail safely rather than silently dropping active queue items.

---

## ADR-06: Solving the Dual-Write Problem with Transactional Outbox

### The Context
When a task state changes (e.g. `BLOCKED` $\rightarrow$ `READY`), we need to:
1. Update the database (`UPDATE task_runs ...`).
2. Publish an event to the Redis task queue so workers can pick it up.

#### Why Simple Dual-Writes Fail:
```go
// ❌ NAIVE DUAL-WRITE (DANGEROUS BUG)
func OnTaskReady(taskID string) error {
    db.Exec("UPDATE task_runs SET state = 'READY' WHERE id = $1", taskID)
    // IF THE PROCESS CRASHES OR NETWORK DROPS HERE:
    // The DB is updated, but Redis NEVER gets the message!
    // The task is stuck in READY forever!
    redis.XAdd("stream:tasks", ...) 
}
```

### The Decision: The Transactional Outbox Pattern
Instead of calling Redis directly during business logic execution:
1. Inside the **exact same PostgreSQL ACID transaction**, we update `task_runs` AND insert a record into `outbox_events` (`sent_at = NULL`).
2. A separate background **Event Relay** process reads unsent outbox rows, publishes them to Redis streams, and marks `sent_at = NOW()`.

```
[ HTTP / Scheduler ]
        │
        ▼ (Single ACID Transaction)
┌───────────────────────────────────────────────────────────┐
│ PostgreSQL                                                │
│   1. UPDATE task_runs SET state = 'READY'                 │
│   2. INSERT INTO outbox_events (event_type, payload, ...) │
└───────────────────────────────────────────────────────────┘
        │
        ▼ (Poll / LISTEN)
[ Event Relay Process ] ───► [ Publish to Redis Stream ] ───► [ UPDATE outbox_events SET sent_at = NOW() ]
```

---

## ADR-07: Unified Event Envelope & Distributed Tracing

### The Context
Events must flow across multiple boundaries: Database Outbox $\rightarrow$ Redis Streams $\rightarrow$ Worker Fleets $\rightarrow$ WebSocket Clients $\rightarrow$ React UI.

### The Decision
All events across the entire platform conform to an immutable, strongly-typed **`EventEnvelope`**:

```json
{
  "event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "event_type": "task.ready",
  "workflow_run_id": "e4eaaaf2-d142-11e1-b3e4-080027620cdd",
  "task_run_id": "a8098c1a-f86e-11da-bd1a-00112444be1e",
  "attempt": 1,
  "occurred_at": "2026-08-18T00:15:00.000Z",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "payload": {
    "task_type": "http",
    "priority": 5
  }
}
```

### Purpose of Fields:
- `event_id`: Unique UUID for consumer-side **idempotency & deduplication**.
- `trace_id`: OpenTelemetry W3C trace context, enabling end-to-end distributed latency tracing from API submission to final commit.
- `occurred_at`: Precise UTC timestamp for computing queue wait times and p95 latencies.

---

## ADR-08: Partial Indexing Strategy for High-Throughput Scheduler Loops

### The Context
The Scheduler loop executes every 200–500ms to find ready tasks and check for expired leases. A full table scan across millions of historical task runs would saturate CPU and lock tables.

### The Decision
Create **Partial (Filtered) Indexes** in PostgreSQL that index only active rows:

```sql
-- 1. Index only tasks that are currently READY for scheduling
CREATE INDEX idx_task_runs_ready 
ON task_runs (state, retry_at) 
WHERE state = 'READY';

-- 2. Index only active leases for quick expiry detection
CREATE INDEX idx_task_runs_lease_check 
ON task_runs (state, lease_expires_at) 
WHERE state IN ('LEASED', 'RUNNING');

-- 3. Index only unsent outbox events for the Event Relay
CREATE INDEX idx_outbox_unsent 
ON outbox_events (created_at) 
WHERE sent_at IS NULL;
```

### Why Partial Indexes?
- In a system with 10,000,000 completed tasks and only 50 active tasks, a standard index indexes all 10 million rows.
- A **partial index** indexes **only the 50 active rows**, fitting entirely in L1/L2 CPU cache and executing in sub-millisecond time.

---

## ADR-09: Transactional Workflow Run Initialization & Outbox Fan-out

### The Context
Starting a workflow execution involves multiple atomic requirements:
1. Validating that the workflow DAG definition is acyclic.
2. Initializing a `workflow_runs` record.
3. Instantiating `task_runs` for all nodes in the DAG with initial state dependencies resolved (roots as `READY`, dependents as `BLOCKED`).
4. Emitting notification events (`workflow.started` and `task.ready` for roots) so the Worker fleet and WebSocket UI immediately receive them.

If any failure occurs midway (e.g. power loss after creating the run but before inserting the root tasks), the workflow is corrupted.

### The Decision
Perform graph resolution in-memory (`dag.BuildGraph` + `GetRoots()`) and execute all database inserts and outbox event emissions within a **single PostgreSQL ACID transaction**:

```
[ POST /workflows/{id}/runs ]
          │
          ▼
┌─────────────────────────────────────────────────────────────┐
│ 1. dag.BuildGraph(wfDef) -> graph.TopologicalSort()         │
│ 2. roots = graph.GetRoots() (InDegree == 0)                 │
│ 3. BEGIN TRANSACTION                                        │
│    ├── INSERT INTO workflow_runs (state = 'RUNNING')        │
│    ├── INSERT INTO task_runs:                               │
│    │     • root_nodes   -> state = 'READY'                  │
│    │     • child_nodes  -> state = 'BLOCKED'                │
│    └── INSERT INTO outbox_events:                           │
│          • "workflow.started"                               │
│          • "task.ready" (for each root node)                │
│    COMMIT TRANSACTION                                       │
└─────────────────────────────────────────────────────────────┘
```

### First-Principles Rationale
1. **Zero Intermediate State Exposure**: Other transactions will never see a `workflow_run` without its corresponding tasks, or tasks in `READY` without their corresponding outbox events.
2. **Crash-Resilience**: If the process crashes during submission, the database automatically rolls back completely. If it commits, the pending outbox events are guaranteed to be picked up by the Event Relay upon restart.

