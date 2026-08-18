# FlowForge ⚡ 
### Fault-Tolerant Distributed Workflow Orchestration Engine

[![Go Version](https://img.shields.io/badge/Go-1.24%2B-00ADD8?style=for-the-badge&logo=go)](https://go.dev/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io/)
[![React](https://img.shields.io/badge/React-18-61DAFB?style=for-the-badge&logo=react&logoColor=black)](https://react.dev/)
[![YouTube Demo](https://img.shields.io/badge/Demo-YouTube-FF0000?style=for-the-badge&logo=youtube&logoColor=white)](https://youtu.be/CiqwISr2ud8)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

**FlowForge** is a distributed Directed Acyclic Graph (DAG) workflow orchestration engine built from first principles in **Go**. It coordinates multi-step computational pipelines across horizontally scalable worker pools with lease-based task ownership, atomic fencing tokens, transactional outbox event streaming, and automated crash recovery.

---

## 📐 Architecture

![FlowForge Distributed Architecture](https://res.cloudinary.com/dgbgxtsrl/image/upload/v1787039377/flowforge_arch_fwiwo2.png)

---

## ⚙️ System Components

### 1. Control Plane
* **REST API Gateway**: Built with `go-chi/chi/v5`, providing workflow submission, execution triggers, run inspection, and simulation endpoints.
* **Topological Validator**: Uses Kahn’s algorithm to perform cycle detection and in-degree validation before persisting DAG definitions.
* **CTE Scheduler**: Advances workflow runs by querying completed predecessor tasks through an atomic PostgreSQL Common Table Expression (CTE) with anti-joins. Unlocks `BLOCKED` nodes into `READY` states in a single roundtrip.
* **Lease Reaper**: Continuously checks for abandoned or unacknowledged worker leases (`lease_expires_at < NOW()`) and safely resets tasks to `READY` with incremented retry attempts.
* **Transactional Outbox Relay**: Polls pending event records from PostgreSQL and publishes them to Redis Streams, guaranteeing zero state-event divergence (dual-write mitigation).

### 2. Execution Plane
* **Horizontally Scalable Workers**: Autonomous Go worker processes consuming tasks from Redis Streams via `XREADGROUP` consumer groups.
* **Atomic Lease Fencing**: Acquires cryptographic lease tokens (`UUID`) during task pickup. All task state transitions enforce fencing token validation to block stale commits from delayed or zombie workers.
* **Pluggable Adapters**: Built-in support for HTTP endpoints, Python scripts, synthetic latency workloads, and deterministic poison-pill testing.
* **Liveness Heartbeats**: Workers refresh ephemeral Redis keys with short TTLs while running tasks.

### 3. Storage & Streaming
* **PostgreSQL 16 (Durable Store)**: Stores DAG templates, workflow run instances, task run state transitions, execution attempt audit logs, and outbox events. Utilizes partial B-Tree indexes for sub-millisecond query loops.
* **Redis 7.2 / Valkey (Ephemeral Coordination)**: Handles low-latency task queue distribution (`stream:tasks`), real-time event broadcasting (`stream:events`), and worker heartbeat key-value pairs.

### 4. Edge & Dashboard
* **WebSocket Hub**: Pushes real-time stream updates to all connected browser clients over persistent WebSockets.
* **React 18 Dashboard**: Neobrutalist UI featuring live interactive DAG visualization, execution progress counters, event audit feeds, and a built-in Chaos Engineering panel.

---

## 🔄 Task Lifecycle State Machine

```
                    ┌─────────────────────────┐
                    │         BLOCKED         │ (Waiting for upstream predecessors)
                    └────────────┬────────────┘
                                 │ In-Degree reaches 0 (All parents SUCCEEDED)
                                 ▼
                    ┌─────────────────────────┐
                    │          READY          │ (Queued in Redis Stream)
                    └────────────┬────────────┘
                                 │ Worker atomically claims lease (Fencing Token)
                                 ▼
                    ┌─────────────────────────┐
                    │         LEASED          │
                    └────────────┬────────────┘
                                 │ Adapter execution starts + Heartbeat ticker
                                 ▼
                    ┌─────────────────────────┐
                    │         RUNNING         │
                    └─────┬──────────┬────────┘
                          │          │
        Success           │          │ Transient Failure (retries remaining)
        ┌─────────────────┘          └─────────────────┐
        ▼                                              ▼
┌───────────────┐                             ┌─────────────────┐
│   SUCCEEDED   │ (Terminal)                  │   RETRY_WAIT    │ (Exponential backoff)
└───────────────┘                             └────────┬────────┘
        ▲                                              │ Backoff timer expires
        │                                              ▼
        │                                     ┌─────────────────┐
        │                                     │      READY      │
        │                                     └─────────────────┘
        │ Non-retryable failure / Max retries exceeded
        ▼
┌───────────────┐        Poison Pill
│    FAILED     │ ──────────────────────► ┌─────────────────┐
└───────────────┘                         │       DLQ       │ (Dead Letter Queue)
                                          └─────────────────┘
```

---

## 🛡️ Fault Tolerance & Recovery Guarantees

| Failure Scenario | Detection Mechanism | Recovery & Mitigation | Safety Invariant |
| :--- | :--- | :--- | :--- |
| **Worker Crash / OOM** | Expired lease timestamp in PostgreSQL (`lease_expires_at < NOW()`) and dropped Redis heartbeat | Scheduler reclaims task to `READY`, increments `attempt`, and queues for next available worker | Automatic self-healing without restarting the entire workflow. |
| **Zombie / Delayed Worker** | Expired or mismatched `lease_token` | PostgreSQL conditional update matches 0 rows; rejected with `ErrStaleLeaseCommit` | Prevents stale workers from overwriting newer, valid task outputs. |
| **Duplicate Stream Messages** | Concurrent workers attempting to lease the same task | Atomic conditional update (`WHERE state = 'READY'`) ensures only one worker acquires the lease | Exactly-one execution winner per delivery attempt. |
| **Server Crash during Publish** | Database commits state mutation and outbox row in a single ACID transaction | Event Relay restarts, reads unsent records (`sent_at IS NULL`), and resumes publishing to Redis | Zero lost events or unnotified state transitions. |
| **Poison Pill Payload** | Deterministic application error or malformed task payload | Exponential backoff retries exhaust `max_retries` $\rightarrow$ Task quarantined to `DLQ` | Faulty tasks cannot block the queue or exhaust worker compute indefinitely. |
| **Cyclic Dependency Submission** | Kahn's Algorithm topological validation on workflow creation | Graph rejected at API boundary with `ErrCycleDetected` | Guarantees deadlocks cannot be introduced into the engine. |

---

## 📁 Project Structure

```
FlowForge/
├── cmd/
│   └── server/             # Engine daemon entrypoint (API, Scheduler, Relay, Workers)
├── internal/
│   ├── api/                # Chi REST routes, WebSocket hub, and request schemas
│   ├── dag/                # Kahn's algorithm topological sorting & cycle detection
│   ├── outbox/             # Transactional outbox event relay & stream publisher
│   ├── repository/         # PostgreSQL persistence (lease fencing, CTEs, partial indexes)
│   ├── scheduler/          # Dependency progression loop & lease reaper
│   └── worker/             # Worker process engine, heartbeat manager, task adapters
├── pkg/
│   ├── contracts/          # Strongly-typed event envelopes & distributed tracing schemas
│   └── domain/             # Core models, state machine transitions, and domain errors
├── migrations/             # PostgreSQL DDL migrations & partial indexes
├── apps/
│   └── web/                # React 18 Neobrutalist UI (Live DAG visualizer & Chaos panel)
└── docker-compose.yml      # Local services (PostgreSQL 16 + Redis 7.2)
```

---

## 🚀 Quickstart & Local Setup

### 1. Prerequisites
* **Go** (1.24+)
* **Node.js** (18+) & **npm**
* **Docker & Docker Compose**

### 2. Start PostgreSQL & Redis
```bash
docker compose up -d
```
* **PostgreSQL 16**: `localhost:5433` (`flowforge` / `flowforge_password`)
* **Redis 7.2**: `localhost:6379`

### 3. Start Backend Services
```bash
go run ./cmd/server
```
* REST API & WebSocket listener boots on `http://localhost:8080`
* Automatically starts 3 concurrent worker processes (`worker-1`, `worker-2`, `worker-3`), the Outbox Relay (100ms interval), and the CTE Scheduler (200ms interval).

### 4. Start Frontend Dashboard
```bash
cd apps/web
npm install
npm run dev
```
Open **`http://localhost:5173`** to access the DAG visualizer and simulation controls.

---

## 🧪 Automated Test Suite

```bash
# Run all unit and integration tests
go test -v ./...

# 1. DAG Cycle Detection & Topological Sort
go test -v ./internal/dag

# 2. State Machine Transitions (18 Test Scenarios)
go test -v ./pkg/domain

# 3. PostgreSQL Repository & Atomic Lease Fencing
go test -v ./internal/repository

# 4. CTE Dependency Progression & Crash Recovery
go test -v ./internal/scheduler

# 5. Worker Fleet Execution & Adapters
go test -v ./internal/worker

# 6. Transactional Outbox Relay
go test -v ./internal/outbox

# 7. REST API Gateway & WebSockets
go test -v ./internal/api
```

---

## 📄 License
This project is open-source and available under the [MIT License](LICENSE).
