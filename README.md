# FlowForge: Fault-Tolerant Distributed Workflow Orchestration Engine

> **"The project should be impressive because it fails well."**
> 
> FlowForge is a distributed workflow engine built from first principles in **Go**, designed to coordinate complex DAGs across horizontally scaled worker processes with lease-based ownership, atomic fencing tokens, transactional outbox event propagation, and automatic self-healing crash recovery.

---

## 🌟 Architecture & First-Principles Overview

```
                                  PUBLIC EDGE
         ┌─────────────────────────────────────────────────────────────┐
         │  React Dashboard (Neobrutalist Live DAG & Chaos Lab)        │
         └──────────────────────────────┬──────────────────────────────┘
                                        │ REST / WebSockets
                                        ▼
                                 CONTROL PLANE
         ┌─────────────────────────────────────────────────────────────┐
         │ • Chi REST API & WebSocket Hub                              │
         │ • Transactional Outbox Event Relay (Pumps events to Redis)  │
         │ • Scheduler Engine (CTE Dependency Progression & Recovery)  │
         │                                                             │
         │   [ PostgreSQL 16 ]            [ Valkey / Redis 7.2 ]       │
         │   (Durable Source of Truth)    (Stream Queues & Heartbeats) │
         └──────────────────────────────┬──────────────────────────────┘
                                        │ Consumer Groups (XREADGROUP)
                                        ▼
                                 EXECUTION PLANE
         ┌─────────────────────────────────────────────────────────────┐
         │  [ Worker 1 ]         [ Worker 2 ]         [ Worker 3 ]     │
         │  • Atomic Lease Acquisition (Postgres Fencing Token)        │
         │  • Dynamic Task Adapters (HTTP, Python, Synthetic)          │
         │  • Heartbeat Tickers & Effectively-Once Result Commits      │
         └─────────────────────────────────────────────────────────────┘
```

---

## 🛡️ Core Reliability Invariants

| Failure Scenario | What FlowForge Detects | Self-Healing Recovery Mechanism | Core Invariant |
| :--- | :--- | :--- | :--- |
| **Worker Process Crash** | Heartbeat disappears; lease expires in PostgreSQL | Scheduler reclaims task to `READY` with attempt $+1$; reassigns to active worker | Progress resumes without manual workflow restart. |
| **Zombie / Slow Worker Overwrites** | Worker wakes from long GC/hang and attempts commit | Fencing Token check matches 0 rows; rejected with `ErrStaleLeaseCommit` | Newer worker ownership is protected from stale overwrites. |
| **Duplicate Message Delivery** | Two workers receive the same task message from stream | Atomic conditional SQL update (`WHERE state = 'READY'`) allows only 1 winner | Duplicate delivery does not cause duplicate execution. |
| **Database Commit vs Publish Crash** | Server dies after DB write but before stream publish | Transactional Outbox table commits state and event atomically; Relay retries | Zero lost state-change notifications. |
| **Poison Pill Task Failure** | Deterministic code bug or malformed payload | Exponential backoff retry exhausted $\rightarrow$ Task quarantined to `DLQ` | Bad work does not loop forever. |

---

## 🚀 Quickstart & Local Setup

### 1. Prerequisites
- **Go** (1.24+)
- **Node.js** (v18+) & **npm**
- **Docker & Docker Compose**

### 2. Start PostgreSQL & Redis
```bash
docker compose up -d
```
* PostgreSQL running on `localhost:5433` (Database: `flowforge`, User: `flowforge`)
* Redis running on `localhost:6379`

### 3. Run Backend Engine
```bash
go run ./cmd/server
```
* REST API & WebSocket server starts on `http://localhost:8080`
* 3 background workers (`worker-1`, `worker-2`, `worker-3`), Scheduler loop, and Event Relay start automatically.

### 4. Start React Neobrutalist UI
```bash
cd apps/web
npm install
npm run dev
```
Open **`http://localhost:5173`** in your browser.

---

## 🧪 Running the Automated Test Matrix

```bash
# Run all unit and integration tests
go test -v ./...

# Test DAG Cycle Detection & Kahn's Algorithm
go test -v ./internal/dag

# Test State Machine Transition Invariants (18 Table Cases)
go test -v ./pkg/domain

# Test Atomic Lease Fencing & Transactional Outbox against PostgreSQL
go test -v ./internal/repository

# Test Multi-Worker End-to-End Execution
go test -v ./internal/worker

# Test Scheduler CTE Progression & Crash Recovery
go test -v ./internal/scheduler

# Test REST API Gateway & WebSockets
go test -v ./internal/api
```

---

## 📖 Architectural Decision Records (ADRs)
For deep mathematical and first-principles breakdowns of all 14 architectural decisions, refer to **[`decisions.md`](decisions.md)**.
