-- FlowForge Initial Database Schema
-- Version: 000001
-- Description: Core tables for workflows, runs, tasks, lease fencing, and transactional outbox.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Workflow Definitions
CREATE TABLE IF NOT EXISTS workflows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Static Nodes in a Workflow
CREATE TABLE IF NOT EXISTS workflow_nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    task_type TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}',
    max_retries INT NOT NULL DEFAULT 3,
    timeout_sec INT NOT NULL DEFAULT 60,
    CONSTRAINT uq_workflow_node UNIQUE (workflow_id, node_id)
);

-- 3. Static Directed Dependencies (Edges)
CREATE TABLE IF NOT EXISTS workflow_edges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    from_node TEXT NOT NULL,
    to_node TEXT NOT NULL,
    CONSTRAINT uq_workflow_edge UNIQUE (workflow_id, from_node, to_node)
);

-- 4. Workflow Run Instances
CREATE TABLE IF NOT EXISTS workflow_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE RESTRICT,
    state TEXT NOT NULL DEFAULT 'RUNNING',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    error JSONB
);

-- 5. Task Run Instances (with Lease Fencing and OCC)
CREATE TABLE IF NOT EXISTS task_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_run_id UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'BLOCKED',
    attempt INT NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_token UUID,
    lease_expires_at TIMESTAMPTZ,
    retry_at TIMESTAMPTZ,
    input JSONB,
    output_ref TEXT,
    error JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT uq_run_node UNIQUE (workflow_run_id, node_id)
);

-- 6. Transactional Outbox Events
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    workflow_run_id UUID NOT NULL,
    task_run_id UUID,
    payload JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ
);

-- 7. Task Execution Attempt Audit Log
CREATE TABLE IF NOT EXISTS task_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_run_id UUID NOT NULL REFERENCES task_runs(id) ON DELETE CASCADE,
    attempt INT NOT NULL,
    worker_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    logs TEXT,
    error JSONB,
    CONSTRAINT uq_task_attempt UNIQUE (task_run_id, attempt)
);

-- Indexes for Fast Scheduler Loops & Event Relay
CREATE INDEX IF NOT EXISTS idx_task_runs_ready ON task_runs (state, retry_at) WHERE state = 'READY';
CREATE INDEX IF NOT EXISTS idx_task_runs_lease_check ON task_runs (state, lease_expires_at) WHERE state IN ('LEASED', 'RUNNING');
CREATE INDEX IF NOT EXISTS idx_task_runs_lookup ON task_runs (workflow_run_id, state);
CREATE INDEX IF NOT EXISTS idx_outbox_unsent ON outbox_events (created_at) WHERE sent_at IS NULL;
