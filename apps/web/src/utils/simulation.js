// Structured Simulation Steps for All FlowForge Workflows

export function generateSimulationSteps(workflowType) {
  // 1. DIAMOND PIPELINE SIMULATION
  if (workflowType === 'diamond') {
    return [
      {
        stepNum: 1,
        component: "DAG VALIDATION",
        title: "Workflow Submission & Cycle Detection",
        description: "Validated DAG structure using Kahn's Algorithm (O(V+E)). In-degree computed for all 4 nodes. Root task 'extract' is initialized to READY, while dependents remain BLOCKED.",
        effect: "PostgreSQL inserted workflow and task runs atomically in a single ACID transaction.",
        taskRuns: [
          { node_id: 'extract', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'transform_a', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'transform_b', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.ready', task_run_id: 'extract', payload: { in_degree: 0 } }
      },
      {
        stepNum: 2,
        component: "OUTBOX RELAY",
        title: "Transactional Outbox Event Fan-Out",
        description: "Background Event Relay polled pending events from 'outbox_events' and pushed 'task.ready' for 'extract' to Redis Stream 'stream:tasks'.",
        effect: "Guarantees at-least-once message delivery without dual-write race conditions.",
        taskRuns: [
          { node_id: 'extract', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'transform_a', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'transform_b', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'stream.published', task_run_id: 'extract', payload: { stream: 'stream:tasks' } }
      },
      {
        stepNum: 3,
        component: "ATOMIC LEASE",
        title: "Worker Lease Acquisition & Fencing Token",
        description: "Worker-1 claimed the task via PostgreSQL conditional update ('WHERE state = READY'). Generated unique fencing token (UUID) and set a 15-second lease timer.",
        effect: "Fencing token prevents zombie or slow workers from overwriting newer state.",
        taskRuns: [
          { node_id: 'extract', state: 'LEASED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'transform_a', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'transform_b', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.leased', task_run_id: 'extract', payload: { worker: 'worker-1', lease_duration: '15s' } }
      },
      {
        stepNum: 4,
        component: "TASK EXECUTION",
        title: "HTTP Adapter Execution & Heartbeat",
        description: "Worker-1 launched the HTTP task adapter, executing 'GET https://httpbin.org/get' while renewing its ephemeral heartbeat key in Redis ('worker:worker-1').",
        effect: "Heartbeat prevents scheduler lease-recovery while worker process is healthy.",
        taskRuns: [
          { node_id: 'extract', state: 'RUNNING', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'transform_a', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'transform_b', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.started', task_run_id: 'extract', payload: { adapter: 'http', endpoint: '/get' } }
      },
      {
        stepNum: 5,
        component: "FENCED COMMIT",
        title: "Atomic Task Completion Commit",
        description: "Worker-1 completed request (HTTP 200) and committed output using its fencing token. PostgreSQL verified token matches active lease and transitioned state to SUCCEEDED.",
        effect: "State transition and outbox event 'task.succeeded' committed atomically.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1', output_ref: '{"status": 200, "bytes": 1024}' },
          { node_id: 'transform_a', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'transform_b', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.succeeded', task_run_id: 'extract', payload: { status_code: 200 } }
      },
      {
        stepNum: 6,
        component: "SCHEDULER CTE",
        title: "DAG Dependency Progression Sweep",
        description: "Scheduler executed SQL anti-join CTE to find BLOCKED tasks whose parents are all SUCCEEDED. Promoted child tasks 'transform_a' and 'transform_b' to READY.",
        effect: "Parallel execution unlocked for downstream branches in a single query.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'transform_a', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'transform_b', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.ready', task_run_id: 'transforms', payload: { unlocked: ['transform_a', 'transform_b'] } }
      },
      {
        stepNum: 7,
        component: "PARALLEL WORKERS",
        title: "Concurrent Multi-Worker Lease & Execution",
        description: "Worker-2 acquired lease for 'transform_a' (Python Adapter) and Worker-3 acquired lease for 'transform_b' (Synthetic Adapter). Both executing concurrently.",
        effect: "Demonstrates horizontal worker scalability across Redis consumer group.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'transform_a', state: 'RUNNING', attempt: 1, lease_owner: 'worker-2' },
          { node_id: 'transform_b', state: 'RUNNING', attempt: 1, lease_owner: 'worker-3' },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.started', task_run_id: 'parallel', payload: { workers: ['worker-2', 'worker-3'] } }
      },
      {
        stepNum: 8,
        component: "FENCED COMMIT",
        title: "Parallel Branch Results Committed",
        description: "Both Worker-2 and Worker-3 successfully committed their intermediate data transformation results to PostgreSQL with fencing verification.",
        effect: "Both transform branches reached SUCCEEDED state simultaneously.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'transform_a', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-2', output_ref: '{"cleaned_rows": 500}' },
          { node_id: 'transform_b', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3', output_ref: '{"enriched_rows": 500}' },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.succeeded', task_run_id: 'transforms-done', payload: { branches: 2 } }
      },
      {
        stepNum: 9,
        component: "SCHEDULER CTE",
        title: "Final Fan-In Dependency Resolution",
        description: "Scheduler detected that both parent tasks ('transform_a' & 'transform_b') have succeeded. Promoted final summary task 'report' from BLOCKED to READY.",
        effect: "Fan-in barrier resolved: summary task is now ready for leasing.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'transform_a', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-2' },
          { node_id: 'transform_b', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3' },
          { node_id: 'report', state: 'READY', attempt: 1, lease_owner: null }
        ],
        event: { event_type: 'task.ready', task_run_id: 'report', payload: { node_id: 'report' } }
      },
      {
        stepNum: 10,
        component: "TASK EXECUTION",
        title: "Report Aggregation Task Execution",
        description: "Worker-1 leased 'report' task and synthesized outputs into a final summary artifact ('s3://reports/summary.pdf').",
        effect: "Final node processing in progress.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'transform_a', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-2' },
          { node_id: 'transform_b', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3' },
          { node_id: 'report', state: 'RUNNING', attempt: 1, lease_owner: 'worker-1' }
        ],
        event: { event_type: 'task.started', task_run_id: 'report', payload: { worker: 'worker-1' } }
      },
      {
        stepNum: 11,
        component: "LIFECYCLE TERMINAL",
        title: "Workflow Terminal Completion (SUCCEEDED)",
        description: "All 4 tasks in the DAG reached SUCCEEDED. Scheduler atomically transitioned Workflow Run to SUCCEEDED and emitted 'workflow.succeeded' event.",
        effect: "100% completion with zero lost state updates and strictly enforced dependency ordering.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'transform_a', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-2' },
          { node_id: 'transform_b', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3' },
          { node_id: 'report', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1', output_ref: 's3://reports/summary.pdf' }
        ],
        event: { event_type: 'workflow.succeeded', payload: { total_tasks: 4, run_state: 'SUCCEEDED' } }
      }
    ];
  }

  // 2. 8-TASK DISTRIBUTED BENCHMARK SIMULATION
  if (workflowType === 'fanout') {
    const allBlocked = (overrides = {}) => {
      const base = {
        ingest: { node_id: 'ingest', state: 'BLOCKED', attempt: 0, lease_owner: null },
        shard_1: { node_id: 'shard_1', state: 'BLOCKED', attempt: 0, lease_owner: null },
        shard_2: { node_id: 'shard_2', state: 'BLOCKED', attempt: 0, lease_owner: null },
        shard_3: { node_id: 'shard_3', state: 'BLOCKED', attempt: 0, lease_owner: null },
        shard_4: { node_id: 'shard_4', state: 'BLOCKED', attempt: 0, lease_owner: null },
        shard_5: { node_id: 'shard_5', state: 'BLOCKED', attempt: 0, lease_owner: null },
        shard_6: { node_id: 'shard_6', state: 'BLOCKED', attempt: 0, lease_owner: null },
        aggregate: { node_id: 'aggregate', state: 'BLOCKED', attempt: 0, lease_owner: null }
      };
      Object.entries(overrides).forEach(([k, v]) => {
        base[k] = { ...base[k], ...v };
      });
      return Object.values(base);
    };

    return [
      {
        stepNum: 1,
        component: "DAG VALIDATION",
        title: "8-Task DAG Submitted & Initialized",
        description: "Evaluated in-degree for 8 nodes: 'ingest' (in-degree 0, READY). All 6 compute shards and the 'aggregate' fan-in barrier initialized to BLOCKED.",
        effect: "Massive fan-out dependency graph staged in PostgreSQL.",
        taskRuns: allBlocked({ ingest: { state: 'READY', attempt: 1 } }),
        event: { event_type: 'workflow.started', payload: { total_nodes: 8 } }
      },
      {
        stepNum: 2,
        component: "TASK EXECUTION",
        title: "Root Ingest Task Processing",
        description: "Worker-1 claimed and executed 'ingest' node, partitioning dataset into 6 parallel chunks.",
        effect: "Worker renewing heartbeat key 'worker:worker-1'.",
        taskRuns: allBlocked({ ingest: { state: 'RUNNING', attempt: 1, lease_owner: 'worker-1' } }),
        event: { event_type: 'task.started', task_run_id: 'ingest', payload: { partitions: 6 } }
      },
      {
        stepNum: 3,
        component: "SCHEDULER CTE",
        title: "Massive 6-Node Fan-Out Unlocked",
        description: "Ingest finished SUCCEEDED. Scheduler CTE query evaluated all 6 shards: all parents met! Promoted shards 1 through 6 simultaneously from BLOCKED to READY.",
        effect: "6 tasks fanned out into Redis stream consumer group.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          shard_1: { state: 'READY', attempt: 1 },
          shard_2: { state: 'READY', attempt: 1 },
          shard_3: { state: 'READY', attempt: 1 },
          shard_4: { state: 'READY', attempt: 1 },
          shard_5: { state: 'READY', attempt: 1 },
          shard_6: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'task.ready', payload: { count: 6 } }
      },
      {
        stepNum: 4,
        component: "CLUSTER EXECUTION",
        title: "Multi-Worker Batch Execution (Phase 1)",
        description: "Worker-1 leased shard_1, Worker-2 leased shard_2, Worker-3 leased shard_3. All 3 workers computing in parallel.",
        effect: "Full worker cluster saturation (3/3 active leases).",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          shard_1: { state: 'RUNNING', attempt: 1, lease_owner: 'worker-1' },
          shard_2: { state: 'RUNNING', attempt: 1, lease_owner: 'worker-2' },
          shard_3: { state: 'RUNNING', attempt: 1, lease_owner: 'worker-3' },
          shard_4: { state: 'READY', attempt: 1 },
          shard_5: { state: 'READY', attempt: 1 },
          shard_6: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'cluster.busy', payload: { running: 3, queue_depth: 3 } }
      },
      {
        stepNum: 5,
        component: "CLUSTER EXECUTION",
        title: "Multi-Worker Batch Execution (Phase 2)",
        description: "Shards 1, 2, 3 committed SUCCEEDED. Workers immediately leased remaining shards 4, 5, 6 from the Redis stream queue.",
        effect: "Zero-latency consumer group pipeline drainage.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          shard_2: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-2' },
          shard_3: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3' },
          shard_4: { state: 'RUNNING', attempt: 1, lease_owner: 'worker-1' },
          shard_5: { state: 'RUNNING', attempt: 1, lease_owner: 'worker-2' },
          shard_6: { state: 'RUNNING', attempt: 1, lease_owner: 'worker-3' }
        }),
        event: { event_type: 'task.succeeded', payload: { completed_shards: 3 } }
      },
      {
        stepNum: 6,
        component: "SCHEDULER CTE",
        title: "Fan-In Barrier Resolved",
        description: "All 6 shards successfully completed. Scheduler evaluated 'aggregate' in-degree: all 6 predecessor dependencies satisfied! Promoted 'aggregate' to READY.",
        effect: "6-way fan-in barrier unlocked.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          shard_2: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-2' },
          shard_3: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3' },
          shard_4: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          shard_5: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-2' },
          shard_6: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3' },
          aggregate: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'barrier.cleared', task_run_id: 'aggregate' }
      },
      {
        stepNum: 7,
        component: "LIFECYCLE TERMINAL",
        title: "Benchmark Complete: 8/8 Tasks Succeeded",
        description: "Worker-2 executed 'aggregate' summary task and committed final results. Workflow run marked SUCCEEDED with maximum parallel throughput.",
        effect: "High-throughput parallel DAG lifecycle verified.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          shard_2: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-2' },
          shard_3: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3' },
          shard_4: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          shard_5: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-2' },
          shard_6: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3' },
          aggregate: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-2', output_ref: '{"total_processed": 60000}' }
        }),
        event: { event_type: 'workflow.succeeded', payload: { throughput_tasks_per_sec: 14.2 } }
      }
    ];
  }

  // 3. POISON PILL DLQ SIMULATION
  if (workflowType === 'poison') {
    return [
      {
        stepNum: 1,
        component: "DAG VALIDATION",
        title: "Poison Pill Workflow Initialized",
        description: "Graph loaded with 3 tasks: 'fetch' (in-degree 0), 'poison_task' (has max_retries = 2), and downstream 'sink'.",
        effect: "Testing deterministic crash isolation and Dead-Letter Queue quarantining.",
        taskRuns: [
          { node_id: 'fetch', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'poison_task', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'sink', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'workflow.started' }
      },
      {
        stepNum: 2,
        component: "TASK EXECUTION",
        title: "Fetch Task Completed",
        description: "Worker-1 leased 'fetch' and committed SUCCEEDED. Scheduler promoted 'poison_task' to READY (Attempt: 1).",
        effect: "Poison task queued for delivery.",
        taskRuns: [
          { node_id: 'fetch', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'poison_task', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'sink', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.ready', task_run_id: 'poison_task' }
      },
      {
        stepNum: 3,
        component: "FAILURE & RETRY",
        title: "Task Throws Error ➔ Exponential Backoff",
        description: "Worker-2 leased 'poison_task' and encountered fatal corrupted payload error! Since attempt (1) < max_retries (2), task transitioned to RETRY_WAIT.",
        effect: "Exponential backoff timer started (2s). Prevents immediate retry hammering.",
        taskRuns: [
          { node_id: 'fetch', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'poison_task', state: 'RETRY_WAIT', attempt: 1, lease_owner: 'worker-2' },
          { node_id: 'sink', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.failed', task_run_id: 'poison_task', payload: { error: 'Corrupted payload', backoff: '2s' } }
      },
      {
        stepNum: 4,
        component: "SCHEDULER RETRY",
        title: "Retry Timer Expired ➔ Re-queued (Attempt 2)",
        description: "Scheduler detected backoff elapsed and reset 'poison_task' to READY with attempt = 2.",
        effect: "Task made available for healthy workers to retry.",
        taskRuns: [
          { node_id: 'fetch', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'poison_task', state: 'READY', attempt: 2, lease_owner: null },
          { node_id: 'sink', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.ready', task_run_id: 'poison_task', payload: { attempt: 2 } }
      },
      {
        stepNum: 5,
        component: "FAILURE & RETRY",
        title: "Second Attempt Fails ➔ Max Retries Exhausted",
        description: "Worker-3 leased 'poison_task' and encountered the same fatal error. Attempt (2) reached max_retries (2).",
        effect: "Retry limit exhausted: task cannot be retried automatically.",
        taskRuns: [
          { node_id: 'fetch', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'poison_task', state: 'FAILED', attempt: 2, lease_owner: 'worker-3' },
          { node_id: 'sink', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.failed', task_run_id: 'poison_task', payload: { retries_exhausted: true } }
      },
      {
        stepNum: 6,
        component: "DEAD LETTER QUEUE",
        title: "Task Quarantined to DLQ (Workflow Halted)",
        description: "Poison task transitioned to 'DLQ'. Scheduler emitted critical 'task.dlq' alert. Downstream task 'sink' remains safely BLOCKED to prevent cascade corruption.",
        effect: "Core Invariant: Bad work is isolated and never loops indefinitely.",
        taskRuns: [
          { node_id: 'fetch', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'poison_task', state: 'DLQ', attempt: 2, lease_owner: null, output_ref: 'ERROR: Corrupted payload quarantined to DLQ' },
          { node_id: 'sink', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.dlq', task_run_id: 'poison_task', payload: { quarantined: true } }
      }
    ];
  }

  return [];
}
