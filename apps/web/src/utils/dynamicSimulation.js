// Dynamic, Reactive Simulation Engine with All Distributed Fault Invariants

export function generateDynamicSimulationSteps(workflowType, workers = []) {
  const deadWorkerIds = new Set(
    workers.filter(w => w.status === 'killed' || w.status === 'dead').map(w => w.id)
  );

  const isAllDead = deadWorkerIds.size >= 3;
  const aliveWorkers = ['worker-1', 'worker-2', 'worker-3'].filter(id => !deadWorkerIds.has(id));
  const fallbackAlive = aliveWorkers[0] || 'worker-1';

  // 1. DIAMOND PIPELINE SIMULATION
  if (workflowType === 'diamond') {
    const steps = [];

    steps.push({
      stepNum: 1,
      component: "DAG VALIDATION",
      title: "Workflow Submitted & Initialized",
      description: "Validated DAG structure with Kahn's Algorithm (0 cycles). Root task 'extract' initialized to READY, while dependents are BLOCKED.",
      effect: "Atomic INSERT into PostgreSQL workflows and task_runs tables.",
      taskRuns: [
        { node_id: 'extract', state: 'READY', attempt: 1, lease_owner: null },
        { node_id: 'transform_a', state: 'BLOCKED', attempt: 0, lease_owner: null },
        { node_id: 'transform_b', state: 'BLOCKED', attempt: 0, lease_owner: null },
        { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
      ],
      event: { event_type: 'task.ready', task_run_id: 'extract' }
    });

    if (isAllDead) {
      steps.push({
        stepNum: 2,
        component: "CLUSTER HALTED",
        title: "⚠️ All Workers Offline (Cluster Stalled)",
        description: "Task 'extract' is waiting in Redis Stream 'stream:tasks', but all 3 workers are DEAD. No task state is lost.",
        effect: "Task safely buffered in queue. Click 'Revive' on any worker to resume processing.",
        taskRuns: [
          { node_id: 'extract', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'transform_a', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'transform_b', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'cluster.halted', payload: { reason: 'zero_active_workers' } }
      });
      return steps;
    }

    const wExtract = aliveWorkers.includes('worker-1') ? 'worker-1' : fallbackAlive;

    steps.push({
      stepNum: 2,
      component: "ATOMIC LEASE",
      title: `Task 'extract' Leased by ${wExtract}`,
      description: `${wExtract} claimed atomic lease in PostgreSQL with UUID fencing token. Renewable heartbeat active in Redis.`,
      effect: "Atomic fencing token protects task from concurrent execution.",
      taskRuns: [
        { node_id: 'extract', state: 'RUNNING', attempt: 1, lease_owner: wExtract },
        { node_id: 'transform_a', state: 'BLOCKED', attempt: 0, lease_owner: null },
        { node_id: 'transform_b', state: 'BLOCKED', attempt: 0, lease_owner: null },
        { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
      ],
      event: { event_type: 'task.started', task_run_id: 'extract', payload: { worker: wExtract } }
    });

    steps.push({
      stepNum: 3,
      component: "FENCED COMMIT",
      title: "Extract Succeeded ➔ Transforms Unlocked",
      description: `${wExtract} committed HTTP response (status 200). Scheduler CTE sweep detected dependencies met: 'transform_a' and 'transform_b' promoted to READY!`,
      effect: "Parallel execution branches unlocked in Redis stream.",
      taskRuns: [
        { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: wExtract, output_ref: '{"status": 200, "rows": 1000}' },
        { node_id: 'transform_a', state: 'READY', attempt: 1, lease_owner: null },
        { node_id: 'transform_b', state: 'READY', attempt: 1, lease_owner: null },
        { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
      ],
      event: { event_type: 'task.ready', payload: { unlocked: ['transform_a', 'transform_b'] } }
    });

    if (deadWorkerIds.has('worker-2')) {
      steps.push({
        stepNum: 4,
        component: "WORKER CRASH",
        title: "💥 Worker-2 Crashed! (Ghost Lease Expired)",
        description: "Worker-2 stopped sending heartbeats to Redis. The lease on 'transform_a' expired in PostgreSQL! Task is temporarily orphaned.",
        effect: "PostgreSQL lease_expires_at < NOW(). Task state flagged as EXPIRED.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: wExtract },
          { node_id: 'transform_a', state: 'LEASED', attempt: 1, lease_owner: 'worker-2', is_expired: true },
          { node_id: 'transform_b', state: 'RUNNING', attempt: 1, lease_owner: aliveWorkers.includes('worker-3') ? 'worker-3' : fallbackAlive },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'worker.crash', payload: { worker_id: 'worker-2', task_orphaned: 'transform_a' } }
      });

      steps.push({
        stepNum: 5,
        component: "SCHEDULER RECOVERY",
        title: "🛡️ Go Scheduler Reclaimed Orphaned Task",
        description: "Scheduler crash-recovery loop executed SQL: 'UPDATE task_runs SET state = READY, lease_owner = NULL, attempt = attempt + 1 WHERE lease_expires_at < NOW()'. Task 'transform_a' recovered to READY (Attempt 2)!",
        effect: "Self-healing triggered. Zero human intervention needed.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: wExtract },
          { node_id: 'transform_a', state: 'READY', attempt: 2, lease_owner: null, is_recovered: true },
          { node_id: 'transform_b', state: 'RUNNING', attempt: 1, lease_owner: aliveWorkers.includes('worker-3') ? 'worker-3' : fallbackAlive },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'scheduler.recovery', payload: { reclaimed_task: 'transform_a', attempt: 2 } }
      });

      const wHealer = fallbackAlive;
      steps.push({
        stepNum: 6,
        component: "SELF-HEALING LEASE",
        title: `🔄 Healthy ${wHealer} Re-Leased 'transform_a'`,
        description: `${wHealer} picked up the orphaned task from Redis stream with a brand new fencing token and executed it to completion.`,
        effect: "New fencing token invalidates any late commits from dead worker-2.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: wExtract },
          { node_id: 'transform_a', state: 'SUCCEEDED', attempt: 2, lease_owner: wHealer },
          { node_id: 'transform_b', state: 'SUCCEEDED', attempt: 1, lease_owner: aliveWorkers.includes('worker-3') ? 'worker-3' : fallbackAlive },
          { node_id: 'report', state: 'READY', attempt: 1, lease_owner: null }
        ],
        event: { event_type: 'task.succeeded', payload: { task_id: 'transform_a', recovered_by: wHealer } }
      });
    } else {
      const wA = aliveWorkers[1] || aliveWorkers[0] || 'worker-2';
      const wB = aliveWorkers[2] || aliveWorkers[0] || 'worker-3';

      steps.push({
        stepNum: 4,
        component: "PARALLEL WORKERS",
        title: `Parallel Execution (${wA} & ${wB})`,
        description: `${wA} leased 'transform_a' and ${wB} leased 'transform_b'. Both computing data transformations concurrently.`,
        effect: "Horizontal scaling across active Redis consumer group.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: wExtract },
          { node_id: 'transform_a', state: 'RUNNING', attempt: 1, lease_owner: wA },
          { node_id: 'transform_b', state: 'RUNNING', attempt: 1, lease_owner: wB },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.started', payload: { workers: [wA, wB] } }
      });

      steps.push({
        stepNum: 5,
        component: "FENCED COMMIT",
        title: "Transforms Finished ➔ Report Ready",
        description: `Both parallel transform tasks succeeded. Scheduler resolved fan-in dependencies and promoted 'report' to READY.`,
        effect: "Summary barrier cleared.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: wExtract },
          { node_id: 'transform_a', state: 'SUCCEEDED', attempt: 1, lease_owner: wA },
          { node_id: 'transform_b', state: 'SUCCEEDED', attempt: 1, lease_owner: wB },
          { node_id: 'report', state: 'READY', attempt: 1, lease_owner: null }
        ],
        event: { event_type: 'task.ready', task_run_id: 'report' }
      });
    }

    const wReport = fallbackAlive;
    steps.push({
      stepNum: steps.length + 1,
      component: "LIFECYCLE COMPLETE",
      title: "Workflow Terminal Completion (SUCCEEDED)",
      description: `${wReport} executed 'report' summary task. All 4 DAG tasks reached SUCCEEDED!`,
      effect: "100% DAG resilience demonstrated.",
      taskRuns: [
        { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: wExtract },
        { node_id: 'transform_a', state: 'SUCCEEDED', attempt: deadWorkerIds.has('worker-2') ? 2 : 1, lease_owner: deadWorkerIds.has('worker-2') ? wReport : 'worker-2' },
        { node_id: 'transform_b', state: 'SUCCEEDED', attempt: 1, lease_owner: aliveWorkers.includes('worker-3') ? 'worker-3' : fallbackAlive },
        { node_id: 'report', state: 'SUCCEEDED', attempt: 1, lease_owner: wReport, output_ref: 's3://reports/summary.pdf' }
      ],
      event: { event_type: 'workflow.succeeded', payload: { status: 'SUCCEEDED' } }
    });

    return steps;
  }

  // 2. ZOMBIE WORKER FENCING PIPELINE (3 Nodes)
  if (workflowType === 'zombie') {
    return [
      {
        stepNum: 1,
        component: "DAG VALIDATION",
        title: "Zombie Fencing Pipeline Initialized",
        description: "Evaluated in-degree: 'auth_check' (in-degree 0, READY). 'long_compute' and 'emit_metrics' staged as BLOCKED.",
        effect: "PostgreSQL transaction committed 3 tasks.",
        taskRuns: [
          { node_id: 'auth_check', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'long_compute', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'emit_metrics', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'workflow.started' }
      },
      {
        stepNum: 2,
        component: "TASK EXECUTION",
        title: "Auth Check Succeeded ➔ Compute Ready",
        description: "Worker-1 executed 'auth_check' and committed SUCCEEDED. Scheduler promoted 'long_compute' to READY.",
        effect: "'long_compute' queued in Redis Stream 'stream:tasks'.",
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'long_compute', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'emit_metrics', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.succeeded', task_run_id: 'auth_check' }
      },
      {
        stepNum: 3,
        component: "LEASE ACQUISITION",
        title: "Worker-2 Leases Long Compute (Token: token-101)",
        description: "Worker-2 acquired lease for 'long_compute' in PostgreSQL. Granted lease_token: 'token-101' with a 15-second expiration timer.",
        effect: "Task state: RUNNING (worker-2, token-101).",
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'long_compute', state: 'RUNNING', attempt: 1, lease_owner: 'worker-2' },
          { node_id: 'emit_metrics', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.leased', payload: { token: 'token-101' } }
      },
      {
        stepNum: 4,
        component: "ZOMBIE HANG / GC",
        title: "🧟 Worker-2 Freezes (Long GC / Network Partition)",
        description: "Worker-2 experienced a 20-second Garbage Collection pause. Heartbeat renewals stopped. Lease expired in PostgreSQL!",
        effect: "PostgreSQL: lease_expires_at < NOW(). Worker-2 is now a Zombie Worker.",
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'long_compute', state: 'LEASED', attempt: 1, lease_owner: 'worker-2', is_expired: true },
          { node_id: 'emit_metrics', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'lease.expired', payload: { expired_token: 'token-101' } }
      },
      {
        stepNum: 5,
        component: "SCHEDULER RECLAIM",
        title: "Go Scheduler Reclaimed Task to READY",
        description: "Scheduler detected expired lease. Reset 'long_compute' to READY and cleared lease token. Incremented attempt to 2.",
        effect: "Task re-queued for healthy workers in Redis.",
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'long_compute', state: 'READY', attempt: 2, lease_owner: null, is_recovered: true },
          { node_id: 'emit_metrics', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.reclaimed', payload: { attempt: 2 } }
      },
      {
        stepNum: 6,
        component: "NEW FENCING TOKEN",
        title: "Worker-1 Leases Task with NEW Token (token-202)",
        description: "Worker-1 leased 'long_compute' and was issued NEW fencing token: 'token-202'. Worker-1 executed and successfully committed output!",
        effect: "State is now SUCCEEDED with valid token-202. 'emit_metrics' unlocked to READY.",
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'long_compute', state: 'SUCCEEDED', attempt: 2, lease_owner: 'worker-1', output_ref: '{"ml_loss": 0.04}' },
          { node_id: 'emit_metrics', state: 'READY', attempt: 1, lease_owner: null }
        ],
        event: { event_type: 'task.succeeded', payload: { token: 'token-202' } }
      },
      {
        stepNum: 7,
        component: "FENCING REJECTION",
        title: "🛑 Zombie Worker-2 Wakes Up ➔ Commit Rejected!",
        description: "Worker-2 woke up from GC freeze and attempted late commit with stale 'token-101'. PostgreSQL query: 'WHERE id = $1 AND lease_token = token-101' matched 0 rows! Commit REJECTED with ErrStaleLeaseCommit.",
        effect: "Core Invariant: Fencing tokens strictly guarantee zombie workers cannot overwrite newer state.",
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'long_compute', state: 'SUCCEEDED', attempt: 2, lease_owner: 'worker-1' },
          { node_id: 'emit_metrics', state: 'READY', attempt: 1, lease_owner: null }
        ],
        event: { event_type: 'lease.fencing_rejected', payload: { stale_token: 'token-101', active_token: 'token-202', rows_affected: 0 } }
      },
      {
        stepNum: 8,
        component: "LIFECYCLE COMPLETE",
        title: "Downstream Metrics Emitted ➔ Pipeline Complete",
        description: "Worker-3 executed final 'emit_metrics' task. All 3 nodes in DAG reached SUCCEEDED without any stale data corruption.",
        effect: "Workflow SUCCEEDED. Complete data consistency preserved.",
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'long_compute', state: 'SUCCEEDED', attempt: 2, lease_owner: 'worker-1' },
          { node_id: 'emit_metrics', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3', output_ref: '{"metrics_sent": true}' }
        ],
        event: { event_type: 'workflow.succeeded' }
      }
    ];
  }

  // 3. DUPLICATE DELIVERY (Payment Processing - 3 Nodes)
  if (workflowType === 'duplicate') {
    return [
      {
        stepNum: 1,
        component: "DAG VALIDATION",
        title: "Payment Workflow Submitted",
        description: "Graph loaded with 3 financial tasks: 'fetch_order' (in-degree 0, READY) ➔ 'process_payment' (BLOCKED) ➔ 'send_receipt' (BLOCKED).",
        effect: "Financial pipeline staged in PostgreSQL.",
        taskRuns: [
          { node_id: 'fetch_order', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'process_payment', state: 'BLOCKED', attempt: 0, lease_owner: null },
          { node_id: 'send_receipt', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'workflow.started' }
      },
      {
        stepNum: 2,
        component: "TASK EXECUTION",
        title: "Order Fetched ➔ Payment Ready",
        description: "Worker-1 fetched order details. Scheduler promoted critical 'process_payment' task to READY in Redis Stream 'stream:tasks'.",
        effect: "'process_payment' eligible for consumer group delivery.",
        taskRuns: [
          { node_id: 'fetch_order', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'process_payment', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'send_receipt', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.ready', task_run_id: 'process_payment' }
      },
      {
        stepNum: 3,
        component: "REDIS STREAM",
        title: "Duplicate Stream Delivery Injected",
        description: "Due to network retries, the same 'process_payment' message was delivered simultaneously to Worker-1 and Worker-2 via consumer group.",
        effect: "Both workers concurrently attempt conditional SQL lease update.",
        taskRuns: [
          { node_id: 'fetch_order', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'process_payment', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'send_receipt', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'stream.duplicate_delivery', payload: { task_id: 'process_payment' } }
      },
      {
        stepNum: 4,
        component: "CONDITIONAL SQL",
        title: "Worker-1 Wins Atomic Lease Update",
        description: "Worker-1 executed 'UPDATE task_runs SET state = LEASED WHERE id = $1 AND state = READY'. Result: 1 row affected (WINNER).",
        effect: "Worker-1 acquires ownership with token-991.",
        taskRuns: [
          { node_id: 'fetch_order', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'process_payment', state: 'RUNNING', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'send_receipt', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'lease.acquired', payload: { winner: 'worker-1' } }
      },
      {
        stepNum: 5,
        component: "IDEMPOTENT REJECT",
        title: "Worker-2 Lease Safely Skipped (0 Rows)",
        description: "Worker-2 executed identical SQL update. Since state is already LEASED, result was 0 rows affected. Worker-2 safely ACKs and discards duplicate message.",
        effect: "Core Invariant: Zero duplicate credit card charge! Exactly-once execution guaranteed.",
        taskRuns: [
          { node_id: 'fetch_order', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'process_payment', state: 'RUNNING', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'send_receipt', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'duplicate.discarded', payload: { worker: 'worker-2', rows_affected: 0 } }
      },
      {
        stepNum: 6,
        component: "FENCED COMMIT",
        title: "Payment Processed ➔ Receipt Sent (100% SUCCEEDED)",
        description: "Worker-1 charged payment and committed output. Downstream 'send_receipt' executed by Worker-3. Entire payment pipeline SUCCEEDED cleanly.",
        effect: "Exactly-once execution semantics achieved with zero double billing.",
        taskRuns: [
          { node_id: 'fetch_order', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          { node_id: 'process_payment', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1', output_ref: '{"charge_id": "ch_981a", "status": "paid"}' },
          { node_id: 'send_receipt', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3', output_ref: '{"email_sent": true}' }
        ],
        event: { event_type: 'workflow.succeeded' }
      }
    ];
  }

  // 4. 8-TASK BENCHMARK
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

    if (isAllDead) {
      return [{
        stepNum: 1,
        component: "CLUSTER HALTED",
        title: "⚠️ All Workers Dead",
        description: "Benchmark workflow cannot proceed because 3/3 workers are offline. Revive at least one worker to start processing shards.",
        effect: "Queue paused.",
        taskRuns: allBlocked({ ingest: { state: 'READY', attempt: 1 } }),
        event: { event_type: 'cluster.halted' }
      }];
    }

    const w1 = aliveWorkers[0] || 'worker-1';
    const w2 = aliveWorkers[1] || w1;
    const w3 = aliveWorkers[2] || w2;

    const steps = [
      {
        stepNum: 1,
        component: "DAG VALIDATION",
        title: "8-Task Cluster Benchmark Initialized",
        description: "Ingest task initialized to READY. 6 compute shards staged in BLOCKED state.",
        effect: "Massive fan-out dependency graph staged.",
        taskRuns: allBlocked({ ingest: { state: 'READY', attempt: 1 } }),
        event: { event_type: 'workflow.started', payload: { nodes: 8 } }
      },
      {
        stepNum: 2,
        component: "TASK EXECUTION",
        title: `Ingest Partitioned by ${w1}`,
        description: `${w1} executed ingest and partitioned data into 6 parallel chunks. Scheduler unlocked all 6 shards simultaneously!`,
        effect: "6 tasks fanned out into Redis stream consumer group.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_1: { state: 'READY', attempt: 1 },
          shard_2: { state: 'READY', attempt: 1 },
          shard_3: { state: 'READY', attempt: 1 },
          shard_4: { state: 'READY', attempt: 1 },
          shard_5: { state: 'READY', attempt: 1 },
          shard_6: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'task.ready', payload: { count: 6 } }
      }
    ];

    if (deadWorkerIds.size > 0) {
      steps.push({
        stepNum: 3,
        component: "CHAOS REBALANCE",
        title: `⚡ Worker Outage Detected: Dead (${Array.from(deadWorkerIds).join(', ')})`,
        description: `Workers (${Array.from(deadWorkerIds).join(', ')}) are DEAD. Stream consumer group re-routed all shards exclusively to healthy workers (${aliveWorkers.join(', ')}).`,
        effect: "Automatic load shedding and rebalancing across surviving worker fleet.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_1: { state: 'RUNNING', attempt: 1, lease_owner: w1 },
          shard_2: { state: 'RUNNING', attempt: 1, lease_owner: w2 },
          shard_3: { state: 'READY', attempt: 1 },
          shard_4: { state: 'READY', attempt: 1 },
          shard_5: { state: 'READY', attempt: 1 },
          shard_6: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'cluster.rebalance', payload: { dead: Array.from(deadWorkerIds), alive: aliveWorkers } }
      });
    } else {
      steps.push({
        stepNum: 3,
        component: "CLUSTER SATURATION",
        title: "Full 3-Worker Parallel Saturation",
        description: `Worker-1, Worker-2, and Worker-3 computing parallel shards simultaneously.`,
        effect: "Maximum cluster throughput.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_1: { state: 'RUNNING', attempt: 1, lease_owner: w1 },
          shard_2: { state: 'RUNNING', attempt: 1, lease_owner: w2 },
          shard_3: { state: 'RUNNING', attempt: 1, lease_owner: w3 },
          shard_4: { state: 'READY', attempt: 1 },
          shard_5: { state: 'READY', attempt: 1 },
          shard_6: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'cluster.busy' }
      });
    }

    steps.push({
      stepNum: steps.length + 1,
      component: "PIPELINE DRAINAGE",
      title: "All Shards Completed Successfully",
      description: `Surviving workers finished all 6 compute shards. Scheduler evaluated 6/6 dependencies met ➔ 'aggregate' promoted to READY.`,
      effect: "6-way fan-in barrier cleared.",
      taskRuns: allBlocked({
        ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
        shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
        shard_2: { state: 'SUCCEEDED', attempt: 1, lease_owner: w2 },
        shard_3: { state: 'SUCCEEDED', attempt: 1, lease_owner: w3 },
        shard_4: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
        shard_5: { state: 'SUCCEEDED', attempt: 1, lease_owner: w2 },
        shard_6: { state: 'SUCCEEDED', attempt: 1, lease_owner: w3 },
        aggregate: { state: 'READY', attempt: 1 }
      }),
      event: { event_type: 'barrier.cleared', task_run_id: 'aggregate' }
    });

    steps.push({
      stepNum: steps.length + 1,
      component: "LIFECYCLE COMPLETE",
      title: "Benchmark SUCCEEDED (8/8 Tasks Complete)",
      description: `${w1} executed aggregate task and committed final dataset. Total tasks: 8, Failures: 0.`,
      effect: "Fault-tolerant benchmark complete.",
      taskRuns: allBlocked({
        ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
        shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
        shard_2: { state: 'SUCCEEDED', attempt: 1, lease_owner: w2 },
        shard_3: { state: 'SUCCEEDED', attempt: 1, lease_owner: w3 },
        shard_4: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
        shard_5: { state: 'SUCCEEDED', attempt: 1, lease_owner: w2 },
        shard_6: { state: 'SUCCEEDED', attempt: 1, lease_owner: w3 },
        aggregate: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1, output_ref: '{"processed_records": 60000}' }
      }),
      event: { event_type: 'workflow.succeeded', payload: { throughput: '14.2 tasks/sec' } }
    });

    return steps;
  }

  // 5. POISON PILL
  if (workflowType === 'poison') {
    const w1 = aliveWorkers[0] || 'worker-1';
    const w2 = aliveWorkers[1] || w1;

    return [
      {
        stepNum: 1,
        component: "DAG VALIDATION",
        title: "Poison Pill Workflow Initialized",
        description: "Graph loaded with 3 tasks: 'fetch' (in-degree 0), 'poison_task' (max_retries = 2), and downstream 'sink'.",
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
        title: `Fetch Completed by ${w1}`,
        description: `${w1} executed 'fetch' and committed SUCCEEDED. Scheduler promoted 'poison_task' to READY (Attempt 1).`,
        effect: "Poison task queued for delivery.",
        taskRuns: [
          { node_id: 'fetch', state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          { node_id: 'poison_task', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'sink', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.ready', task_run_id: 'poison_task' }
      },
      {
        stepNum: 3,
        component: "FAILURE & RETRY",
        title: `Task Throws Error on ${w2} ➔ Exponential Backoff`,
        description: `${w2} leased 'poison_task' and encountered fatal corrupted payload error. Task transitioned to RETRY_WAIT with 2s exponential backoff.`,
        effect: "Prevents retry storm hammering.",
        taskRuns: [
          { node_id: 'fetch', state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          { node_id: 'poison_task', state: 'RETRY_WAIT', attempt: 1, lease_owner: w2 },
          { node_id: 'sink', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.failed', task_run_id: 'poison_task', payload: { error: 'Corrupted payload', backoff: '2s' } }
      },
      {
        stepNum: 4,
        component: "SCHEDULER RETRY",
        title: "Retry Timer Expired ➔ Re-queued (Attempt 2)",
        description: "Scheduler detected backoff elapsed and reset 'poison_task' to READY with attempt = 2.",
        effect: "Task re-queued for second execution attempt.",
        taskRuns: [
          { node_id: 'fetch', state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          { node_id: 'poison_task', state: 'READY', attempt: 2, lease_owner: null },
          { node_id: 'sink', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.ready', task_run_id: 'poison_task', payload: { attempt: 2 } }
      },
      {
        stepNum: 5,
        component: "FAILURE & RETRY",
        title: `Second Attempt Fails on ${w1} ➔ Max Retries Exhausted`,
        description: `${w1} retried 'poison_task' and encountered identical fatal error. Attempt (2) reached max_retries (2).`,
        effect: "Automatic retries exhausted.",
        taskRuns: [
          { node_id: 'fetch', state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          { node_id: 'poison_task', state: 'FAILED', attempt: 2, lease_owner: w1 },
          { node_id: 'sink', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.failed', payload: { retries_exhausted: true } }
      },
      {
        stepNum: 6,
        component: "DEAD LETTER QUEUE",
        title: "Quarantined to DLQ (Workflow Halted Safely)",
        description: "Poison task quarantined to 'DLQ'. Downstream 'sink' safely kept BLOCKED. Poison payload isolated without crashing the engine.",
        effect: "Dead Letter Queue quarantine active.",
        taskRuns: [
          { node_id: 'fetch', state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          { node_id: 'poison_task', state: 'DLQ', attempt: 2, lease_owner: null, output_ref: 'ERROR: Corrupted payload quarantined to DLQ' },
          { node_id: 'sink', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.dlq', task_run_id: 'poison_task', payload: { quarantined: true } }
      }
    ];
  }

  return [];
}
