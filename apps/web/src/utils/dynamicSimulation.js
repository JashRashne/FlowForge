// Dynamic, Reactive Simulation Engine with Worker Fault Injection & Self-Healing for all 5 Scenarios

export function generateDynamicSimulationSteps(workflowType, workers = []) {
  const deadWorkerIds = new Set(
    workers.filter(w => w.status === 'killed' || w.status === 'dead').map(w => w.id)
  );

  const isAllDead = deadWorkerIds.size >= 3;
  const aliveWorkers = ['worker-1', 'worker-2', 'worker-3'].filter(id => !deadWorkerIds.has(id));
  const fallbackAlive = aliveWorkers[0] || 'worker-1';
  const secondAlive = aliveWorkers[1] || fallbackAlive;
  const thirdAlive = aliveWorkers[2] || secondAlive;

  // Helper for Cluster Halted step
  const getClusterHaltedStep = (initialTasks) => ({
    stepNum: 1,
    component: "CLUSTER HALTED",
    title: "⚠️ All Workers Offline (Cluster Stalled)",
    description: "All 3 workers are DEAD. Tasks remain safely queued in Redis and PostgreSQL. Zero data loss.",
    effect: "Queue paused. Click any worker to revive it and resume execution.",
    taskRuns: initialTasks.map(t => ({ ...t, lease_owner: null, state: t.state === 'RUNNING' || t.state === 'LEASED' ? 'READY' : t.state })),
    event: { event_type: 'cluster.halted', payload: { alive_workers: 0 } }
  });

  // 1. DIAMOND PIPELINE SIMULATION
  if (workflowType === 'diamond') {
    const initTasks = [
      { node_id: 'extract', state: 'READY', attempt: 1, lease_owner: null },
      { node_id: 'transform_a', state: 'BLOCKED', attempt: 0, lease_owner: null },
      { node_id: 'transform_b', state: 'BLOCKED', attempt: 0, lease_owner: null },
      { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
    ];

    if (isAllDead) return [getClusterHaltedStep(initTasks)];

    const steps = [];

    // Step 1: Initialized
    steps.push({
      stepNum: 1,
      component: "DAG VALIDATION",
      title: "Workflow Initialized (Kahn's Algorithm)",
      description: "DAG validated with 0 cycles. Root task 'extract' set to READY; dependent branches staged as BLOCKED.",
      effect: "Atomic INSERT into PostgreSQL workflows and task_runs tables.",
      taskRuns: initTasks,
      event: { event_type: 'workflow.started' }
    });

    // Step 2: Extract Leased & Executing
    steps.push({
      stepNum: 2,
      component: "ATOMIC LEASE",
      title: `Task 'extract' Leased by ${fallbackAlive}`,
      description: `${fallbackAlive} acquired atomic lease in PostgreSQL with UUID fencing token.`,
      effect: "Active Redis heartbeat keeps lease renewed.",
      taskRuns: [
        { node_id: 'extract', state: 'RUNNING', attempt: 1, lease_owner: fallbackAlive },
        { node_id: 'transform_a', state: 'BLOCKED', attempt: 0, lease_owner: null },
        { node_id: 'transform_b', state: 'BLOCKED', attempt: 0, lease_owner: null },
        { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
      ],
      event: { event_type: 'task.started', task_run_id: 'extract', payload: { worker: fallbackAlive } }
    });

    // Step 3: Extract Succeeded ➔ Transforms Unlocked
    steps.push({
      stepNum: 3,
      component: "FENCED COMMIT",
      title: "Extract Succeeded ➔ Transforms Unlocked",
      description: `${fallbackAlive} committed HTTP extract output. Scheduler CTE resolved in-degrees: 'transform_a' and 'transform_b' promoted to READY!`,
      effect: "Parallel execution branches ready for worker consumption.",
      taskRuns: [
        { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive, output_ref: '{"status": 200, "rows": 1000}' },
        { node_id: 'transform_a', state: 'READY', attempt: 1, lease_owner: null },
        { node_id: 'transform_b', state: 'READY', attempt: 1, lease_owner: null },
        { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
      ],
      event: { event_type: 'task.ready', payload: { unlocked: ['transform_a', 'transform_b'] } }
    });

    // IF ANY WORKER IS DEAD: SHOW EXPLICIT 3-PHASE RECOVERY!
    if (deadWorkerIds.size > 0) {
      const deadWorkerName = Array.from(deadWorkerIds)[0];
      steps.push({
        stepNum: 4,
        component: "WORKER CRASH",
        title: `💥 ${deadWorkerName} Crashed! (Ghost Lease Expired)`,
        description: `${deadWorkerName} heartbeat timed out in Redis. Lease on 'transform_a' expired in PostgreSQL. Task temporarily orphaned.`,
        effect: "PostgreSQL: lease_expires_at < NOW().",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'transform_a', state: 'LEASED', attempt: 1, lease_owner: deadWorkerName, is_expired: true },
          { node_id: 'transform_b', state: 'RUNNING', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'worker.crash', payload: { dead_worker: deadWorkerName, orphaned: 'transform_a' } }
      });

      steps.push({
        stepNum: 5,
        component: "SCHEDULER RECOVERY",
        title: "🛡️ Go Scheduler Reclaimed Orphaned Task",
        description: "Scheduler executed crash recovery CTE: Reclaimed 'transform_a' back to READY and incremented attempt to 2.",
        effect: "Self-healing triggered. Zero human intervention needed.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'transform_a', state: 'READY', attempt: 2, lease_owner: null, is_recovered: true },
          { node_id: 'transform_b', state: 'RUNNING', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.reclaimed', payload: { task_id: 'transform_a', attempt: 2 } }
      });

      steps.push({
        stepNum: 6,
        component: "SELF-HEALING LEASE",
        title: `🔄 Healthy ${fallbackAlive} Re-Leased 'transform_a'`,
        description: `Healthy ${fallbackAlive} leased orphaned task with fresh fencing token and executed to completion.`,
        effect: "New fencing token invalidates any zombie late commits.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'transform_a', state: 'SUCCEEDED', attempt: 2, lease_owner: fallbackAlive },
          { node_id: 'transform_b', state: 'SUCCEEDED', attempt: 1, lease_owner: secondAlive },
          { node_id: 'report', state: 'READY', attempt: 1, lease_owner: null }
        ],
        event: { event_type: 'task.succeeded', payload: { task_id: 'transform_a', recovered_by: fallbackAlive } }
      });
    } else {
      steps.push({
        stepNum: 4,
        component: "PARALLEL WORKERS",
        title: "Parallel Execution (Worker-2 & Worker-3)",
        description: "Worker-2 leased 'transform_a' and Worker-3 leased 'transform_b'. Both computing concurrently.",
        effect: "Horizontal parallel throughput.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'transform_a', state: 'RUNNING', attempt: 1, lease_owner: 'worker-2' },
          { node_id: 'transform_b', state: 'RUNNING', attempt: 1, lease_owner: 'worker-3' },
          { node_id: 'report', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'tasks.parallel', payload: { workers: ['worker-2', 'worker-3'] } }
      });

      steps.push({
        stepNum: 5,
        component: "FENCED COMMIT",
        title: "Transforms Finished ➔ Report Ready",
        description: "Both transforms succeeded. Scheduler resolved fan-in barrier and promoted 'report' to READY.",
        effect: "Fan-in barrier cleared.",
        taskRuns: [
          { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'transform_a', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-2' },
          { node_id: 'transform_b', state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3' },
          { node_id: 'report', state: 'READY', attempt: 1, lease_owner: null }
        ],
        event: { event_type: 'task.ready', task_run_id: 'report' }
      });
    }

    // Final Report Step
    steps.push({
      stepNum: steps.length + 1,
      component: "LIFECYCLE COMPLETE",
      title: "Workflow Terminal Completion (SUCCEEDED)",
      description: `${fallbackAlive} executed 'report' summary task. All 4 DAG tasks reached SUCCEEDED!`,
      effect: "100% DAG resilience demonstrated.",
      taskRuns: [
        { node_id: 'extract', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive },
        { node_id: 'transform_a', state: 'SUCCEEDED', attempt: deadWorkerIds.size > 0 ? 2 : 1, lease_owner: fallbackAlive },
        { node_id: 'transform_b', state: 'SUCCEEDED', attempt: 1, lease_owner: secondAlive },
        { node_id: 'report', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive, output_ref: 's3://reports/summary.pdf' }
      ],
      event: { event_type: 'workflow.succeeded', payload: { status: 'SUCCEEDED' } }
    });

    return steps;
  }

  // 2. ZOMBIE WORKER FENCING PIPELINE (3 Nodes)
  if (workflowType === 'zombie') {
    const initTasks = [
      { node_id: 'auth_check', state: 'READY', attempt: 1, lease_owner: null },
      { node_id: 'long_compute', state: 'BLOCKED', attempt: 0, lease_owner: null },
      { node_id: 'emit_metrics', state: 'BLOCKED', attempt: 0, lease_owner: null }
    ];

    if (isAllDead) return [getClusterHaltedStep(initTasks)];

    const zombieWorker = deadWorkerIds.has('worker-2') ? 'worker-2' : (deadWorkerIds.size > 0 ? Array.from(deadWorkerIds)[0] : 'worker-2');

    return [
      {
        stepNum: 1,
        component: "DAG VALIDATION",
        title: "Zombie Fencing Pipeline Initialized",
        description: "Evaluated in-degree: 'auth_check' (in-degree 0, READY). 'long_compute' and 'emit_metrics' staged as BLOCKED.",
        effect: "PostgreSQL transaction committed 3 tasks.",
        taskRuns: initTasks,
        event: { event_type: 'workflow.started' }
      },
      {
        stepNum: 2,
        component: "TASK EXECUTION",
        title: `Auth Check Succeeded on ${fallbackAlive}`,
        description: `${fallbackAlive} executed 'auth_check' and committed SUCCEEDED. Scheduler promoted 'long_compute' to READY.`,
        effect: "'long_compute' queued in Redis Stream 'stream:tasks'.",
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'long_compute', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'emit_metrics', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.succeeded', task_run_id: 'auth_check' }
      },
      {
        stepNum: 3,
        component: "LEASE ACQUISITION",
        title: `${zombieWorker} Leases Long Compute (Token: token-101)`,
        description: `${zombieWorker} acquired lease for 'long_compute' in PostgreSQL. Granted lease_token: 'token-101' with a 15-second expiration timer.`,
        effect: `Task state: RUNNING (${zombieWorker}, token-101).`,
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'long_compute', state: 'RUNNING', attempt: 1, lease_owner: zombieWorker },
          { node_id: 'emit_metrics', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.leased', payload: { token: 'token-101', worker: zombieWorker } }
      },
      {
        stepNum: 4,
        component: "ZOMBIE HANG / GC",
        title: `🧟 ${zombieWorker} Freezes (Long GC Pause / Network Partition)`,
        description: `${zombieWorker} experienced a 20-second Garbage Collection freeze. Heartbeat renewals stopped. Lease expired in PostgreSQL!`,
        effect: `PostgreSQL: lease_expires_at < NOW(). ${zombieWorker} is now a Zombie Worker.`,
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'long_compute', state: 'LEASED', attempt: 1, lease_owner: zombieWorker, is_expired: true },
          { node_id: 'emit_metrics', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'lease.expired', payload: { expired_token: 'token-101', worker: zombieWorker } }
      },
      {
        stepNum: 5,
        component: "SCHEDULER RECLAIM",
        title: "Go Scheduler Reclaimed Task to READY",
        description: "Scheduler detected expired lease. Reset 'long_compute' to READY and cleared lease token. Incremented attempt to 2.",
        effect: "Task re-queued for healthy workers in Redis.",
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'long_compute', state: 'READY', attempt: 2, lease_owner: null, is_recovered: true },
          { node_id: 'emit_metrics', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.reclaimed', payload: { attempt: 2 } }
      },
      {
        stepNum: 6,
        component: "NEW FENCING TOKEN",
        title: `Healthy ${fallbackAlive} Leases Task with NEW Token (token-202)`,
        description: `Healthy ${fallbackAlive} leased 'long_compute' and was issued NEW fencing token: 'token-202'. ${fallbackAlive} executed and committed output!`,
        effect: "State is now SUCCEEDED with valid token-202. 'emit_metrics' unlocked to READY.",
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'long_compute', state: 'SUCCEEDED', attempt: 2, lease_owner: fallbackAlive, output_ref: '{"ml_loss": 0.04}' },
          { node_id: 'emit_metrics', state: 'READY', attempt: 1, lease_owner: null }
        ],
        event: { event_type: 'task.succeeded', payload: { token: 'token-202', worker: fallbackAlive } }
      },
      {
        stepNum: 7,
        component: "FENCING REJECTION",
        title: `🛑 Zombie ${zombieWorker} Wakes Up ➔ Commit Rejected!`,
        description: `${zombieWorker} woke up from GC freeze and attempted late commit with stale 'token-101'. PostgreSQL query: 'WHERE id = $1 AND lease_token = token-101' matched 0 rows! Commit REJECTED with ErrStaleLeaseCommit.`,
        effect: "Core Invariant: Fencing tokens strictly guarantee zombie workers cannot overwrite newer state.",
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'long_compute', state: 'SUCCEEDED', attempt: 2, lease_owner: fallbackAlive },
          { node_id: 'emit_metrics', state: 'READY', attempt: 1, lease_owner: null }
        ],
        event: { event_type: 'lease.fencing_rejected', payload: { stale_token: 'token-101', active_token: 'token-202', rows_affected: 0 } }
      },
      {
        stepNum: 8,
        component: "LIFECYCLE COMPLETE",
        title: "Downstream Metrics Emitted ➔ Pipeline Complete",
        description: `${secondAlive} executed final 'emit_metrics' task. All 3 nodes in DAG reached SUCCEEDED without any stale data corruption.`,
        effect: "Workflow SUCCEEDED. Complete data consistency preserved.",
        taskRuns: [
          { node_id: 'auth_check', state: 'SUCCEEDED', attempt: 1, lease_owner: fallbackAlive },
          { node_id: 'long_compute', state: 'SUCCEEDED', attempt: 2, lease_owner: fallbackAlive },
          { node_id: 'emit_metrics', state: 'SUCCEEDED', attempt: 1, lease_owner: secondAlive, output_ref: '{"metrics_sent": true}' }
        ],
        event: { event_type: 'workflow.succeeded' }
      }
    ];
  }

  // 3. DUPLICATE DELIVERY (Payment Processing - 3 Nodes)
  if (workflowType === 'duplicate') {
    const initTasks = [
      { node_id: 'fetch_order', state: 'READY', attempt: 1, lease_owner: null },
      { node_id: 'process_payment', state: 'BLOCKED', attempt: 0, lease_owner: null },
      { node_id: 'send_receipt', state: 'BLOCKED', attempt: 0, lease_owner: null }
    ];

    if (isAllDead) return [getClusterHaltedStep(initTasks)];

    const workerA = fallbackAlive;
    const workerB = deadWorkerIds.size > 0 ? (deadWorkerIds.has('worker-2') ? 'worker-2' : Array.from(deadWorkerIds)[0]) : 'worker-2';

    return [
      {
        stepNum: 1,
        component: "DAG VALIDATION",
        title: "Payment Workflow Submitted",
        description: "Graph loaded with 3 financial tasks: 'fetch_order' (READY) ➔ 'process_payment' (BLOCKED) ➔ 'send_receipt' (BLOCKED).",
        effect: "Financial pipeline staged in PostgreSQL.",
        taskRuns: initTasks,
        event: { event_type: 'workflow.started' }
      },
      {
        stepNum: 2,
        component: "TASK EXECUTION",
        title: `Order Fetched by ${workerA}`,
        description: `${workerA} fetched order details. Scheduler promoted critical 'process_payment' task to READY in Redis Stream 'stream:tasks'.`,
        effect: "'process_payment' eligible for consumer group delivery.",
        taskRuns: [
          { node_id: 'fetch_order', state: 'SUCCEEDED', attempt: 1, lease_owner: workerA },
          { node_id: 'process_payment', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'send_receipt', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.ready', task_run_id: 'process_payment' }
      },
      {
        stepNum: 3,
        component: "REDIS STREAM",
        title: "Duplicate Stream Delivery Injected",
        description: `Due to network retries, the same 'process_payment' message was delivered concurrently to ${workerA} and ${workerB}.`,
        effect: "Both workers concurrently attempt conditional SQL lease update.",
        taskRuns: [
          { node_id: 'fetch_order', state: 'SUCCEEDED', attempt: 1, lease_owner: workerA },
          { node_id: 'process_payment', state: 'READY', attempt: 1, lease_owner: null },
          { node_id: 'send_receipt', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'stream.duplicate_delivery', payload: { task_id: 'process_payment' } }
      },
      {
        stepNum: 4,
        component: "CONDITIONAL SQL",
        title: `${workerA} Wins Atomic Lease Update`,
        description: `${workerA} executed 'UPDATE task_runs SET state = LEASED WHERE id = $1 AND state = READY'. Result: 1 row affected (WINNER).`,
        effect: `${workerA} acquires ownership with token-991.`,
        taskRuns: [
          { node_id: 'fetch_order', state: 'SUCCEEDED', attempt: 1, lease_owner: workerA },
          { node_id: 'process_payment', state: 'RUNNING', attempt: 1, lease_owner: workerA },
          { node_id: 'send_receipt', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'lease.acquired', payload: { winner: workerA } }
      },
      {
        stepNum: 5,
        component: "IDEMPOTENT REJECT",
        title: `${workerB} Lease Safely Skipped (0 Rows Affected)`,
        description: `${workerB} executed identical SQL update. Since state is already LEASED, result was 0 rows affected. ${workerB} safely ACKs and discards duplicate message.`,
        effect: "Core Invariant: Zero duplicate credit card charge! Exactly-once execution guaranteed.",
        taskRuns: [
          { node_id: 'fetch_order', state: 'SUCCEEDED', attempt: 1, lease_owner: workerA },
          { node_id: 'process_payment', state: 'RUNNING', attempt: 1, lease_owner: workerA },
          { node_id: 'send_receipt', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'duplicate.discarded', payload: { worker: workerB, rows_affected: 0 } }
      },
      {
        stepNum: 6,
        component: "FENCED COMMIT",
        title: "Payment Processed ➔ Receipt Sent (100% SUCCEEDED)",
        description: `${workerA} charged payment and committed output. Downstream 'send_receipt' executed by ${secondAlive}. Entire payment pipeline SUCCEEDED cleanly.`,
        effect: "Exactly-once execution semantics achieved with zero double billing.",
        taskRuns: [
          { node_id: 'fetch_order', state: 'SUCCEEDED', attempt: 1, lease_owner: workerA },
          { node_id: 'process_payment', state: 'SUCCEEDED', attempt: 1, lease_owner: workerA, output_ref: '{"charge_id": "ch_981a", "status": "paid"}' },
          { node_id: 'send_receipt', state: 'SUCCEEDED', attempt: 1, lease_owner: secondAlive, output_ref: '{"email_sent": true}' }
        ],
        event: { event_type: 'workflow.succeeded' }
      }
    ];
  }

  // 4. 8-TASK BENCHMARK (With Explicit Crash & Reclaim Lifecycle)
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
        title: "⚠️ All Workers Dead (Benchmark Stalled)",
        description: "Benchmark workflow cannot proceed because 3/3 workers are offline. Revive at least one worker to start processing shards.",
        effect: "Queue paused.",
        taskRuns: allBlocked({ ingest: { state: 'READY', attempt: 1 } }),
        event: { event_type: 'cluster.halted' }
      }];
    }

    const w1 = fallbackAlive;
    const w2 = secondAlive;
    const w3 = thirdAlive;

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
      const deadNames = Array.from(deadWorkerIds);
      const deadDesc = deadNames.join(' & ');

      // Step 3: Initial Leases Assigned across the fleet before the crash
      steps.push({
        stepNum: 3,
        component: "PARALLEL LEASES",
        title: `Initial Leases Distributed Across Fleet`,
        description: `Shards leased across workers: shard_1 to ${w1}, shard_2 to ${deadNames[0]}, shard_3 to ${deadNames[1] || 'worker-3'}. Shards 4, 5, 6 remain queued in READY.`,
        effect: "Workers computing parallel shards in Redis stream consumer group.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_1: { state: 'RUNNING', attempt: 1, lease_owner: w1 },
          shard_2: { state: 'RUNNING', attempt: 1, lease_owner: deadNames[0] },
          shard_3: { state: 'RUNNING', attempt: 1, lease_owner: deadNames[1] || (aliveWorkers.includes('worker-3') ? 'worker-3' : 'worker-2') },
          shard_4: { state: 'READY', attempt: 1 },
          shard_5: { state: 'READY', attempt: 1 },
          shard_6: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'cluster.busy' }
      });

      // Step 4: Worker Crash & Ghost Leases Expire
      steps.push({
        stepNum: 4,
        component: "WORKER CRASH",
        title: `💥 ${deadDesc} Crashed! (Ghost Leases Expired)`,
        description: `${deadDesc} stopped sending heartbeats to Redis. Leases on shard_2 ${deadNames.length > 1 ? 'and shard_3' : ''} expired in PostgreSQL! Tasks are orphaned.`,
        effect: "PostgreSQL: lease_expires_at < NOW(). Expired badges highlighted in red.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_2: { state: 'LEASED', attempt: 1, lease_owner: deadNames[0], is_expired: true },
          shard_3: deadNames.length > 1 
            ? { state: 'LEASED', attempt: 1, lease_owner: deadNames[1], is_expired: true }
            : { state: 'RUNNING', attempt: 1, lease_owner: w2 },
          shard_4: { state: 'READY', attempt: 1 },
          shard_5: { state: 'READY', attempt: 1 },
          shard_6: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'worker.crash', payload: { dead: deadNames } }
      });

      // Step 5: Scheduler Reclaims Orphaned Shards to READY (Attempt 2)
      steps.push({
        stepNum: 5,
        component: "SCHEDULER RECOVERY",
        title: `🛡️ Go Scheduler Reclaimed Orphaned Shards`,
        description: `Scheduler executed SQL sweep: 'UPDATE task_runs SET state = READY, lease_owner = NULL, attempt = attempt + 1 WHERE lease_expires_at < NOW()'. Orphaned shards recovered to READY (Attempt 2)!`,
        effect: "Orphaned shards re-enter Redis stream queue.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_2: { state: 'READY', attempt: 2, lease_owner: null, is_recovered: true },
          shard_3: deadNames.length > 1
            ? { state: 'READY', attempt: 2, lease_owner: null, is_recovered: true }
            : { state: 'SUCCEEDED', attempt: 1, lease_owner: w2 },
          shard_4: { state: 'READY', attempt: 1 },
          shard_5: { state: 'READY', attempt: 1 },
          shard_6: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'scheduler.recovery', payload: { recovered: ['shard_2', deadNames.length > 1 ? 'shard_3' : null].filter(Boolean) } }
      });

      // Step 6: Surviving Worker Re-Leases Reclaimed Shard 2
      steps.push({
        stepNum: 6,
        component: "SELF-HEALING EXECUTION",
        title: `🔄 Surviving ${w1} Re-Leases Reclaimed Shard 2`,
        description: `Surviving ${w1} picked up orphaned shard_2 with a brand new fencing token and began computation.`,
        effect: "New fencing token guarantees state isolation.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_2: { state: 'RUNNING', attempt: 2, lease_owner: w1 },
          shard_3: deadNames.length > 1 
            ? { state: 'READY', attempt: 2, lease_owner: null, is_recovered: true }
            : { state: 'SUCCEEDED', attempt: 1, lease_owner: w2 },
          shard_4: { state: 'READY', attempt: 1 },
          shard_5: { state: 'READY', attempt: 1 },
          shard_6: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'task.started', task_run_id: 'shard_2' }
      });

      // Step 7: Shard 2 Succeeded & Worker-1 Leases Reclaimed Shard 3
      if (deadNames.length > 1) {
        steps.push({
          stepNum: 7,
          component: "SELF-HEALING EXECUTION",
          title: `Shard 2 Succeeded ➔ ${w1} Re-Leases Reclaimed Shard 3`,
          description: `${w1} committed shard_2 (Attempt 2) and pulled orphaned shard_3 from the Redis stream queue.`,
          effect: "Sequential draining of reclaimed tasks by surviving worker.",
          taskRuns: allBlocked({
            ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
            shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
            shard_2: { state: 'SUCCEEDED', attempt: 2, lease_owner: w1 },
            shard_3: { state: 'RUNNING', attempt: 2, lease_owner: w1 },
            shard_4: { state: 'READY', attempt: 1 },
            shard_5: { state: 'READY', attempt: 1 },
            shard_6: { state: 'READY', attempt: 1 }
          }),
          event: { event_type: 'task.started', task_run_id: 'shard_3' }
        });
      }

      // Step 8: Shard 3 Succeeded & Worker-1 Leases Shard 4
      steps.push({
        stepNum: steps.length + 1,
        component: "QUEUE DRAINAGE",
        title: `Shards (1, 2, 3) Complete ➔ ${w1} Leases Shard 4`,
        description: `All reclaimed shards finished. ${w1} pulls the next queued task (shard_4) from the Redis stream.`,
        effect: "Sequential processing of pending queue.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_2: { state: 'SUCCEEDED', attempt: 2, lease_owner: w1 },
          shard_3: { state: 'SUCCEEDED', attempt: deadNames.length > 1 ? 2 : 1, lease_owner: deadNames.length > 1 ? w1 : w2 },
          shard_4: { state: 'RUNNING', attempt: 1, lease_owner: w1 },
          shard_5: { state: 'READY', attempt: 1 },
          shard_6: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'task.started', task_run_id: 'shard_4' }
      });

      // Step 9: Shard 4 Succeeded & Worker-1 Leases Shard 5
      steps.push({
        stepNum: steps.length + 1,
        component: "QUEUE DRAINAGE",
        title: `Shard 4 Succeeded ➔ ${w1} Leases Shard 5`,
        description: `${w1} committed shard_4 and leased shard_5 from the stream.`,
        effect: "Ongoing queue consumption.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_2: { state: 'SUCCEEDED', attempt: 2, lease_owner: w1 },
          shard_3: { state: 'SUCCEEDED', attempt: deadNames.length > 1 ? 2 : 1, lease_owner: deadNames.length > 1 ? w1 : w2 },
          shard_4: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_5: { state: 'RUNNING', attempt: 1, lease_owner: w1 },
          shard_6: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'task.started', task_run_id: 'shard_5' }
      });

      // Step 10: Shard 5 Succeeded & Worker-1 Leases Final Shard 6
      steps.push({
        stepNum: steps.length + 1,
        component: "QUEUE DRAINAGE",
        title: `Shard 5 Succeeded ➔ ${w1} Leases Final Shard 6`,
        description: `${w1} committed shard_5 and leased final compute shard_6.`,
        effect: "Last compute shard executing.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_2: { state: 'SUCCEEDED', attempt: 2, lease_owner: w1 },
          shard_3: { state: 'SUCCEEDED', attempt: deadNames.length > 1 ? 2 : 1, lease_owner: deadNames.length > 1 ? w1 : w2 },
          shard_4: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_5: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_6: { state: 'RUNNING', attempt: 1, lease_owner: w1 }
        }),
        event: { event_type: 'task.started', task_run_id: 'shard_6' }
      });

      // Step 11: All 6 Shards Succeeded ➔ Aggregate Promoted to READY
      steps.push({
        stepNum: steps.length + 1,
        component: "PIPELINE DRAINAGE",
        title: "All 6 Shards Completed ➔ Aggregate Unlocked",
        description: `Surviving ${w1} successfully drained all 6 compute shards. Scheduler evaluated 6/6 dependencies met ➔ 'aggregate' promoted to READY!`,
        effect: "6-way fan-in barrier cleared.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_2: { state: 'SUCCEEDED', attempt: 2, lease_owner: w1 },
          shard_3: { state: 'SUCCEEDED', attempt: deadNames.length > 1 ? 2 : 1, lease_owner: deadNames.length > 1 ? w1 : w2 },
          shard_4: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_5: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_6: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          aggregate: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'barrier.cleared', task_run_id: 'aggregate' }
      });
    } else {
      steps.push({
        stepNum: 3,
        component: "CLUSTER SATURATION",
        title: "Full 3-Worker Parallel Saturation",
        description: "Worker-1, Worker-2, and Worker-3 computing parallel shards simultaneously.",
        effect: "Maximum cluster throughput.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_1: { state: 'RUNNING', attempt: 1, lease_owner: 'worker-1' },
          shard_2: { state: 'RUNNING', attempt: 1, lease_owner: 'worker-2' },
          shard_3: { state: 'RUNNING', attempt: 1, lease_owner: 'worker-3' },
          shard_4: { state: 'READY', attempt: 1 },
          shard_5: { state: 'READY', attempt: 1 },
          shard_6: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'cluster.busy' }
      });

      steps.push({
        stepNum: 4,
        component: "PIPELINE DRAINAGE",
        title: "All Shards Completed Successfully",
        description: "All 3 workers finished all 6 compute shards. Scheduler evaluated 6/6 dependencies met ➔ 'aggregate' promoted to READY.",
        effect: "6-way fan-in barrier cleared.",
        taskRuns: allBlocked({
          ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          shard_2: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-2' },
          shard_3: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3' },
          shard_4: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-1' },
          shard_5: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-2' },
          shard_6: { state: 'SUCCEEDED', attempt: 1, lease_owner: 'worker-3' },
          aggregate: { state: 'READY', attempt: 1 }
        }),
        event: { event_type: 'barrier.cleared', task_run_id: 'aggregate' }
      });
    }

    // Final Aggregate Step
    steps.push({
      stepNum: steps.length + 1,
      component: "LIFECYCLE COMPLETE",
      title: "Benchmark SUCCEEDED (8/8 Tasks Complete)",
      description: `${w1} executed aggregate task and committed final dataset. Total tasks: 8, Failures: 0.`,
      effect: "Fault-tolerant benchmark complete.",
      taskRuns: allBlocked({
        ingest: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
        shard_1: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
        shard_2: { state: 'SUCCEEDED', attempt: deadWorkerIds.has('worker-2') ? 2 : 1, lease_owner: deadWorkerIds.has('worker-2') ? w1 : w2 },
        shard_3: { state: 'SUCCEEDED', attempt: deadWorkerIds.has('worker-3') ? 2 : 1, lease_owner: deadWorkerIds.has('worker-3') ? w1 : w3 },
        shard_4: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
        shard_5: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
        shard_6: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
        aggregate: { state: 'SUCCEEDED', attempt: 1, lease_owner: w1, output_ref: '{"processed_records": 60000}' }
      }),
      event: { event_type: 'workflow.succeeded', payload: { throughput: '14.2 tasks/sec' } }
    });

    return steps;
  }

  // 5. POISON PILL
  if (workflowType === 'poison') {
    const initTasks = [
      { node_id: 'fetch', state: 'READY', attempt: 1, lease_owner: null },
      { node_id: 'poison_task', state: 'BLOCKED', attempt: 0, lease_owner: null },
      { node_id: 'sink', state: 'BLOCKED', attempt: 0, lease_owner: null }
    ];

    if (isAllDead) return [getClusterHaltedStep(initTasks)];

    const w1 = fallbackAlive;
    const w2 = secondAlive;

    const steps = [
      {
        stepNum: 1,
        component: "DAG VALIDATION",
        title: "Poison Pill Workflow Initialized",
        description: "Graph loaded with 3 tasks: 'fetch' (in-degree 0), 'poison_task' (max_retries = 2), and downstream 'sink'.",
        effect: "Testing deterministic crash isolation and Dead-Letter Queue quarantining.",
        taskRuns: initTasks,
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
      }
    ];

    if (deadWorkerIds.size > 0) {
      const deadName = Array.from(deadWorkerIds)[0];
      steps.push({
        stepNum: 3,
        component: "WORKER CRASH & RECLAIM",
        title: `💥 ${deadName} Crashed During Poison Task ➔ Scheduler Reclaimed!`,
        description: `${deadName} died mid-execution. Scheduler detected expired lease and reclaimed 'poison_task' to READY (Attempt 2).`,
        effect: "Lease reclaimed without dropping task.",
        taskRuns: [
          { node_id: 'fetch', state: 'SUCCEEDED', attempt: 1, lease_owner: w1 },
          { node_id: 'poison_task', state: 'READY', attempt: 2, lease_owner: null, is_recovered: true },
          { node_id: 'sink', state: 'BLOCKED', attempt: 0, lease_owner: null }
        ],
        event: { event_type: 'task.reclaimed', payload: { worker: deadName, attempt: 2 } }
      });
    } else {
      steps.push({
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
      });

      steps.push({
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
      });
    }

    steps.push({
      stepNum: steps.length + 1,
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
    });

    steps.push({
      stepNum: steps.length + 1,
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
    });

    return steps;
  }

  return [];
}
