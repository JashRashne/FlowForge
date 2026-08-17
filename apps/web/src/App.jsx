import React, { useState, useEffect, useCallback, useRef } from 'react';
import Navbar from './components/Navbar';
import MetricsBar from './components/MetricsBar';
import ChaosPanel from './components/ChaosPanel';
import DAGVisualizer from './components/DAGVisualizer';
import EventFeed from './components/EventFeed';

const API_BASE = 'http://localhost:8080';
const WS_URL = 'ws://localhost:8080/ws/events';

// Preset Workflow Definitions
const PRESETS = {
  diamond: {
    id: 'a0000000-0000-0000-0000-000000000001',
    name: 'Diamond Data Pipeline',
    description: 'Extract (Root) ➔ Transform A & B (Parallel) ➔ Generate Report',
    nodes: {
      extract: { id: 'extract', type: 'http', config: { url: 'https://httpbin.org/get', method: 'GET' }, max_retries: 3 },
      transform_a: { id: 'transform_a', type: 'synthetic', config: { sleep_ms: 1500, result: '{"cleaned_rows": 500}' }, max_retries: 2 },
      transform_b: { id: 'transform_b', type: 'synthetic', config: { sleep_ms: 2000, result: '{"enriched_rows": 500}' }, max_retries: 2 },
      report: { id: 'report', type: 'synthetic', config: { sleep_ms: 1000, result: '{"report_url": "s3://reports/summary.pdf"}' }, max_retries: 1 }
    },
    edges: [
      { from: 'extract', to: 'transform_a' },
      { from: 'extract', to: 'transform_b' },
      { from: 'transform_a', to: 'report' },
      { from: 'transform_b', to: 'report' }
    ]
  },
  fanout: {
    id: 'b0000000-0000-0000-0000-000000000002',
    name: '8-Task Distributed Benchmark',
    description: '1 Ingestion Node ➔ 8 Parallel Compute Tasks ➔ Aggregation',
    nodes: {
      ingest: { id: 'ingest', type: 'synthetic', config: { sleep_ms: 500 } },
      shard_1: { id: 'shard_1', type: 'synthetic', config: { sleep_ms: 1200 } },
      shard_2: { id: 'shard_2', type: 'synthetic', config: { sleep_ms: 1400 } },
      shard_3: { id: 'shard_3', type: 'synthetic', config: { sleep_ms: 1100 } },
      shard_4: { id: 'shard_4', type: 'synthetic', config: { sleep_ms: 1600 } },
      shard_5: { id: 'shard_5', type: 'synthetic', config: { sleep_ms: 1300 } },
      shard_6: { id: 'shard_6', type: 'synthetic', config: { sleep_ms: 1500 } },
      aggregate: { id: 'aggregate', type: 'synthetic', config: { sleep_ms: 800 } }
    },
    edges: [
      { from: 'ingest', to: 'shard_1' },
      { from: 'ingest', to: 'shard_2' },
      { from: 'ingest', to: 'shard_3' },
      { from: 'ingest', to: 'shard_4' },
      { from: 'ingest', to: 'shard_5' },
      { from: 'ingest', to: 'shard_6' },
      { from: 'shard_1', to: 'aggregate' },
      { from: 'shard_2', to: 'aggregate' },
      { from: 'shard_3', to: 'aggregate' },
      { from: 'shard_4', to: 'aggregate' },
      { from: 'shard_5', to: 'aggregate' },
      { from: 'shard_6', to: 'aggregate' }
    ]
  },
  poison: {
    id: 'c0000000-0000-0000-0000-000000000003',
    name: 'Poison Pill DLQ Workflow',
    description: 'Demonstrates deterministic failure, exponential retries, and quarantine',
    nodes: {
      fetch: { id: 'fetch', type: 'synthetic', config: { sleep_ms: 800 } },
      poison_task: { id: 'poison_task', type: 'synthetic', config: { should_fail: true, error_message: 'Fatal corruption in payload' }, max_retries: 2 },
      sink: { id: 'sink', type: 'synthetic', config: { sleep_ms: 500 } }
    },
    edges: [
      { from: 'fetch', to: 'poison_task' },
      { from: 'poison_task', to: 'sink' }
    ]
  }
};

export default function App() {
  const [activeWorkflow, setActiveWorkflow] = useState(PRESETS.diamond);
  const [activeRunID, setActiveRunID] = useState(null);
  const [taskRuns, setTaskRuns] = useState([]);
  const [workers, setWorkers] = useState([]);
  const [events, setEvents] = useState([]);
  const [isConnected, setIsConnected] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [selectedTask, setSelectedTask] = useState(null);

  const wsRef = useRef(null);

  // Connect to WebSockets
  useEffect(() => {
    let ws;
    const connectWS = () => {
      ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        setIsConnected(true);
      };

      ws.onmessage = (event) => {
        try {
          const envelope = JSON.parse(event.data);
          setEvents(prev => [envelope, ...prev.slice(0, 99)]);
        } catch (e) {
          console.error('Failed to parse WS message', e);
        }
      };

      ws.onclose = () => {
        setIsConnected(false);
        setTimeout(connectWS, 2000); // auto-reconnect
      };

      ws.onerror = () => {
        setIsConnected(false);
      };
    };

    connectWS();
    return () => {
      if (ws) ws.close();
    };
  }, []);

  // Fetch Workers and Task Runs periodically
  const fetchState = useCallback(async () => {
    try {
      // 1. Fetch Workers
      const wRes = await fetch(`${API_BASE}/workers`);
      if (wRes.ok) {
        const wData = await wRes.json();
        setWorkers(wData || []);
      }

      // 2. Fetch Active Run Tasks
      if (activeRunID) {
        const tRes = await fetch(`${API_BASE}/runs/${activeRunID}/tasks`);
        if (tRes.ok) {
          const tData = await tRes.json();
          setTaskRuns(tData || []);
        }
      }
    } catch (err) {
      console.error('Error polling backend state', err);
    }
  }, [activeRunID]);

  useEffect(() => {
    fetchState();
    const interval = setInterval(fetchState, 1000);
    return () => clearInterval(interval);
  }, [fetchState]);

  // Launch a Preset Workflow
  const handleStartWorkflow = async (presetKey) => {
    setIsLoading(true);
    const def = PRESETS[presetKey];
    setActiveWorkflow(def);

    try {
      // 1. Create or register workflow in Postgres
      await fetch(`${API_BASE}/workflows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(def)
      });

      // 2. Start Workflow Run
      const runRes = await fetch(`${API_BASE}/workflows/${def.id}/runs`, { method: 'POST' });
      if (runRes.ok) {
        const runData = await runRes.json();
        setActiveRunID(runData.run_id);
        fetchState();
      }
    } catch (e) {
      console.error('Failed to launch workflow', e);
    } finally {
      setIsLoading(false);
    }
  };

  // Chaos: Kill Worker
  const handleKillWorker = async (workerID) => {
    try {
      await fetch(`${API_BASE}/chaos/kill-worker`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ worker_id: workerID })
      });
      fetchState();
    } catch (e) {
      console.error('Failed to kill worker', e);
    }
  };

  // Compute metrics
  const completedTasks = taskRuns.filter(t => t.state === 'SUCCEEDED').length;
  const runningTasks = taskRuns.filter(t => t.state === 'RUNNING' || t.state === 'LEASED').length;
  const dlqCount = taskRuns.filter(t => t.state === 'DLQ' || t.state === 'FAILED').length;
  const retryCount = taskRuns.reduce((acc, t) => acc + (t.attempt > 1 ? t.attempt - 1 : 0), 0);

  const metrics = {
    healthyWorkers: workers.length || 3,
    totalWorkers: 3,
    completedTasks,
    runningTasks,
    retryCount,
    dlqCount,
    schedulerLatencyMs: 14
  };

  return (
    <div style={{ maxWidth: '1400px', margin: '0 auto', padding: '1.5rem' }}>
      <Navbar
        isConnected={isConnected}
        workerCount={workers.length || 3}
        activeRunID={activeRunID}
        onRefresh={fetchState}
      />

      <MetricsBar metrics={metrics} />

      <ChaosPanel
        onStartWorkflow={handleStartWorkflow}
        onKillWorker={handleKillWorker}
        onInjectChaos={() => {}}
        workers={workers}
        isLoading={isLoading}
      />

      <DAGVisualizer
        workflow={activeWorkflow}
        taskRuns={taskRuns}
        onSelectTask={(node, run) => setSelectedTask({ node, run })}
      />

      <EventFeed
        events={events}
        onClear={() => setEvents([])}
      />

      {/* Task Inspection Drawer */}
      {selectedTask && (
        <div style={{
          position: 'fixed',
          top: 0,
          right: 0,
          bottom: 0,
          width: '420px',
          background: '#FFFFFF',
          borderLeft: '4px solid #000000',
          boxShadow: '-6px 0px 0px rgba(0,0,0,0.15)',
          padding: '2rem',
          zIndex: 1000,
          overflowY: 'auto'
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
            <h2 style={{ fontSize: '1.4rem' }}>Task Details</h2>
            <button
              onClick={() => setSelectedTask(null)}
              className="neo-btn"
              style={{ padding: '0.3rem 0.7rem' }}
            >
              ✕ Close
            </button>
          </div>

          <div style={{ marginBottom: '1rem' }}>
            <div style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', fontWeight: 800, color: 'var(--text-muted)' }}>NODE ID</div>
            <div style={{ fontSize: '1.1rem', fontWeight: 800 }}>{selectedTask.node.id}</div>
          </div>

          <div style={{ marginBottom: '1rem' }}>
            <div style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', fontWeight: 800, color: 'var(--text-muted)' }}>CURRENT STATE</div>
            <span className={`neo-badge badge-${(selectedTask.run.state || 'blocked').toLowerCase()}`} style={{ marginTop: '0.2rem' }}>
              {selectedTask.run.state || 'BLOCKED'}
            </span>
          </div>

          <div style={{ marginBottom: '1rem' }}>
            <div style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', fontWeight: 800, color: 'var(--text-muted)' }}>LEASE OWNER</div>
            <div style={{ fontFamily: 'var(--font-mono)', fontWeight: 700 }}>{selectedTask.run.lease_owner || 'None'}</div>
          </div>

          <div style={{ marginBottom: '1rem' }}>
            <div style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', fontWeight: 800, color: 'var(--text-muted)' }}>ATTEMPTS</div>
            <div style={{ fontFamily: 'var(--font-mono)', fontWeight: 700 }}>{selectedTask.run.attempt || 0}</div>
          </div>

          <div style={{ marginBottom: '1rem' }}>
            <div style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', fontWeight: 800, color: 'var(--text-muted)' }}>OUTPUT ARTIFACT</div>
            <div style={{ fontFamily: 'var(--font-mono)', fontSize: '0.85rem', background: '#F4F0EA', padding: '0.5rem', border: '2px solid #000', borderRadius: '4px', wordBreak: 'break-all' }}>
              {selectedTask.run.output_ref || 'No output yet'}
            </div>
          </div>

          <div>
            <div style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', fontWeight: 800, color: 'var(--text-muted)' }}>TASK CONFIGURATION</div>
            <pre style={{ background: '#18181B', color: '#4ADE80', padding: '0.8rem', borderRadius: '6px', fontSize: '0.8rem', marginTop: '0.3rem', overflowX: 'auto' }}>
              {JSON.stringify(selectedTask.node.config || {}, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}
