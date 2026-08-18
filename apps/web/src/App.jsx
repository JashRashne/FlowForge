import React, { useState, useEffect, useCallback, useRef } from 'react';
import LandingPage from './components/LandingPage';
import NotFound from './components/NotFound';
import DAGVisualizer from './components/DAGVisualizer';
import StepNarrativeBar from './components/StepNarrativeBar';
import { generateDynamicSimulationSteps } from './utils/dynamicSimulation';
import { Play, Pause, SkipForward, RotateCcw, Flame, Skull, Cpu, Zap, Activity, Globe, Database, Terminal, ChevronDown, ChevronUp, X, Sparkles, Server, Copy, ShieldAlert, CheckCircle2, Home, ArrowLeft } from 'lucide-react';

const API_BASE = 'http://localhost:8080';
const WS_URL = 'ws://localhost:8080/ws/events';

// Preset Workflow Definitions
const PRESETS = {
  diamond: {
    id: 'a0000000-0000-0000-0000-000000000001',
    name: 'Diamond Data Pipeline',
    description: '1 Ingest Node ➔ 2 Parallel Transform Tasks ➔ 1 Report Aggregation',
    nodes: {
      extract: { id: 'extract', type: 'synthetic', config: { sleep_ms: 600, result: '{"extracted_records": 2500}' }, max_retries: 3 },
      transform_a: { id: 'transform_a', type: 'python', config: { script: 'normalize.py', sleep_ms: 800 }, max_retries: 2 },
      transform_b: { id: 'transform_b', type: 'synthetic', config: { sleep_ms: 1000 }, max_retries: 2 },
      report: { id: 'report', type: 'synthetic', config: { sleep_ms: 600, result: '{"report_generated": true}' }, max_retries: 1 }
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
    description: '1 Ingest Node ➔ 6 Parallel Worker Compute Nodes ➔ Aggregation',
    nodes: {
      ingest: { id: 'ingest', type: 'synthetic', config: { sleep_ms: 400 } },
      shard_1: { id: 'shard_1', type: 'synthetic', config: { sleep_ms: 800 } },
      shard_2: { id: 'shard_2', type: 'synthetic', config: { sleep_ms: 900 } },
      shard_3: { id: 'shard_3', type: 'synthetic', config: { sleep_ms: 750 } },
      shard_4: { id: 'shard_4', type: 'synthetic', config: { sleep_ms: 1000 } },
      shard_5: { id: 'shard_5', type: 'synthetic', config: { sleep_ms: 850 } },
      shard_6: { id: 'shard_6', type: 'synthetic', config: { sleep_ms: 950 } },
      aggregate: { id: 'aggregate', type: 'synthetic', config: { sleep_ms: 600 } }
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
      fetch: { id: 'fetch', type: 'synthetic', config: { sleep_ms: 500 } },
      poison_task: { id: 'poison_task', type: 'synthetic', config: { should_fail: true, error_message: 'Corrupted payload' }, max_retries: 2 },
      sink: { id: 'sink', type: 'synthetic', config: { sleep_ms: 500 } }
    },
    edges: [
      { from: 'fetch', to: 'poison_task' },
      { from: 'poison_task', to: 'sink' }
    ]
  },
  zombie: {
    id: 'd0000000-0000-0000-0000-000000000004',
    name: 'Zombie Worker Fencing Pipeline',
    description: 'Auth Check ➔ Long Compute (GC Freeze / Stale Commit Rejection) ➔ Emit Metrics',
    nodes: {
      auth_check: { id: 'auth_check', type: 'synthetic', config: { sleep_ms: 400, result: '{"authorized": true}' }, max_retries: 2 },
      long_compute: { id: 'long_compute', type: 'python', config: { script: 'ml_inference.py', sleep_ms: 2000 }, max_retries: 2 },
      emit_metrics: { id: 'emit_metrics', type: 'synthetic', config: { sleep_ms: 500 }, max_retries: 1 }
    },
    edges: [
      { from: 'auth_check', to: 'long_compute' },
      { from: 'long_compute', to: 'emit_metrics' }
    ]
  },
  duplicate: {
    id: 'e0000000-0000-0000-0000-000000000005',
    name: 'Payment Processing (Idempotent Deduplication)',
    description: 'Fetch Order ➔ Charge Payment (Duplicate Stream Delivery Protected) ➔ Send Receipt',
    nodes: {
      fetch_order: { id: 'fetch_order', type: 'synthetic', config: { sleep_ms: 400, result: '{"order_id": "ORD-9921"}' }, max_retries: 2 },
      process_payment: { id: 'process_payment', type: 'synthetic', config: { amount: 500, sleep_ms: 1000 }, max_retries: 1 },
      send_receipt: { id: 'send_receipt', type: 'synthetic', config: { email: 'user@example.com', sleep_ms: 500 }, max_retries: 1 }
    },
    edges: [
      { from: 'fetch_order', to: 'process_payment' },
      { from: 'process_payment', to: 'send_receipt' }
    ]
  }
};

export default function App() {
  const resolveRoute = useCallback(() => {
    if (typeof window === 'undefined') return 'landing';
    const path = window.location.pathname.replace(/\/+$/, '') || '/';
    const hash = window.location.hash.replace(/\/+$/, '') || '';
    if (path === '/simulator' || hash === '#/simulator' || hash === '#simulator') {
      return 'simulator';
    }
    if (path === '/' && (hash === '' || hash === '#/' || hash === '#')) {
      return 'landing';
    }
    return '404';
  }, []);

  const [currentView, setCurrentView] = useState(() => {
    if (typeof window !== 'undefined') {
      const path = window.location.pathname.replace(/\/+$/, '') || '/';
      const hash = window.location.hash.replace(/\/+$/, '') || '';
      if (path === '/simulator' || hash === '#/simulator' || hash === '#simulator') {
        return 'simulator';
      }
      if (path === '/' && (hash === '' || hash === '#/' || hash === '#')) {
        return 'landing';
      }
      return '404';
    }
    return 'landing';
  });

  const [activePreset, setActivePreset] = useState('diamond');
  const [activeWorkflow, setActiveWorkflow] = useState(PRESETS.diamond);
  const [activeRunID, setActiveRunID] = useState(null);
  const [taskRuns, setTaskRuns] = useState([]);
  const [workers, setWorkers] = useState([
    { id: 'worker-1', status: 'healthy' },
    { id: 'worker-2', status: 'healthy' },
    { id: 'worker-3', status: 'healthy' }
  ]);
  const [events, setEvents] = useState([]);
  const [selectedTask, setSelectedTask] = useState(null);
  const [showLogs, setShowLogs] = useState(false);
  const [isBackendConnected, setIsBackendConnected] = useState(false);

  // Simulation State
  const [isSimulationMode, setIsSimulationMode] = useState(true);
  const [simulationSteps, setSimulationSteps] = useState([]);
  const [currentStepIndex, setCurrentStepIndex] = useState(0);
  const [isPlaying, setIsPlaying] = useState(false);
  const [simulationSpeed, setSimulationSpeed] = useState(1200);

  // Sync document title with current route (max 2 words)
  useEffect(() => {
    if (currentView === 'simulator') {
      document.title = 'FlowForge Simulator';
    } else if (currentView === 'landing') {
      document.title = 'FlowForge';
    } else if (currentView === '404') {
      document.title = 'Not Found';
    }
  }, [currentView]);

  // Check Go backend connectivity when in Real Cluster mode
  useEffect(() => {
    if (!isSimulationMode) {
      fetch(`${API_BASE}/health`)
        .then(res => setIsBackendConnected(res.ok))
        .catch(() => setIsBackendConnected(false));
    }
  }, [isSimulationMode]);

  // Real Backend WebSocket Event Subscriber
  useEffect(() => {
    let ws;
    let reconnectTimer;

    const connectWS = () => {
      try {
        ws = new WebSocket(WS_URL);
        ws.onopen = () => {
          setIsBackendConnected(true);
        };
        ws.onmessage = (msg) => {
          try {
            const data = JSON.parse(msg.data);
            setEvents(prev => [data, ...prev.slice(0, 49)]);
          } catch (e) {
            console.error('Failed to parse WebSocket message', e);
          }
        };
        ws.onclose = () => {
          reconnectTimer = setTimeout(connectWS, 2500);
        };
        ws.onerror = () => {
          ws.close();
        };
      } catch (e) {
        reconnectTimer = setTimeout(connectWS, 2500);
      }
    };

    connectWS();

    return () => {
      if (ws) ws.close();
      if (reconnectTimer) clearTimeout(reconnectTimer);
    };
  }, []);

  // Sync view state with browser back/forward and hash changes
  useEffect(() => {
    const handleNav = () => {
      setCurrentView(resolveRoute());
    };
    handleNav();
    window.addEventListener('popstate', handleNav);
    window.addEventListener('hashchange', handleNav);
    return () => {
      window.removeEventListener('popstate', handleNav);
      window.removeEventListener('hashchange', handleNav);
    };
  }, [resolveRoute]);

  const navigateToHome = () => {
    window.history.pushState({}, '', '/');
    setCurrentView('landing');
  };

  const navigateToSimulator = () => {
    window.history.pushState({}, '', '/simulator');
    setCurrentView('simulator');
  };

  // Initialize / dynamically regenerate simulation steps on preset or worker health changes
  useEffect(() => {
    const steps = generateDynamicSimulationSteps(activePreset, workers);
    setSimulationSteps(steps);
    setCurrentStepIndex(prev => {
      const validIdx = Math.min(prev, Math.max(0, steps.length - 1));
      if (steps[validIdx]) {
        setTaskRuns(steps[validIdx].taskRuns);
      }
      return validIdx;
    });
  }, [activePreset, workers]);

  // Simulation auto-play loop
  useEffect(() => {
    let timer;
    if (isSimulationMode && isPlaying) {
      timer = setTimeout(() => {
        if (currentStepIndex < simulationSteps.length - 1) {
          const nextIdx = currentStepIndex + 1;
          setCurrentStepIndex(nextIdx);
          const nextStep = simulationSteps[nextIdx];
          setTaskRuns(nextStep.taskRuns);
          if (nextStep.event) {
            setEvents(prev => [nextStep.event, ...prev.slice(0, 49)]);
          }
        } else {
          setIsPlaying(false);
        }
      }, simulationSpeed);
    }
    return () => clearTimeout(timer);
  }, [isSimulationMode, isPlaying, currentStepIndex, simulationSteps, simulationSpeed]);

  const handleStepNext = () => {
    if (currentStepIndex < simulationSteps.length - 1) {
      const nextIdx = currentStepIndex + 1;
      setCurrentStepIndex(nextIdx);
      const nextStep = simulationSteps[nextIdx];
      setTaskRuns(nextStep.taskRuns);
      if (nextStep.event) {
        setEvents(prev => [nextStep.event, ...prev.slice(0, 49)]);
      }
    }
  };

  const handleStepPrev = () => {
    if (currentStepIndex > 0) {
      const prevIdx = currentStepIndex - 1;
      setCurrentStepIndex(prevIdx);
      const prevStep = simulationSteps[prevIdx];
      setTaskRuns(prevStep.taskRuns);
    }
  };

  const handleResetSimulation = () => {
    setIsPlaying(false);
    setCurrentStepIndex(0);
    if (simulationSteps.length > 0) {
      setTaskRuns(simulationSteps[0].taskRuns);
    }
  };

  // Real Backend State Polling
  const fetchRealState = useCallback(async () => {
    if (isSimulationMode) return;
    try {
      const wRes = await fetch(`${API_BASE}/workers`);
      if (wRes.ok) {
        setIsBackendConnected(true);
        const wData = await wRes.json();
        if (wData && wData.length > 0) setWorkers(wData);
      } else {
        setIsBackendConnected(false);
      }
      if (activeRunID) {
        const tRes = await fetch(`${API_BASE}/runs/${activeRunID}/tasks`);
        if (tRes.ok) {
          const tData = await tRes.json();
          setTaskRuns(tData || []);
        }
      }
    } catch (e) {
      setIsBackendConnected(false);
    }
  }, [activeRunID, isSimulationMode]);

  useEffect(() => {
    if (!isSimulationMode) {
      fetchRealState();
      const interval = setInterval(fetchRealState, 800);
      return () => clearInterval(interval);
    }
  }, [fetchRealState, isSimulationMode]);

  // Trigger Real Cluster Workflow Run
  const triggerRealWorkflowRun = async (def) => {
    try {
      await fetch(`${API_BASE}/workflows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(def)
      });
      const rRes = await fetch(`${API_BASE}/workflows/${def.id}/runs`, { method: 'POST' });
      if (rRes.ok) {
        const rData = await rRes.json();
        setActiveRunID(rData.run_id);
        const tRes = await fetch(`${API_BASE}/runs/${rData.run_id}/tasks`);
        if (tRes.ok) {
          const tData = await tRes.json();
          setTaskRuns(tData || []);
        }
      }
    } catch (e) {
      console.error('Failed to trigger workflow run:', e);
    }
  };

  // Select Preset
  const handleSelectPreset = async (key) => {
    setActivePreset(key);
    const def = PRESETS[key];
    setActiveWorkflow(def);
    setCurrentStepIndex(0);

    if (!isSimulationMode) {
      await triggerRealWorkflowRun(def);
    }
  };

  // Kill / Revive Worker
  const handleKillWorker = (workerID) => {
    setWorkers(prev => prev.map(w => w.id === workerID ? { ...w, status: w.status === 'killed' || w.status === 'dead' ? 'healthy' : 'killed' } : w));

    if (isSimulationMode) {
      setEvents(prev => [{
        event_type: 'worker.offline',
        payload: { worker_id: workerID, reason: 'heartbeat_stopped' },
        occurred_at: new Date().toISOString()
      }, ...prev]);
    } else {
      fetch(`${API_BASE}/chaos/kill-worker`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ worker_id: workerID })
      })
        .then(res => res.json())
        .then(() => fetchRealState())
        .catch(err => console.error('Failed to kill/revive worker:', err));
    }
  };

  if (currentView === '404') {
    return <NotFound onNavigateHome={navigateToHome} onNavigateSimulator={navigateToSimulator} />;
  }

  if (currentView === 'landing') {
    return <LandingPage onLaunchSimulator={navigateToSimulator} />;
  }

  const currentStepData = simulationSteps[currentStepIndex];

  // Dynamic Metrics Calculation in real time
  const completedTasks = taskRuns.filter(t => t.state === 'SUCCEEDED').length;
  const inFlightLeases = taskRuns.filter(t => t.state === 'RUNNING' || t.state === 'LEASED').length;
  const dlqTasks = taskRuns.filter(t => t.state === 'DLQ' || t.state === 'FAILED').length;
  const aliveWorkersCount = workers.filter(w => w.status !== 'killed' && w.status !== 'dead').length;

  return (
    <div style={{ maxWidth: '1240px', margin: '0 auto', padding: '1rem' }}>
      {/* 1. Navigation & Header Bar */}
      <header className="neo-box" style={{ padding: '0.65rem 1.25rem', marginBottom: '0.85rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '0.75rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.85rem' }}>
          <button
            onClick={navigateToHome}
            className="neo-btn neo-btn-sm"
            title="Return to Landing Page"
          >
            <ArrowLeft size={14} />
            Landing Page
          </button>

          <div style={{
            width: '36px',
            height: '36px',
            background: 'var(--pop-yellow)',
            border: '2px solid #000000',
            boxShadow: '2.5px 2.5px 0px #000000',
            borderRadius: '8px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            flexShrink: 0
          }}>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M13 2L3.5 13.5H12L10.5 22L20.5 10.5H12L13 2Z" fill="#000000" stroke="#000000" strokeWidth="1.5" strokeLinejoin="round" />
            </svg>
          </div>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <h1 style={{ fontSize: '1.25rem' }}>FlowForge Lab</h1>
              <span className="neo-pill pill-ready">v1.1 Go</span>
            </div>
          </div>
        </div>

        {/* Runtime Mode Selector */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
          <button
            onClick={() => { setIsSimulationMode(true); setIsPlaying(false); }}
            className={`neo-btn neo-btn-sm ${isSimulationMode ? 'neo-btn-primary' : ''}`}
          >
            <Sparkles size={14} />
            Step-by-Step Simulator
          </button>

          <button
            onClick={() => { setIsSimulationMode(false); setIsPlaying(false); }}
            className={`neo-btn neo-btn-sm ${!isSimulationMode ? 'neo-btn-primary' : ''}`}
          >
            <Server size={14} />
            Real Go Cluster
          </button>
        </div>
      </header>

      {/* 2. Real Cluster Connection Status Banner (When in Real Mode) */}
      {!isSimulationMode && (
        <div className="neo-box" style={{
          padding: '0.85rem 1.25rem',
          marginBottom: '0.85rem',
          background: isBackendConnected ? '#DCFCE7' : '#FEF3C7',
          borderColor: '#000000',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          flexWrap: 'wrap',
          gap: '0.75rem'
        }}>
          {isBackendConnected ? (
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', fontSize: '0.85rem', fontWeight: 800 }}>
              <span style={{ display: 'inline-block', width: '10px', height: '10px', borderRadius: '50%', background: '#16A34A', border: '1.5px solid #000' }}></span>
              <span>🟢 Live Go Server Connected (`localhost:8080`) — WebSockets & Redis Stream Active</span>
            </div>
          ) : (
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', fontSize: '0.85rem', fontWeight: 800 }}>
              <span style={{ display: 'inline-block', width: '10px', height: '10px', borderRadius: '50%', background: '#EAB308', border: '1.5px solid #000' }}></span>
              <span>📡 Real Go Server is not running on `localhost:8080` (Run `docker compose up -d` & `go run ./cmd/server` to connect local cluster).</span>
            </div>
          )}

          {!isBackendConnected && (
            <button
              onClick={() => setIsSimulationMode(true)}
              className="neo-btn neo-btn-sm neo-btn-primary"
            >
              <Sparkles size={13} />
              Switch to In-Browser Simulator
            </button>
          )}
        </div>
      )}

      {/* 2. Structured Step Narrative Bar with Quick Controls */}
      <StepNarrativeBar
        isSimulationMode={isSimulationMode}
        isBackendConnected={isBackendConnected}
        currentStepIndex={currentStepIndex}
        totalSteps={simulationSteps.length}
        stepData={currentStepData}
        activeWorkflowName={activeWorkflow.name}
        isPlaying={isPlaying}
        onTogglePlay={() => setIsPlaying(!isPlaying)}
        onStepNext={handleStepNext}
        onStepPrev={handleStepPrev}
        onReset={handleResetSimulation}
        speed={simulationSpeed}
        onSpeedChange={setSimulationSpeed}
        onRunWorkflow={() => triggerRealWorkflowRun(activeWorkflow)}
      />

      {/* 3. Compact Live HUD Metrics */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: '0.6rem', marginBottom: '0.85rem' }}>
        <div className="neo-box" style={{ padding: '0.5rem 0.85rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div>
            <div style={{ fontSize: '0.68rem', fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', fontWeight: 800 }}>ACTIVE WORKERS</div>
            <div style={{ fontSize: '1.25rem', fontWeight: 900 }}>{aliveWorkersCount}/3</div>
          </div>
          <Zap size={18} color="#6366F1" />
        </div>

        <div className="neo-box" style={{ padding: '0.5rem 0.85rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div>
            <div style={{ fontSize: '0.68rem', fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', fontWeight: 800 }}>TASKS SUCCEEDED</div>
            <div style={{ fontSize: '1.25rem', fontWeight: 900, color: '#047857' }}>{completedTasks}</div>
          </div>
          <CheckCircle2 size={18} color="#10B981" />
        </div>

        <div className="neo-box" style={{ padding: '0.5rem 0.85rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div>
            <div style={{ fontSize: '0.68rem', fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', fontWeight: 800 }}>ACTIVE LEASES</div>
            <div style={{ fontSize: '1.25rem', fontWeight: 900, color: '#0369A1' }}>{inFlightLeases}</div>
          </div>
          <Activity size={18} color="#0EA5E9" />
        </div>

        <div className="neo-box" style={{ padding: '0.5rem 0.85rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div>
            <div style={{ fontSize: '0.68rem', fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', fontWeight: 800 }}>DEAD LETTER QUEUE</div>
            <div style={{ fontSize: '1.25rem', fontWeight: 900, color: dlqTasks > 0 ? '#DC2626' : '#52525B' }}>{dlqTasks}</div>
          </div>
          <Skull size={18} color={dlqTasks > 0 ? '#EF4444' : '#94A3B8'} />
        </div>
      </div>

      {/* 4. Hero Visual DAG Graph Viewport */}
      <div style={{ marginBottom: '1rem' }}>
        <DAGVisualizer
          workflow={activeWorkflow}
          taskRuns={taskRuns}
          onSelectNode={(node, run) => setSelectedTask({ node, run })}
          activeNodeId={selectedTask?.node?.id}
        />
      </div>

      {/* 5. Controls & Chaos Dashboard */}
      <div className="neo-box" style={{ padding: '1.1rem', marginBottom: '1rem', background: '#FFFFFF' }}>
        <div style={{ display: 'grid', gridTemplateColumns: '1.4fr 1fr', gap: '1.25rem', flexWrap: 'wrap' }}>
          {/* Left: Workflow Selection */}
          <div>
            <div style={{ fontSize: '0.75rem', fontWeight: 800, fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', marginBottom: '0.5rem' }}>
              DISTRIBUTED SCENARIOS & WORKFLOW PRESETS
            </div>
            <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
              <button
                onClick={() => handleSelectPreset('diamond')}
                className={`neo-btn neo-btn-sm ${activePreset === 'diamond' ? 'neo-btn-primary' : ''}`}
              >
                <Play size={13} />
                Diamond Pipeline
              </button>

              <button
                onClick={() => handleSelectPreset('fanout')}
                className={`neo-btn neo-btn-sm ${activePreset === 'fanout' ? 'neo-btn-cyan' : ''}`}
              >
                <Cpu size={13} />
                8-Task Benchmark
              </button>

              <button
                onClick={() => handleSelectPreset('poison')}
                className={`neo-btn neo-btn-sm ${activePreset === 'poison' ? 'neo-btn-coral' : ''}`}
              >
                <Skull size={13} />
                Poison Pill (DLQ)
              </button>

              <button
                onClick={() => handleSelectPreset('zombie')}
                className={`neo-btn neo-btn-sm ${activePreset === 'zombie' ? 'neo-btn-purple' : ''}`}
              >
                <ShieldAlert size={13} />
                Zombie Fencing Demo
              </button>

              <button
                onClick={() => handleSelectPreset('duplicate')}
                className={`neo-btn neo-btn-sm ${activePreset === 'duplicate' ? 'neo-btn-green' : ''}`}
              >
                <Copy size={13} />
                Duplicate Delivery Demo
              </button>
            </div>
          </div>

          {/* Right: Worker Fleet & Chaos Testing */}
          <div>
            <div style={{ fontSize: '0.75rem', fontWeight: 800, fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', marginBottom: '0.5rem' }}>
              WORKER FLEET (CLICK TO KILL WORKER)
            </div>
            <div style={{ display: 'flex', gap: '0.5rem' }}>
              {workers.map(w => {
                const isDead = w.status === 'killed' || w.status === 'dead';
                return (
                  <button
                    key={w.id}
                    onClick={() => handleKillWorker(w.id)}
                    className={`neo-btn neo-btn-sm ${isDead ? 'neo-btn-coral' : ''}`}
                    style={{ flex: 1, justifyContent: 'center' }}
                  >
                    <Skull size={12} />
                    {w.id}: {isDead ? 'DEAD' : 'ALIVE'}
                  </button>
                );
              })}
            </div>
          </div>
        </div>
      </div>

      {/* 6. Collapsible Real-Time Event Feed */}
      <div className="neo-box" style={{ overflow: 'hidden' }}>
        <button
          onClick={() => setShowLogs(!showLogs)}
          style={{
            width: '100%',
            padding: '0.65rem 1.25rem',
            background: '#FFFFFF',
            border: 'none',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            cursor: 'pointer',
            fontFamily: 'var(--font-mono)',
            fontWeight: 800,
            fontSize: '0.8rem'
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <Terminal size={15} />
            <span>REDIS STREAM EVENT LOG ({events.length} Events Broadcasted)</span>
          </div>
          {showLogs ? <ChevronUp size={15} /> : <ChevronDown size={15} />}
        </button>

        {showLogs && (
          <div style={{ padding: '0.75rem 1.25rem', borderTop: '2px solid #000', maxHeight: '180px', overflowY: 'auto', background: '#FAFAF8', fontFamily: 'var(--font-mono)', fontSize: '0.75rem' }}>
            {events.length === 0 ? (
              <div style={{ color: 'var(--text-muted)', fontStyle: 'italic' }}>No events recorded yet.</div>
            ) : (
              events.map((e, idx) => (
                <div key={idx} style={{ padding: '0.2rem 0', borderBottom: '1px solid #E4E4E7' }}>
                  <span style={{ color: '#71717A', marginRight: '0.5rem' }}>[{new Date(e.occurred_at || Date.now()).toLocaleTimeString()}]</span>
                  <span style={{ fontWeight: 800, marginRight: '0.5rem' }}>{e.event_type}</span>
                  {e.payload && <span style={{ color: '#52525B' }}>{JSON.stringify(e.payload)}</span>}
                </div>
              ))
            )}
          </div>
        )}
      </div>

      {/* 7. Task Inspection Drawer */}
      {selectedTask && (
        <div style={{
          position: 'fixed',
          top: 0,
          right: 0,
          bottom: 0,
          width: '420px',
          maxWidth: '90vw',
          background: '#FFFFFF',
          borderLeft: '3px solid #000000',
          boxShadow: '-6px 0px 0px rgba(0,0,0,0.15)',
          padding: '1.5rem',
          zIndex: 1000,
          overflowY: 'auto'
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.25rem', borderBottom: '2px solid #000', paddingBottom: '0.6rem' }}>
            <h2 style={{ fontSize: '1.2rem', fontWeight: 900 }}>Task Inspector</h2>
            <button onClick={() => setSelectedTask(null)} className="neo-btn neo-btn-sm" style={{ padding: '0.2rem 0.5rem' }}>
              <X size={14} />
            </button>
          </div>

          <div style={{ marginBottom: '1rem' }}>
            <div style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', fontWeight: 800, color: 'var(--text-muted)' }}>NODE ID</div>
            <div style={{ fontSize: '1.2rem', fontWeight: 900, marginTop: '0.15rem' }}>{selectedTask.node.id}</div>
          </div>

          <div style={{ marginBottom: '1rem' }}>
            <div style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', fontWeight: 800, color: 'var(--text-muted)' }}>CURRENT STATE</div>
            <span className={`neo-pill ${selectedTask.run.state === 'SUCCEEDED' ? 'pill-succeeded' : selectedTask.run.state === 'DLQ' ? 'pill-dlq' : selectedTask.run.state === 'FAILED' ? 'pill-failed' : 'pill-ready'}`} style={{ marginTop: '0.3rem', display: 'inline-flex' }}>
              {selectedTask.run.state || 'BLOCKED'}
            </span>
          </div>

          <div style={{ marginBottom: '1rem' }}>
            <div style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', fontWeight: 800, color: 'var(--text-muted)' }}>LEASE OWNER</div>
            <div style={{ fontFamily: 'var(--font-mono)', fontWeight: 800, marginTop: '0.15rem' }}>
              {selectedTask.run.lease_owner || 'None (Unleased)'}
            </div>
          </div>

          {selectedTask.run.output_ref && (
            <div style={{ marginBottom: '1rem' }}>
              <div style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', fontWeight: 800, color: 'var(--text-muted)', marginBottom: '0.35rem' }}>OUTPUT ARTIFACT</div>
              <pre style={{
                fontFamily: 'var(--font-mono)',
                fontSize: '0.75rem',
                background: 'var(--pop-yellow)',
                padding: '0.65rem 0.85rem',
                border: '2px solid #000000',
                boxShadow: '2px 2px 0px #000000',
                borderRadius: '6px',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-all',
                color: '#000000',
                fontWeight: 700,
                lineHeight: 1.4,
                margin: 0
              }}>
                {(() => {
                  try {
                    return JSON.stringify(JSON.parse(selectedTask.run.output_ref), null, 2);
                  } catch (e) {
                    return selectedTask.run.output_ref;
                  }
                })()}
              </pre>
            </div>
          )}

          <div>
            <div style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', fontWeight: 800, color: 'var(--text-muted)', marginBottom: '0.35rem' }}>NODE CONFIGURATION</div>
            <pre style={{ background: '#18181B', color: '#86EFAC', padding: '0.75rem', borderRadius: '6px', fontSize: '0.75rem', overflowX: 'auto', border: '2px solid #000', whiteSpace: 'pre-wrap', wordBreak: 'break-all', margin: 0 }}>
              {JSON.stringify(selectedTask.node.config || {}, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}
