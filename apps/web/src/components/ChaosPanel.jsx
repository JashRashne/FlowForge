import React, { useState } from 'react';
import { Flame, Play, Skull, Copy, FastForward, ShieldAlert, Cpu } from 'lucide-react';

export default function ChaosPanel({ onStartWorkflow, onKillWorker, onInjectChaos, workers, isLoading }) {
  const [selectedWorker, setSelectedWorker] = useState(workers[0]?.id || 'worker-1');

  return (
    <div className="neo-card" style={{ padding: '1.25rem', background: '#FFFFFF', marginBottom: '1.5rem' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem', borderBottom: '2px solid #000000', paddingBottom: '0.6rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
          <Flame size={22} color="#FF5E5E" />
          <h2 style={{ fontSize: '1.25rem' }}>Workflow Launcher & Chaos Engineering Lab</h2>
        </div>
        <span className="neo-badge" style={{ background: 'var(--neo-yellow)' }}>
          Active Fault Injection Engine
        </span>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.5rem' }}>
        {/* Left Column: Preset Workflows */}
        <div>
          <div style={{ fontSize: '0.85rem', fontWeight: 800, fontFamily: 'var(--font-mono)', marginBottom: '0.6rem', color: 'var(--text-muted)' }}>
            1. LAUNCH PRESET WORKFLOW DAG
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.6rem' }}>
            <button
              onClick={() => onStartWorkflow('diamond')}
              disabled={isLoading}
              className="neo-btn neo-btn-primary"
              style={{ flex: '1 1 auto' }}
            >
              <Play size={16} />
              Diamond Pipeline (Fan-Out/In)
            </button>

            <button
              onClick={() => onStartWorkflow('fanout')}
              disabled={isLoading}
              className="neo-btn neo-btn-cyan"
              style={{ flex: '1 1 auto' }}
            >
              <Cpu size={16} />
              8-Task Parallel Cluster Benchmark
            </button>

            <button
              onClick={() => onStartWorkflow('poison')}
              disabled={isLoading}
              className="neo-btn neo-btn-coral"
              style={{ flex: '1 1 auto' }}
            >
              <Skull size={16} />
              Poison Task (DLQ Test)
            </button>
          </div>
        </div>

        {/* Right Column: Chaos Controls */}
        <div>
          <div style={{ fontSize: '0.85rem', fontWeight: 800, fontFamily: 'var(--font-mono)', marginBottom: '0.6rem', color: 'var(--text-muted)' }}>
            2. INJECT SYSTEM FAULTS
          </div>
          <div style={{ display: 'flex', gap: '0.6rem', alignItems: 'center', flexWrap: 'wrap' }}>
            <select
              value={selectedWorker}
              onChange={(e) => setSelectedWorker(e.target.value)}
              className="neo-card"
              style={{
                padding: '0.55rem 0.8rem',
                fontFamily: 'var(--font-mono)',
                fontWeight: 700,
                fontSize: '0.9rem',
                border: '3px solid #000000',
                borderRadius: '6px',
                cursor: 'pointer',
                background: '#FFFFFF'
              }}
            >
              {workers.length > 0 ? (
                workers.map(w => <option key={w.id} value={w.id}>{w.id} ({w.status})</option>)
              ) : (
                <>
                  <option value="worker-1">worker-1</option>
                  <option value="worker-2">worker-2</option>
                  <option value="worker-3">worker-3</option>
                </>
              )}
            </select>

            <button
              onClick={() => onKillWorker(selectedWorker)}
              className="neo-btn neo-btn-coral"
            >
              <Skull size={16} />
              Kill Worker Process
            </button>

            <button
              onClick={() => onInjectChaos('duplicate')}
              className="neo-btn neo-btn-purple"
            >
              <Copy size={16} />
              Duplicate Stream Message
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
