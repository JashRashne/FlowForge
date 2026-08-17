import React from 'react';
import { Play, Flame, Skull, Cpu, Copy, RefreshCw, Zap, ShieldAlert, CheckCircle2 } from 'lucide-react';

export default function ChaosPanel({ onStartWorkflow, onKillWorker, activePreset, workers, isLoading, isSimulationMode }) {
  const presets = [
    {
      key: 'diamond',
      name: 'Diamond Pipeline',
      desc: '1 Root ➔ 2 Parallel ➔ 1 Summary',
      icon: <Play size={14} color="#6366F1" />
    },
    {
      key: 'fanout',
      name: '8-Task Benchmark',
      desc: 'High-throughput parallel cluster test',
      icon: <Cpu size={14} color="#0EA5E9" />
    },
    {
      key: 'poison',
      name: 'Poison Pill (DLQ)',
      desc: 'Deterministic failure & quarantine',
      icon: <Skull size={14} color="#EF4444" />
    }
  ];

  const defaultWorkers = [
    { id: 'worker-1', status: 'healthy' },
    { id: 'worker-2', status: 'healthy' },
    { id: 'worker-3', status: 'healthy' }
  ];

  const displayWorkers = workers && workers.length > 0 ? workers : defaultWorkers;

  return (
    <div className="min-card" style={{ padding: '1.25rem', marginBottom: '1.25rem', background: '#FFFFFF' }}>
      <div style={{ display: 'grid', gridTemplateColumns: '1.2fr 1fr', gap: '1.5rem', flexWrap: 'wrap' }}>
        {/* Left: Workflow Selection */}
        <div>
          <div style={{ fontSize: '0.78rem', fontWeight: 700, fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', marginBottom: '0.6rem' }}>
            STEP 1: SELECT WORKFLOW DAG TEMPLATE
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '0.6rem' }}>
            {presets.map(p => {
              const isSelected = activePreset === p.key;
              return (
                <button
                  key={p.key}
                  onClick={() => onStartWorkflow(p.key)}
                  disabled={isLoading}
                  className="min-card min-card-interactive"
                  style={{
                    padding: '0.75rem',
                    textAlign: 'left',
                    background: isSelected ? 'var(--accent-primary-light)' : '#FFFFFF',
                    border: isSelected ? '1.5px solid var(--accent-primary)' : '1px solid var(--border-subtle)',
                    cursor: 'pointer',
                    borderRadius: '8px'
                  }}
                >
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', fontWeight: 700, fontSize: '0.85rem', color: isSelected ? 'var(--accent-primary)' : 'var(--text-primary)' }}>
                    {p.icon}
                    <span>{p.name}</span>
                  </div>
                  <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: '0.2rem' }}>
                    {p.desc}
                  </div>
                </button>
              );
            })}
          </div>
        </div>

        {/* Right: Worker Fleet & Chaos Controls */}
        <div>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.6rem' }}>
            <span style={{ fontSize: '0.78rem', fontWeight: 700, fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}>
              STEP 2: WORKER FLEET & CHAOS TESTING
            </span>
            <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>
              Click to kill a worker
            </span>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.5rem' }}>
            {displayWorkers.map(w => {
              const isDead = w.status === 'dead' || w.status === 'killed';
              return (
                <div
                  key={w.id}
                  className="min-card"
                  style={{
                    padding: '0.6rem 0.75rem',
                    background: isDead ? '#FEF2F2' : '#F8FAFC',
                    border: isDead ? '1px solid #FECACA' : '1px solid var(--border-subtle)',
                    display: 'flex',
                    flexDirection: 'column',
                    justifyContent: 'space-between'
                  }}
                >
                  <div>
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', fontSize: '0.78rem', fontWeight: 700 }}>
                      <span>{w.id}</span>
                      <span style={{
                        width: '8px',
                        height: '8px',
                        borderRadius: '50%',
                        background: isDead ? '#EF4444' : '#10B981'
                      }}></span>
                    </div>
                    <div style={{ fontSize: '0.7rem', color: isDead ? '#DC2626' : 'var(--text-muted)', marginTop: '0.15rem' }}>
                      {isDead ? 'Heartbeat Dead' : 'Consuming'}
                    </div>
                  </div>

                  <button
                    onClick={() => onKillWorker(w.id)}
                    className="btn btn-sm btn-danger"
                    style={{ marginTop: '0.5rem', padding: '0.2rem 0.4rem', fontSize: '0.7rem', justifyContent: 'center' }}
                  >
                    <Skull size={11} />
                    {isDead ? 'Revive' : 'Kill'}
                  </button>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}
