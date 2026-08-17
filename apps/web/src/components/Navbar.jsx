import React from 'react';
import { Activity, ShieldCheck, Zap, Database, RefreshCw, Layers } from 'lucide-react';

export default function Navbar({ isConnected, workerCount, activeRunID, onRefresh, isSimulationMode }) {
  return (
    <header className="min-card" style={{ marginBottom: '1.25rem', padding: '0.85rem 1.5rem', background: '#FFFFFF', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '1rem' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.85rem' }}>
        <div style={{
          width: '36px',
          height: '36px',
          background: 'var(--accent-primary)',
          color: '#FFFFFF',
          borderRadius: '8px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontWeight: 800,
          fontSize: '1.1rem'
        }}>
          ⚡
        </div>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <h1 style={{ fontSize: '1.2rem', fontWeight: 800, letterSpacing: '-0.02em' }}>FlowForge</h1>
            <span style={{ fontSize: '0.72rem', background: 'var(--bg-subtle)', color: 'var(--text-secondary)', padding: '0.15rem 0.45rem', borderRadius: '4px', fontWeight: 700, fontFamily: 'var(--font-mono)' }}>
              v1.1 Go
            </span>
          </div>
          <p style={{ fontSize: '0.78rem', color: 'var(--text-muted)' }}>
            Distributed Workflow Orchestration Engine & Crash Recovery Lab
          </p>
        </div>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
        {/* Connection Status */}
        <div className="status-pill" style={{
          background: isConnected ? 'var(--state-succeeded-bg)' : 'var(--state-failed-bg)',
          color: isConnected ? '#047857' : '#B91C1C'
        }}>
          <Activity size={12} />
          {isSimulationMode ? 'SIMULATION CLOCK' : (isConnected ? 'WS LIVE' : 'WS RECONNECTING')}
        </div>

        {/* Worker Pool */}
        <div className="status-pill" style={{ background: 'var(--bg-subtle)', color: 'var(--text-secondary)' }}>
          <Zap size={12} color="#6366F1" />
          {workerCount} WORKERS
        </div>

        {/* Sync Button */}
        <button onClick={onRefresh} className="btn btn-sm btn-outline">
          <RefreshCw size={13} />
          Sync State
        </button>
      </div>
    </header>
  );
}
