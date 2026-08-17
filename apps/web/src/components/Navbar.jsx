import React from 'react';
import { Activity, ShieldCheck, Zap, Database, RefreshCw } from 'lucide-react';

export default function Navbar({ isConnected, workerCount, activeRunID, onRefresh }) {
  return (
    <header className="neo-card" style={{ marginBottom: '1.5rem', padding: '1rem 1.5rem', background: '#FFFFFF', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
        <div style={{
          width: '42px',
          height: '42px',
          background: 'var(--neo-yellow)',
          border: '3px solid #000000',
          boxShadow: '3px 3px 0px #000000',
          borderRadius: '8px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontWeight: 900,
          fontSize: '1.4rem'
        }}>
          ⚡
        </div>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
            <h1 style={{ fontSize: '1.5rem', lineHeight: 1.1 }}>FlowForge</h1>
            <span className="neo-badge" style={{ background: 'var(--neo-cyan)' }}>v1.1 Go</span>
          </div>
          <p style={{ fontSize: '0.82rem', color: 'var(--text-muted)', fontWeight: 600 }}>
            Fault-Tolerant Distributed Workflow Orchestration Engine
          </p>
        </div>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
        {/* Connection Status */}
        <div className="neo-badge" style={{
          background: isConnected ? 'var(--neo-lime)' : 'var(--neo-coral)',
          color: isConnected ? '#000000' : '#FFFFFF'
        }}>
          <Activity size={14} />
          {isConnected ? 'WS LIVE CONNECTED' : 'WS RECONNECTING'}
        </div>

        {/* Worker Fleet Badge */}
        <div className="neo-badge" style={{ background: 'var(--neo-purple)', color: '#000000' }}>
          <Zap size={14} />
          {workerCount} WORKERS ACTIVE
        </div>

        {/* Active Run */}
        {activeRunID && (
          <div className="neo-badge" style={{ background: '#FFFFFF', color: '#000000' }}>
            <Database size={14} />
            RUN: {activeRunID.slice(0, 8)}...
          </div>
        )}

        <button onClick={onRefresh} className="neo-btn" style={{ padding: '0.4rem 0.8rem', fontSize: '0.85rem' }}>
          <RefreshCw size={15} />
          Sync
        </button>
      </div>
    </header>
  );
}
