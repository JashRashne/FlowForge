import React from 'react';
import { Check, Loader2, AlertCircle, Clock, ShieldAlert, Cpu, Database, Globe } from 'lucide-react';

export default function DAGVisualizer({ workflow, taskRuns, onSelectTask }) {
  if (!workflow || !workflow.nodes) {
    return (
      <div className="neo-card" style={{ padding: '3rem', textAlign: 'center', background: '#FFFFFF' }}>
        <p style={{ fontFamily: 'var(--font-mono)', fontWeight: 700, color: 'var(--text-muted)' }}>
          No active workflow loaded. Click a preset above to launch a distributed DAG!
        </p>
      </div>
    );
  }

  // Group nodes into topological layers/columns
  const nodeMap = workflow.nodes || {};
  const edges = workflow.edges || [];

  // Create a map of task run states
  const runMap = {};
  (taskRuns || []).forEach(tr => {
    runMap[tr.node_id] = tr;
  });

  const getStatusBadgeClass = (state) => {
    switch (state) {
      case 'READY': return 'badge-ready';
      case 'LEASED': return 'badge-leased';
      case 'RUNNING': return 'badge-running';
      case 'SUCCEEDED': return 'badge-succeeded';
      case 'RETRY_WAIT': return 'badge-retry_wait';
      case 'FAILED': return 'badge-failed';
      case 'DLQ': return 'badge-dlq';
      default: return 'badge-blocked';
    }
  };

  const getNodeIcon = (type) => {
    switch (type) {
      case 'http': return <Globe size={16} />;
      case 'python': return <Database size={16} />;
      default: return <Cpu size={16} />;
    }
  };

  return (
    <div className="neo-card" style={{ padding: '1.5rem', background: '#FFFFFF', position: 'relative', overflow: 'hidden' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem', borderBottom: '2px solid #000000', paddingBottom: '0.6rem' }}>
        <div>
          <h2 style={{ fontSize: '1.3rem' }}>{workflow.name || 'Active DAG Topology'}</h2>
          <p style={{ fontSize: '0.82rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
            {workflow.description || 'Live Node Execution & Lease Fencing State'}
          </p>
        </div>
        <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
          <span className="neo-badge badge-blocked">BLOCKED</span>
          <span className="neo-badge badge-ready">READY</span>
          <span className="neo-badge badge-leased">LEASED</span>
          <span className="neo-badge badge-running">RUNNING</span>
          <span className="neo-badge badge-succeeded">SUCCEEDED</span>
          <span className="neo-badge badge-failed">FAILED</span>
        </div>
      </div>

      {/* Grid of Nodes */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))',
        gap: '1.25rem',
        padding: '0.5rem 0'
      }}>
        {Object.entries(nodeMap).map(([nodeID, node]) => {
          const run = runMap[nodeID] || { state: 'BLOCKED', attempt: 0 };
          const state = run.state || 'BLOCKED';

          return (
            <div
              key={nodeID}
              onClick={() => onSelectTask && onSelectTask(node, run)}
              className="neo-card neo-card-interactive"
              style={{
                padding: '1rem',
                background: state === 'SUCCEEDED' ? '#F0FDF4' : (state === 'RUNNING' ? '#FAF5FF' : '#FFFFFF'),
                cursor: 'pointer',
                border: '3px solid #000000',
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'space-between',
                minHeight: '140px'
              }}
            >
              <div>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', fontFamily: 'var(--font-mono)', fontWeight: 800, fontSize: '0.95rem' }}>
                    {getNodeIcon(node.type)}
                    {nodeID}
                  </div>
                  <span className={`neo-badge ${getStatusBadgeClass(state)}`}>
                    {state === 'RUNNING' && <Loader2 size={11} className="spin" style={{ animation: 'spin 1s linear infinite' }} />}
                    {state}
                  </span>
                </div>

                <div style={{ fontSize: '0.78rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                  Type: <b>{node.type || 'synthetic'}</b> | Max Retries: {node.max_retries || 3}
                </div>
              </div>

              <div style={{ marginTop: '0.8rem', paddingTop: '0.6rem', borderTop: '2px dashed #E5E7EB', display: 'flex', alignItems: 'center', justifyContent: 'space-between', fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
                <span>
                  {run.lease_owner ? (
                    <span style={{ background: 'var(--neo-yellow)', padding: '0.1rem 0.4rem', border: '1.5px solid #000', borderRadius: '4px', fontWeight: 700 }}>
                      ⚡ {run.lease_owner}
                    </span>
                  ) : (
                    <span style={{ color: '#9CA3AF' }}>No worker leased</span>
                  )}
                </span>
                <span>Attempt: <b>{run.attempt || 0}</b></span>
              </div>
            </div>
          );
        })}
      </div>

      {/* Dependency Links summary */}
      {edges.length > 0 && (
        <div style={{ marginTop: '1.5rem', padding: '0.8rem', background: '#F8FAFC', border: '2px solid #000000', borderRadius: '6px', fontSize: '0.8rem', fontFamily: 'var(--font-mono)' }}>
          <div style={{ fontWeight: 800, marginBottom: '0.3rem' }}>🔗 DIRECTED DEPENDENCY EDGES:</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.6rem' }}>
            {edges.map((e, idx) => (
              <span key={idx} style={{ background: '#FFFFFF', padding: '0.2rem 0.5rem', border: '1.5px solid #000', borderRadius: '4px', fontWeight: 600 }}>
                {e.from} ➔ {e.to}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
