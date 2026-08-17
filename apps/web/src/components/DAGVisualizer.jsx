import React from 'react';
import { Globe, Database, Cpu, Loader2, CheckCircle2, AlertCircle, ArrowRight } from 'lucide-react';

export default function DAGVisualizer({ workflow, taskRuns, onSelectNode, activeNodeId }) {
  if (!workflow || !workflow.nodes) {
    return (
      <div className="graph-viewport" style={{ padding: '2.5rem', textAlign: 'center' }}>
        <p style={{ fontFamily: 'var(--font-mono)', fontWeight: 700, color: 'var(--text-muted)' }}>
          No active DAG loaded. Select a workflow from the toolbar below to start.
        </p>
      </div>
    );
  }

  const nodes = workflow.nodes || {};
  const edges = workflow.edges || [];

  // Build task state lookup
  const runMap = {};
  (taskRuns || []).forEach(tr => {
    runMap[tr.node_id] = tr;
  });

  // Calculate topological levels (ranks)
  const inDegree = {};
  const childrenMap = {};
  Object.keys(nodes).forEach(id => {
    inDegree[id] = 0;
    childrenMap[id] = [];
  });

  edges.forEach(e => {
    if (inDegree[e.to] !== undefined) inDegree[e.to]++;
    if (childrenMap[e.from]) childrenMap[e.from].push(e.to);
  });

  const levels = {};
  const queue = [];
  Object.keys(nodes).forEach(id => {
    if (inDegree[id] === 0) {
      levels[id] = 0;
      queue.push(id);
    }
  });

  const inDegreeCopy = { ...inDegree };
  while (queue.length > 0) {
    const curr = queue.shift();
    const currLevel = levels[curr] || 0;
    (childrenMap[curr] || []).forEach(child => {
      levels[child] = Math.max(levels[child] || 0, currLevel + 1);
      inDegreeCopy[child]--;
      if (inDegreeCopy[child] === 0) {
        queue.push(child);
      }
    });
  }

  // Group nodes by level for coordinates
  const columns = {};
  Object.entries(levels).forEach(([nodeId, lvl]) => {
    if (!columns[lvl]) columns[lvl] = [];
    columns[lvl].push(nodeId);
  });

  // Generous node dimensions so long status pills (e.g. SUCCEEDED) never truncate text
  const NODE_WIDTH = 236;
  const NODE_HEIGHT = 100;
  const COL_GAP = 95;
  const ROW_GAP = 30;
  const PAD_X = 40;
  const PAD_Y = 32;

  const positions = {};
  let maxColWidth = 0;
  let maxColHeight = 0;

  Object.entries(columns).forEach(([lvlStr, nodeIds]) => {
    const colIdx = parseInt(lvlStr, 10);
    const colHeight = nodeIds.length * NODE_HEIGHT + (nodeIds.length - 1) * ROW_GAP;
    maxColHeight = Math.max(maxColHeight, colHeight);

    nodeIds.forEach((nodeId, rowIdx) => {
      const x = PAD_X + colIdx * (NODE_WIDTH + COL_GAP);
      const y = PAD_Y + rowIdx * (NODE_HEIGHT + ROW_GAP);
      positions[nodeId] = { x, y };
      maxColWidth = Math.max(maxColWidth, x + NODE_WIDTH + PAD_X);
    });
  });

  const svgWidth = Math.max(maxColWidth, 800);
  const svgHeight = Math.max(maxColHeight + PAD_Y * 2, 330);

  const getNodeIcon = (type) => {
    switch (type) {
      case 'http': return <Globe size={14} />;
      case 'python': return <Database size={14} />;
      default: return <Cpu size={14} />;
    }
  };

  const getPillClass = (state) => {
    switch (state) {
      case 'READY': return 'pill-ready';
      case 'LEASED': return 'pill-leased';
      case 'RUNNING': return 'pill-running';
      case 'SUCCEEDED': return 'pill-succeeded';
      case 'FAILED': return 'pill-failed';
      case 'DLQ': return 'pill-dlq';
      default: return 'pill-blocked';
    }
  };

  return (
    <div className="graph-viewport" style={{ width: '100%', minHeight: `${svgHeight}px` }}>
      {/* SVG Arrows Layer */}
      <svg
        style={{
          position: 'absolute',
          top: 0,
          left: 0,
          width: `${svgWidth}px`,
          height: `${svgHeight}px`,
          pointerEvents: 'none',
          zIndex: 1
        }}
      >
        <defs>
          <marker
            id="neo-arrow"
            markerWidth="8"
            markerHeight="6"
            refX="7"
            refY="3"
            orient="auto"
          >
            <polygon points="0 0, 8 3, 0 6" fill="#000000" />
          </marker>
          <marker
            id="neo-arrow-done"
            markerWidth="8"
            markerHeight="6"
            refX="7"
            refY="3"
            orient="auto"
          >
            <polygon points="0 0, 8 3, 0 6" fill="#10B981" />
          </marker>
        </defs>

        {edges.map((edge, idx) => {
          const fromPos = positions[edge.from];
          const toPos = positions[edge.to];
          if (!fromPos || !toPos) return null;

          const startX = fromPos.x + NODE_WIDTH;
          const startY = fromPos.y + NODE_HEIGHT / 2;
          const endX = toPos.x;
          const endY = toPos.y + NODE_HEIGHT / 2;

          const ctrl = (endX - startX) / 2;
          const d = `M ${startX} ${startY} C ${startX + ctrl} ${startY}, ${endX - ctrl} ${endY}, ${endX} ${endY}`;

          const fromRun = runMap[edge.from];
          const isDone = fromRun && fromRun.state === 'SUCCEEDED';

          return (
            <path
              key={idx}
              d={d}
              fill="none"
              stroke={isDone ? '#10B981' : '#000000'}
              strokeWidth={isDone ? '3' : '2.2'}
              strokeDasharray={isDone ? 'none' : '5 4'}
              markerEnd={isDone ? 'url(#neo-arrow-done)' : 'url(#neo-arrow)'}
              style={{ transition: 'all 0.3s ease' }}
            />
          );
        })}
      </svg>

      {/* Nodes Layer */}
      <div style={{ position: 'relative', width: `${svgWidth}px`, height: `${svgHeight}px`, zIndex: 2 }}>
        {Object.entries(nodes).map(([nodeId, node]) => {
          const pos = positions[nodeId] || { x: 40, y: 30 };
          const run = runMap[nodeId] || { state: 'BLOCKED', attempt: 0 };
          const state = run.state || 'BLOCKED';
          const isSelected = activeNodeId === nodeId;

          return (
            <div
              key={nodeId}
              onClick={() => onSelectNode && onSelectNode(node, run)}
              className="neo-box"
              style={{
                position: 'absolute',
                left: `${pos.x}px`,
                top: `${pos.y}px`,
                width: `${NODE_WIDTH}px`,
                height: `${NODE_HEIGHT}px`,
                padding: '0.75rem 0.95rem',
                cursor: 'pointer',
                background: state === 'SUCCEEDED' ? '#ECFDF5' : (state === 'RUNNING' ? '#FAF5FF' : '#FFFFFF'),
                boxShadow: isSelected ? '5px 5px 0px #000000' : '3px 3px 0px #000000',
                transform: isSelected ? 'translate(-2px, -2px)' : 'none',
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'space-between',
                transition: 'all 0.15s ease'
              }}
            >
              <div>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '0.4rem', marginBottom: '0.25rem' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', fontWeight: 800, fontSize: '0.9rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {getNodeIcon(node.type)}
                    <span>{nodeId}</span>
                  </div>
                  <span className={`neo-pill ${getPillClass(state)}`} style={{ fontSize: '0.66rem', padding: '0.12rem 0.45rem', flexShrink: 0 }}>
                    {state === 'RUNNING' && <Loader2 size={9} style={{ animation: 'spin 1s linear infinite' }} />}
                    {state}
                  </span>
                </div>

                <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                  Adapter: <b>{node.type || 'synthetic'}</b>
                </div>
              </div>

              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderTop: '1.5px dashed #E4E4E7', paddingTop: '0.35rem', fontSize: '0.72rem', fontFamily: 'var(--font-mono)' }}>
                <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '145px' }}>
                  {run.is_expired ? (
                    <span style={{ background: '#FEE2E2', color: '#DC2626', padding: '0.08rem 0.35rem', border: '1.5px solid #DC2626', borderRadius: '4px', fontWeight: 800, fontSize: '0.68rem', display: 'inline-block' }}>
                      ⚠️ EXPIRED ({run.lease_owner ? run.lease_owner.replace('worker-', 'W').toUpperCase() : 'DEAD'})
                    </span>
                  ) : run.is_recovered ? (
                    <span style={{ background: '#E0F2FE', color: '#0369A1', padding: '0.08rem 0.35rem', border: '1.5px solid #0284C7', borderRadius: '4px', fontWeight: 800, fontSize: '0.68rem', display: 'inline-block' }}>
                      🛡️ RECLAIMED
                    </span>
                  ) : run.lease_owner ? (
                    <span style={{ background: 'var(--pop-yellow)', padding: '0.08rem 0.35rem', border: '1.5px solid #000', borderRadius: '4px', fontWeight: 800, fontSize: '0.68rem', display: 'inline-block' }}>
                      ⚡ {run.lease_owner}
                    </span>
                  ) : (
                    <span style={{ color: '#A1A1AA' }}>Unassigned</span>
                  )}
                </span>
                <span style={{ flexShrink: 0, fontWeight: 700 }}>Att: <b>{run.attempt || 0}</b></span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
