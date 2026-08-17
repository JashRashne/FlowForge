import React, { useEffect } from 'react';
import { Play, Sparkles, Server, ShieldCheck, Zap, ArrowRight, Code2, ExternalLink, Cpu, Database, Skull, Copy, CheckCircle2, Video, Layers } from 'lucide-react';

export default function LandingPage({ onLaunchSimulator }) {
  useEffect(() => {
    document.title = 'FlowForge';
  }, []);

  const features = [
    {
      icon: <ShieldCheck size={22} color="#000000" />,
      bg: 'var(--pop-yellow)',
      title: 'Atomic Lease Fencing',
      desc: 'UUID fencing tokens checked on every database commit. Stale/zombie workers are strictly rejected with zero split-brain data corruption.'
    },
    {
      icon: <Database size={22} color="#000000" />,
      bg: 'var(--pop-cyan)',
      title: 'Transactional Outbox',
      desc: 'State updates and events committed in the same ACID PostgreSQL transaction, fanning out to Redis stream consumer groups without lost events.'
    },
    {
      icon: <Cpu size={22} color="#000000" />,
      bg: 'var(--pop-purple)',
      title: "Kahn's DAG Algorithm",
      desc: 'O(V+E) topological sorting and cycle detection at the API boundary, with SQL CTE anti-joins for sub-millisecond dependency resolution.'
    },
    {
      icon: <Zap size={22} color="#000000" />,
      bg: 'var(--pop-green)',
      title: 'Self-Healing Recovery',
      desc: 'Scheduler monitors worker heartbeat clocks and automatically reclaims orphaned tasks from crashed nodes with exponential retry backoff.'
    }
  ];

  const scenarios = [
    { name: 'Diamond Data Pipeline', tag: 'Kahn\'s DAG', desc: '1 Ingest ➔ 2 Parallel Transforms ➔ 1 Summary Report' },
    { name: '8-Task Parallel Cluster', tag: 'High Throughput', desc: 'Massive parallel shard distribution across worker fleet' },
    { name: 'Zombie Worker Fencing', tag: 'ADR-08 Proof', desc: 'Stale commit rejection after long GC hang / network partition' },
    { name: 'Duplicate Stream Delivery', tag: 'Idempotency', desc: 'Prevents double-execution during network retries' },
    { name: 'Poison Pill DLQ', tag: 'Fault Isolation', desc: 'Deterministic failure isolation & Dead Letter Queue quarantine' }
  ];

  const handleLaunch = (e) => {
    if (e) e.preventDefault();
    if (onLaunchSimulator) onLaunchSimulator();
  };

  return (
    <div style={{ maxWidth: '1200px', margin: '0 auto', padding: '2rem 1.5rem' }}>
      {/* 1. Header Bar with Rich Neobrutalist Logo */}
      <header className="neo-box" style={{ padding: '0.85rem 1.5rem', marginBottom: '2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.95rem' }}>
          {/* Clean Bold Neobrutalist Logo Mark */}
          <div style={{
            width: '42px',
            height: '42px',
            background: 'var(--pop-yellow)',
            border: '2.5px solid #000000',
            boxShadow: '3px 3px 0px #000000',
            borderRadius: '9px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            position: 'relative',
            flexShrink: 0
          }}>
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M13 2L3.5 13.5H12L10.5 22L20.5 10.5H12L13 2Z" fill="#000000" stroke="#000000" strokeWidth="1.5" strokeLinejoin="round" />
            </svg>
          </div>

          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <h1 style={{ fontSize: '1.45rem', letterSpacing: '-0.02em', fontWeight: 900 }}>FlowForge</h1>
              <span className="neo-pill pill-ready">v1.1 Go</span>
            </div>
            <p style={{ fontSize: '0.78rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
              Fault-Tolerant Distributed Workflow Engine
            </p>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <a
            href="https://github.com/JashRashne/FlowForge"
            target="_blank"
            rel="noreferrer"
            className="neo-btn neo-btn-sm"
          >
            <Code2 size={14} />
            GitHub Code
          </a>

          <button
            onClick={handleLaunch}
            className="neo-btn neo-btn-sm neo-btn-primary"
          >
            <Play size={14} />
            Launch Simulator
          </button>
        </div>
      </header>

      {/* 2. Hero Section */}
      <section className="neo-box" style={{ padding: '3rem 2rem', marginBottom: '2.5rem', background: '#FFFFFF', textAlign: 'center', position: 'relative', overflow: 'hidden' }}>
        <div style={{ display: 'inline-flex', alignItems: 'center', gap: '0.5rem', background: 'var(--pop-yellow)', border: '2px solid #000', padding: '0.3rem 0.8rem', borderRadius: '999px', fontSize: '0.8rem', fontWeight: 800, fontFamily: 'var(--font-mono)', marginBottom: '1.25rem', boxShadow: '2px 2px 0px #000' }}>
          <Sparkles size={14} />
          DISTRIBUTED SYSTEMS ENGINEERING FROM FIRST PRINCIPLES
        </div>

        <h1 style={{ fontSize: 'clamp(2rem, 5vw, 3.2rem)', fontWeight: 900, lineHeight: 1.15, marginBottom: '1.25rem', letterSpacing: '-0.03em' }}>
          The Distributed Workflow Engine <br />
          <span style={{ background: 'var(--pop-cyan)', padding: '0.1rem 0.6rem', border: '2.5px solid #000', borderRadius: '8px', boxShadow: '3px 3px 0px #000', display: 'inline-block', transform: 'rotate(-1deg)' }}>
            Engineered to Fail Gracefully.
          </span>
        </h1>

        <p style={{ fontSize: '1.05rem', color: 'var(--text-muted)', maxWidth: '780px', margin: '0 auto 2rem auto', lineHeight: 1.6 }}>
          FlowForge is a resilient DAG orchestrator built from scratch in <b>Go</b>. It coordinates complex dependency pipelines across horizontally scaled workers with lease-based fencing, transactional outbox event streams, and self-healing crash recovery.
        </p>

        {/* Hero CTAs */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '1rem', flexWrap: 'wrap' }}>
          <button
            onClick={handleLaunch}
            className="neo-btn neo-btn-primary"
            style={{ fontSize: '1.05rem', padding: '0.85rem 1.8rem' }}
          >
            <Play size={18} />
            Explore Interactive Visual Simulator
            <ArrowRight size={18} />
          </button>

          <a
            href="https://www.youtube.com/watch?v=dQw4w9WgXcQ"
            target="_blank"
            rel="noreferrer"
            className="neo-btn neo-btn-coral"
            style={{ fontSize: '1.05rem', padding: '0.85rem 1.5rem', textDecoration: 'none' }}
          >
            <Video size={18} />
            Watch Real Server Demo (YouTube)
            <ExternalLink size={14} />
          </a>
        </div>

        {/* Notice Banner */}
        <div style={{ marginTop: '2rem', padding: '0.75rem 1.25rem', background: '#FBF9F4', border: '2px dashed #000', borderRadius: '8px', display: 'inline-block', maxWidth: '650px', fontSize: '0.82rem', fontFamily: 'var(--font-mono)' }}>
          ℹ️ <b>Live Simulator Notice:</b> The web app features an interactive simulation engine to visualize internal state transitions step-by-step. The full production cluster running against PostgreSQL & Redis is showcased in the video!
        </div>
      </section>

      {/* 3. Core Architectural Pillars */}
      <section style={{ marginBottom: '2.5rem' }}>
        <div style={{ textAlign: 'center', marginBottom: '1.5rem' }}>
          <h2 style={{ fontSize: '1.8rem', fontWeight: 800 }}>Reliability & Invariant Architecture</h2>
          <p style={{ fontSize: '0.9rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
            Every component is engineered to guarantee zero data corruption during distributed faults.
          </p>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: '1.25rem' }}>
          {features.map((f, idx) => (
            <div key={idx} className="neo-box" style={{ padding: '1.5rem', background: '#FFFFFF', display: 'flex', flexDirection: 'column', justifyContent: 'space-between' }}>
              <div>
                <div style={{
                  width: '42px',
                  height: '42px',
                  background: f.bg,
                  border: '2px solid #000',
                  boxShadow: '2.5px 2.5px 0px #000',
                  borderRadius: '8px',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  marginBottom: '1rem'
                }}>
                  {f.icon}
                </div>
                <h3 style={{ fontSize: '1.15rem', fontWeight: 800, marginBottom: '0.5rem' }}>{f.title}</h3>
                <p style={{ fontSize: '0.88rem', color: 'var(--text-muted)', lineHeight: 1.5 }}>{f.desc}</p>
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* 4. Interactive Scenarios Preview */}
      <section className="neo-box" style={{ padding: '2rem', marginBottom: '2.5rem', background: '#FFFFFF' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.25rem', borderBottom: '2px solid #000', paddingBottom: '0.8rem', flexWrap: 'wrap', gap: '0.5rem' }}>
          <div>
            <h2 style={{ fontSize: '1.4rem' }}>5 Interactive Distributed Scenarios</h2>
            <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
              Step through Kahn's DAG resolution, worker crashes, zombie fencing, and DLQ quarantine.
            </p>
          </div>
          <button onClick={handleLaunch} className="neo-btn neo-btn-sm neo-btn-green">
            <Play size={14} /> Open Simulator
          </button>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '0.85rem' }}>
          {scenarios.map((s, idx) => (
            <div key={idx} style={{ padding: '1rem', background: '#FBF9F4', border: '2px solid #000', borderRadius: '8px', boxShadow: '2.5px 2.5px 0px #000' }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.4rem' }}>
                <span style={{ fontWeight: 800, fontSize: '0.9rem' }}>{s.name}</span>
                <span className="neo-pill" style={{ background: 'var(--pop-yellow)', fontSize: '0.65rem' }}>{s.tag}</span>
              </div>
              <p style={{ fontSize: '0.78rem', color: 'var(--text-muted)' }}>{s.desc}</p>
            </div>
          ))}
        </div>
      </section>

      {/* 5. How To Use Guide */}
      <section className="neo-box" style={{ padding: '2rem', marginBottom: '2.5rem', background: 'var(--pop-cyan)' }}>
        <h2 style={{ fontSize: '1.4rem', marginBottom: '1rem', color: '#000000' }}>How to Use the Interactive Lab</h2>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: '1rem' }}>
          <div style={{ background: '#FFFFFF', padding: '1.25rem', border: '2px solid #000', borderRadius: '8px', boxShadow: '3px 3px 0px #000' }}>
            <div style={{ fontSize: '1.5rem', fontWeight: 900, marginBottom: '0.3rem' }}>1. Pick a Scenario</div>
            <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>
              Choose from Diamond Pipeline, 8-Task Benchmark, Zombie Fencing, or Poison Pill.
            </p>
          </div>

          <div style={{ background: '#FFFFFF', padding: '1.25rem', border: '2px solid #000', borderRadius: '8px', boxShadow: '3px 3px 0px #000' }}>
            <div style={{ fontSize: '1.5rem', fontWeight: 900, marginBottom: '0.3rem' }}>2. Step & Inspect</div>
            <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>
              Use <b>[Next Step ⏭]</b> or <b>[Auto Play ▶]</b> to watch topological progression and click any node to view JSON config.
            </p>
          </div>

          <div style={{ background: '#FFFFFF', padding: '1.25rem', border: '2px solid #000', borderRadius: '8px', boxShadow: '3px 3px 0px #000' }}>
            <div style={{ fontSize: '1.5rem', fontWeight: 900, marginBottom: '0.3rem' }}>3. Inject Chaos</div>
            <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>
              Click <b>[worker-2: DEAD]</b> to simulate a node crash and watch the Go Scheduler automatically self-heal and re-lease!
            </p>
          </div>
        </div>

        <div style={{ textAlign: 'center', marginTop: '1.5rem' }}>
          <button
            onClick={handleLaunch}
            className="neo-btn neo-btn-primary"
            style={{ fontSize: '1.05rem', padding: '0.75rem 1.8rem' }}
          >
            <Play size={16} />
            Launch Interactive Simulation Page
          </button>
        </div>
      </section>

      {/* 6. Footer */}
      <footer style={{ textAlign: 'center', borderTop: '2px solid #000', paddingTop: '1.5rem', fontFamily: 'var(--font-mono)', fontSize: '0.82rem', color: 'var(--text-muted)' }}>
        <p>FlowForge Distributed Workflow Engine • Built with Go, PostgreSQL, Redis & React</p>
        <p style={{ marginTop: '0.4rem' }}>
          <a href="https://github.com/JashRashne/FlowForge" target="_blank" rel="noreferrer" style={{ color: '#000', fontWeight: 800 }}>GitHub Repository</a> • <a href="https://github.com/JashRashne/FlowForge/blob/main/decisions.md" target="_blank" rel="noreferrer" style={{ color: '#000', fontWeight: 800 }}>Living ADR Log (14 Records)</a>
        </p>
      </footer>
    </div>
  );
}
