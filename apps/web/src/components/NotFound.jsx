import React, { useEffect } from 'react';
import { ArrowLeft, Play, Home, AlertOctagon, HelpCircle, Code2 } from 'lucide-react';

export default function NotFound({ onNavigateHome, onNavigateSimulator }) {
  useEffect(() => {
    document.title = 'Not Found';
  }, []);

  return (
    <div style={{ maxWidth: '1200px', margin: '0 auto', padding: '2rem 1.5rem', minHeight: '85vh', display: 'flex', flexDirection: 'column' }}>
      {/* 1. Header Bar for 404 Page */}
      <header className="neo-box" style={{ padding: '0.85rem 1.5rem', marginBottom: '3rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.95rem' }}>
          <div style={{
            width: '40px',
            height: '40px',
            background: 'var(--pop-yellow)',
            border: '2.5px solid #000000',
            boxShadow: '3px 3px 0px #000000',
            borderRadius: '8px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            flexShrink: 0
          }}>
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M13 2L3.5 13.5H12L10.5 22L20.5 10.5H12L13 2Z" fill="#000000" stroke="#000000" strokeWidth="1.5" strokeLinejoin="round" />
            </svg>
          </div>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <h1 style={{ fontSize: '1.3rem' }}>FlowForge</h1>
              <span className="neo-pill pill-ready">404</span>
            </div>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <button
            onClick={onNavigateHome}
            className="neo-btn neo-btn-sm"
          >
            <Home size={14} />
            Landing Page
          </button>

          <button
            onClick={onNavigateSimulator}
            className="neo-btn neo-btn-sm neo-btn-primary"
          >
            <Play size={14} />
            Simulator
          </button>
        </div>
      </header>

      {/* 2. Standalone 404 Hero Card */}
      <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div className="neo-box" style={{ maxWidth: '750px', width: '100%', padding: '3.5rem 2rem', background: '#FFFFFF', textAlign: 'center', position: 'relative' }}>
          <div style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: '0.5rem',
            background: 'var(--pop-coral)',
            color: '#FFFFFF',
            border: '2px solid #000',
            padding: '0.35rem 0.85rem',
            borderRadius: '999px',
            fontSize: '0.85rem',
            fontWeight: 800,
            fontFamily: 'var(--font-mono)',
            marginBottom: '1.5rem',
            boxShadow: '3px 3px 0px #000'
          }}>
            <AlertOctagon size={16} />
            ERROR 404: UNKNOWN_NODE_ROUTE
          </div>

          <h1 style={{ fontSize: 'clamp(2.5rem, 6vw, 4.5rem)', fontWeight: 900, lineHeight: 1.1, letterSpacing: '-0.03em', marginBottom: '1.25rem' }}>
            DAG Node Not Found <br />
            <span style={{ background: 'var(--pop-yellow)', padding: '0 0.6rem', border: '3px solid #000', borderRadius: '8px', boxShadow: '4px 4px 0px #000', display: 'inline-block', transform: 'rotate(-1.5deg)' }}>
              In-Degree = 0
            </span>
          </h1>

          <p style={{ fontSize: '1.05rem', color: 'var(--text-muted)', maxWidth: '520px', margin: '0 auto 2.25rem auto', lineHeight: 1.6 }}>
            The URL route you requested does not exist in our topological dependency graph. No worker could acquire this lease.
          </p>

          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '1rem', flexWrap: 'wrap' }}>
            <button
              onClick={onNavigateHome}
              className="neo-btn neo-btn-primary"
              style={{ fontSize: '1.05rem', padding: '0.85rem 1.8rem' }}
            >
              <Home size={18} />
              Return to Landing Page
            </button>

            <button
              onClick={onNavigateSimulator}
              className="neo-btn neo-btn-cyan"
              style={{ fontSize: '1.05rem', padding: '0.85rem 1.8rem' }}
            >
              <Play size={18} />
              Launch Live Simulator
            </button>
          </div>
        </div>
      </div>

      {/* 3. Footer */}
      <footer style={{ textAlign: 'center', borderTop: '2px solid #000', paddingTop: '1.5rem', marginTop: '2rem', fontFamily: 'var(--font-mono)', fontSize: '0.82rem', color: 'var(--text-muted)' }}>
        FlowForge Distributed Workflow Engine • <a href="https://github.com/JashRashne/FlowForge" target="_blank" rel="noreferrer" style={{ color: '#000', fontWeight: 800 }}>GitHub Repository</a>
      </footer>
    </div>
  );
}
