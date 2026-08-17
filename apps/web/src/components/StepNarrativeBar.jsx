import React from 'react';
import { Play, Pause, SkipForward, SkipBack, RotateCcw, Sparkles, Zap, Activity, Info, ArrowRight, CheckCircle2, AlertTriangle, ShieldCheck, Flame, WifiOff, Wifi } from 'lucide-react';

export default function StepNarrativeBar({
  isSimulationMode,
  isBackendConnected,
  currentStepIndex,
  totalSteps,
  stepData,
  activeWorkflowName,
  isPlaying,
  onTogglePlay,
  onStepNext,
  onStepPrev,
  onReset,
  speed,
  onSpeedChange
}) {
  if (!isSimulationMode) {
    return (
      <div className="neo-box" style={{
        padding: '0.85rem 1.25rem',
        marginBottom: '0.85rem',
        background: '#FFFFFF',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        flexWrap: 'wrap',
        gap: '0.75rem'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.85rem' }}>
          <div style={{
            background: 'var(--pop-cyan)',
            border: '2px solid #000000',
            boxShadow: '2px 2px 0px #000000',
            borderRadius: '6px',
            padding: '0.3rem 0.65rem',
            fontFamily: 'var(--font-mono)',
            fontWeight: 900,
            fontSize: '0.78rem'
          }}>
            LIVE CLUSTER
          </div>
          <div>
            <div style={{ fontWeight: 800, fontSize: '0.98rem' }}>
              Active Run: {activeWorkflowName}
            </div>
            <div style={{ fontSize: '0.78rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)', marginTop: '0.15rem' }}>
              Executing on distributed Go workers with Redis stream groups and PostgreSQL lease fencing.
            </div>
          </div>
        </div>

        {isBackendConnected ? (
          <div className="neo-pill pill-running" style={{ background: '#DCFCE7', color: '#166534', border: '1.5px solid #16A34A', fontWeight: 800 }}>
            <Wifi size={12} />
            WEBSOCKET LIVE
          </div>
        ) : (
          <div className="neo-pill pill-failed" style={{ background: '#FEE2E2', color: '#991B1B', border: '1.5px solid #DC2626', fontWeight: 800 }}>
            <WifiOff size={12} />
            BACKEND OFFLINE
          </div>
        )}
      </div>
    );
  }

  const step = stepData || {
    component: "DAG PROGRESSION",
    title: "Workflow Initialized",
    description: "Click 'Next Step' or 'Auto Play' to begin stepping through the distributed engine.",
    effect: "Engine ready."
  };

  return (
    <div className="neo-box" style={{
      padding: '0.85rem 1.2rem',
      marginBottom: '0.9rem',
      background: '#FFFFFF',
      display: 'flex',
      flexDirection: 'column',
      gap: '0.65rem'
    }}>
      {/* Top Header Row */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '1rem' }}>
        {/* Left Side: Step Counter & Tag */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
          <div style={{
            background: 'var(--pop-yellow)',
            border: '2px solid #000000',
            boxShadow: '2px 2px 0px #000000',
            borderRadius: '6px',
            padding: '0.25rem 0.6rem',
            fontFamily: 'var(--font-mono)',
            fontWeight: 900,
            fontSize: '0.78rem',
            display: 'flex',
            alignItems: 'center',
            gap: '0.35rem'
          }}>
            <Sparkles size={12} />
            <span>STEP {currentStepIndex + 1} OF {totalSteps}</span>
          </div>

          <span className="neo-pill" style={{ background: 'var(--pop-cyan)', fontSize: '0.7rem', padding: '0.15rem 0.5rem' }}>
            {step.component || "DAG ENGINE"}
          </span>
        </div>

        {/* Right Side: Step Navigation & Speed Controls */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.45rem', flexWrap: 'wrap' }}>
          <button
            onClick={onStepPrev}
            disabled={isPlaying || currentStepIndex === 0}
            className="neo-btn neo-btn-sm"
            style={{ opacity: currentStepIndex === 0 ? 0.4 : 1 }}
            title="Previous Step"
          >
            <SkipBack size={13} />
            Prev
          </button>

          <button
            onClick={onStepNext}
            disabled={isPlaying || currentStepIndex >= totalSteps - 1}
            className="neo-btn neo-btn-sm neo-btn-green"
            style={{ opacity: currentStepIndex >= totalSteps - 1 ? 0.4 : 1 }}
            title="Next Step"
          >
            <SkipForward size={13} />
            Next Step
          </button>

          <button
            onClick={onTogglePlay}
            className={`neo-btn neo-btn-sm ${isPlaying ? 'neo-btn-coral' : 'neo-btn-primary'}`}
          >
            {isPlaying ? <Pause size={13} /> : <Play size={13} />}
            {isPlaying ? 'Pause' : 'Auto Play'}
          </button>

          <button
            onClick={onReset}
            className="neo-btn neo-btn-sm"
            title="Rewind to start"
          >
            <RotateCcw size={13} />
          </button>

          <select
            value={speed}
            onChange={(e) => onSpeedChange && onSpeedChange(Number(e.target.value))}
            className="neo-pill"
            style={{ background: '#FFFFFF', cursor: 'pointer', outline: 'none', marginLeft: '0.3rem', fontSize: '0.72rem' }}
          >
            <option value={1800}>0.5x</option>
            <option value={1200}>1.0x</option>
            <option value={500}>2.0x</option>
          </select>
        </div>
      </div>

      {/* Main Content: Clean Headline + Explanation + Effect */}
      <div style={{ borderTop: '2px dashed #E4E4E7', paddingTop: '0.75rem' }}>
        <h3 style={{ fontSize: '1.05rem', fontWeight: 800, marginBottom: '0.35rem', color: '#000000' }}>
          {step.title}
        </h3>
        <p style={{ fontSize: '0.88rem', color: '#27272A', lineHeight: 1.5, marginBottom: '0.35rem' }}>
          {step.description}
        </p>
        {step.effect && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', fontSize: '0.78rem', color: '#047857', fontFamily: 'var(--font-mono)', fontWeight: 700 }}>
            <span>↳ System Invariant:</span>
            <span>{step.effect}</span>
          </div>
        )}
      </div>

      {/* Segmented Step Progress Bar */}
      <div style={{ display: 'flex', gap: '4px', height: '5px', width: '100%', marginTop: '0.2rem' }}>
        {Array.from({ length: totalSteps }).map((_, idx) => {
          const isPast = idx < currentStepIndex;
          const isCurrent = idx === currentStepIndex;
          return (
            <div
              key={idx}
              style={{
                flex: 1,
                borderRadius: '3px',
                border: '1.5px solid #000000',
                background: isCurrent ? 'var(--pop-yellow)' : (isPast ? '#000000' : '#F4F0EA'),
                transition: 'all 0.2s ease'
              }}
            />
          );
        })}
      </div>
    </div>
  );
}
