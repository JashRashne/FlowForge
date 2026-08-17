import React from 'react';
import { Play, Pause, SkipForward, RotateCcw, Sparkles, Cpu, Layers, Info } from 'lucide-react';

export default function SimulationController({
  isSimulationMode,
  onToggleMode,
  isPlaying,
  onTogglePlay,
  onStepNext,
  onReset,
  currentStepIndex,
  totalSteps,
  stepNarrative,
  speed,
  onSpeedChange
}) {
  return (
    <div className="min-card" style={{ padding: '1rem 1.25rem', marginBottom: '1.25rem', background: '#FFFFFF' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '1rem' }}>
        {/* Left: Mode Switcher */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
          <div style={{ display: 'flex', background: 'var(--bg-subtle)', padding: '0.25rem', borderRadius: '8px', border: '1px solid var(--border-subtle)' }}>
            <button
              onClick={() => onToggleMode(false)}
              className={`btn btn-sm ${!isSimulationMode ? 'btn-primary' : 'btn-outline'}`}
              style={{ border: 'none', borderRadius: '6px' }}
            >
              <Cpu size={14} />
              Real Go Backend
            </button>
            <button
              onClick={() => onToggleMode(true)}
              className={`btn btn-sm ${isSimulationMode ? 'btn-primary' : 'btn-outline'}`}
              style={{ border: 'none', borderRadius: '6px' }}
            >
              <Sparkles size={14} />
              Step-by-Step Simulation
            </button>
          </div>
          <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
            {isSimulationMode ? 'Visualizing engine state transitions step-by-step' : 'Executing on real Go cluster & PostgreSQL'}
          </span>
        </div>

        {/* Right: Simulation Controls (Active when in simulation mode) */}
        {isSimulationMode && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <button onClick={onTogglePlay} className={`btn btn-sm ${isPlaying ? 'btn-danger' : 'btn-primary'}`}>
              {isPlaying ? <Pause size={14} /> : <Play size={14} />}
              {isPlaying ? 'Pause' : 'Auto Play'}
            </button>

            <button onClick={onStepNext} disabled={isPlaying} className="btn btn-sm btn-outline">
              <SkipForward size={14} />
              Step Next
            </button>

            <button onClick={onReset} className="btn btn-sm btn-outline">
              <RotateCcw size={14} />
              Reset
            </button>

            <div style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', marginLeft: '0.5rem' }}>
              <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)', fontWeight: 600 }}>Speed:</span>
              <select
                value={speed}
                onChange={(e) => onSpeedChange(Number(e.target.value))}
                style={{
                  padding: '0.25rem 0.5rem',
                  fontSize: '0.75rem',
                  borderRadius: '4px',
                  border: '1px solid var(--border-subtle)',
                  background: '#FFFFFF',
                  fontFamily: 'var(--font-mono)'
                }}
              >
                <option value={1500}>0.5x (Slow)</option>
                <option value={800}>1.0x (Normal)</option>
                <option value={300}>2.0x (Fast)</option>
              </select>
            </div>
          </div>
        )}
      </div>

      {/* Step Explanation Banner */}
      {isSimulationMode && (
        <div style={{
          marginTop: '0.85rem',
          padding: '0.75rem 1rem',
          background: 'var(--accent-primary-light)',
          border: '1px solid #C7D2FE',
          borderRadius: '8px',
          display: 'flex',
          alignItems: 'center',
          gap: '0.75rem'
        }}>
          <div style={{
            background: 'var(--accent-primary)',
            color: '#FFFFFF',
            borderRadius: '50%',
            width: '24px',
            height: '24px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.75rem',
            fontWeight: 800,
            flexShrink: 0
          }}>
            {currentStepIndex + 1}
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: '0.85rem', fontWeight: 600, color: '#312E81' }}>
              {stepNarrative || 'Click "Step Next" or "Auto Play" to start the distributed workflow execution.'}
            </div>
            <div style={{ fontSize: '0.75rem', color: '#4338CA', marginTop: '0.15rem' }}>
              Step {currentStepIndex + 1} of {totalSteps || 1}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
