import React from 'react';
import { Users, CheckCircle2, PlayCircle, AlertTriangle, Skull, Clock } from 'lucide-react';

export default function MetricsBar({ metrics }) {
  const cards = [
    {
      title: 'ACTIVE WORKERS',
      value: `${metrics.healthyWorkers}/${metrics.totalWorkers}`,
      sub: 'Valkey Stream Consumers',
      bg: 'var(--neo-yellow)',
      icon: <Users size={18} />
    },
    {
      title: 'TASKS COMPLETED',
      value: metrics.completedTasks,
      sub: 'Effectively-Once Commits',
      bg: 'var(--neo-lime)',
      icon: <CheckCircle2 size={18} />
    },
    {
      title: 'ACTIVE LEASES',
      value: metrics.runningTasks,
      sub: 'Fenced In-Flight',
      bg: 'var(--neo-purple)',
      icon: <PlayCircle size={18} />
    },
    {
      title: 'RETRIES DETECTED',
      value: metrics.retryCount,
      sub: 'Exponential Backoff',
      bg: 'var(--neo-orange)',
      icon: <AlertTriangle size={18} />
    },
    {
      title: 'DEAD LETTER QUEUE',
      value: metrics.dlqCount,
      sub: 'Poison Tasks Quarantined',
      bg: metrics.dlqCount > 0 ? 'var(--neo-coral)' : '#FFFFFF',
      color: metrics.dlqCount > 0 ? '#FFFFFF' : '#000000',
      icon: <Skull size={18} />
    },
    {
      title: 'SCHEDULER TICK',
      value: `${metrics.schedulerLatencyMs} ms`,
      sub: 'CTE Dependency Sweep',
      bg: 'var(--neo-cyan)',
      icon: <Clock size={18} />
    }
  ];

  return (
    <div style={{
      display: 'grid',
      gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
      gap: '1rem',
      marginBottom: '1.5rem'
    }}>
      {cards.map((card, idx) => (
        <div
          key={idx}
          className="neo-card"
          style={{
            background: card.bg,
            color: card.color || '#000000',
            padding: '1rem',
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'space-between'
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.4rem' }}>
            <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.72rem', fontWeight: 800, letterSpacing: '0.05em' }}>
              {card.title}
            </span>
            {card.icon}
          </div>
          <div style={{ fontFamily: 'var(--font-heading)', fontSize: '2rem', fontWeight: 900, lineHeight: 1 }}>
            {card.value}
          </div>
          <div style={{ fontSize: '0.75rem', fontWeight: 600, marginTop: '0.4rem', opacity: 0.85 }}>
            {card.sub}
          </div>
        </div>
      ))}
    </div>
  );
}
