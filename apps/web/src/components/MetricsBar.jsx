import React from 'react';
import { Users, CheckCircle2, PlayCircle, AlertTriangle, Skull, Clock } from 'lucide-react';

export default function MetricsBar({ metrics }) {
  const cards = [
    {
      title: 'ACTIVE WORKERS',
      value: `${metrics.healthyWorkers}/${metrics.totalWorkers}`,
      sub: 'Stream Consumer Pool',
      icon: <Users size={16} color="#6366F1" />
    },
    {
      title: 'COMPLETED TASKS',
      value: metrics.completedTasks,
      sub: 'Effectively-Once Commits',
      icon: <CheckCircle2 size={16} color="#10B981" />
    },
    {
      title: 'IN-FLIGHT LEASES',
      value: metrics.runningTasks,
      sub: 'Fenced Active Ownership',
      icon: <PlayCircle size={16} color="#0EA5E9" />
    },
    {
      title: 'RETRIES DETECTED',
      value: metrics.retryCount,
      sub: 'Exponential Backoff',
      icon: <AlertTriangle size={16} color="#F59E0B" />
    },
    {
      title: 'DEAD LETTER QUEUE',
      value: metrics.dlqCount,
      sub: 'Poison Tasks Quarantined',
      icon: <Skull size={16} color={metrics.dlqCount > 0 ? '#EF4444' : '#94A3B8'} />
    }
  ];

  return (
    <div style={{
      display: 'grid',
      gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
      gap: '0.85rem',
      marginBottom: '1.25rem'
    }}>
      {cards.map((card, idx) => (
        <div
          key={idx}
          className="min-card"
          style={{
            padding: '0.85rem 1rem',
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'space-between'
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.35rem' }}>
            <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.7rem', fontWeight: 700, color: 'var(--text-muted)' }}>
              {card.title}
            </span>
            {card.icon}
          </div>
          <div style={{ fontSize: '1.5rem', fontWeight: 800, color: 'var(--text-primary)', lineHeight: 1.1 }}>
            {card.value}
          </div>
          <div style={{ fontSize: '0.72rem', color: 'var(--text-secondary)', marginTop: '0.3rem' }}>
            {card.sub}
          </div>
        </div>
      ))}
    </div>
  );
}
