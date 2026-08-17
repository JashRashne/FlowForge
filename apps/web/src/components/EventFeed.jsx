import React from 'react';
import { Terminal, Trash2, ShieldCheck, Activity } from 'lucide-react';

export default function EventFeed({ events, onClear }) {
  return (
    <div className="min-card" style={{ marginTop: '1.25rem', overflow: 'hidden', background: '#FFFFFF' }}>
      <div style={{
        padding: '0.75rem 1.25rem',
        borderBottom: '1px solid var(--border-subtle)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        background: '#FAFBFD'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <Terminal size={15} color="#6366F1" />
          <span style={{ fontSize: '0.8rem', fontWeight: 700, fontFamily: 'var(--font-mono)', color: 'var(--text-secondary)' }}>
            LIVE STREAM EVENT LOG (REDIS STREAM FAN-OUT)
          </span>
        </div>

        <button onClick={onClear} className="btn btn-sm btn-outline" style={{ padding: '0.2rem 0.5rem', fontSize: '0.72rem' }}>
          <Trash2 size={12} /> Clear
        </button>
      </div>

      <div style={{
        maxHeight: '220px',
        overflowY: 'auto',
        padding: '0.5rem 1.25rem',
        fontFamily: 'var(--font-mono)',
        fontSize: '0.78rem',
        lineHeight: 1.6
      }}>
        {events.length === 0 ? (
          <div style={{ color: 'var(--text-muted)', padding: '1rem 0', fontStyle: 'italic' }}>
            Awaiting events... Click a workflow above to watch real-time state broadcasts.
          </div>
        ) : (
          events.map((evt, idx) => {
            const timeStr = new Date(evt.occurred_at || Date.now()).toLocaleTimeString();
            let eventBadgeClass = 'status-BLOCKED';
            if (evt.event_type?.includes('succeeded')) eventBadgeClass = 'status-SUCCEEDED';
            if (evt.event_type?.includes('ready')) eventBadgeClass = 'status-READY';
            if (evt.event_type?.includes('leased')) eventBadgeClass = 'status-LEASED';
            if (evt.event_type?.includes('started')) eventBadgeClass = 'status-RUNNING';
            if (evt.event_type?.includes('failed') || evt.event_type?.includes('dlq')) eventBadgeClass = 'status-FAILED';

            return (
              <div key={idx} style={{ padding: '0.3rem 0', borderBottom: '1px solid #F1F5F9', display: 'flex', alignItems: 'center', gap: '0.6rem', flexWrap: 'wrap' }}>
                <span style={{ color: 'var(--text-muted)' }}>[{timeStr}]</span>
                <span className={`status-pill ${eventBadgeClass}`} style={{ fontSize: '0.68rem', padding: '0.1rem 0.4rem' }}>
                  {evt.event_type}
                </span>
                {evt.task_run_id && (
                  <span style={{ color: 'var(--text-secondary)', fontWeight: 600 }}>
                    task:{evt.task_run_id.slice(0, 8)}
                  </span>
                )}
                {evt.payload && (
                  <span style={{ color: 'var(--text-muted)' }}>
                    {JSON.stringify(evt.payload)}
                  </span>
                )}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
