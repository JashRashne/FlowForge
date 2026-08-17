import React from 'react';
import { Terminal, Trash2 } from 'lucide-react';

export default function EventFeed({ events, onClear }) {
  return (
    <div className="terminal-window" style={{ marginTop: '1.5rem', maxHeight: '350px', display: 'flex', flexDirection: 'column' }}>
      <div className="terminal-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
          <span className="terminal-dot terminal-dot-red"></span>
          <span className="terminal-dot terminal-dot-yellow"></span>
          <span className="terminal-dot terminal-dot-green"></span>
          <span style={{ fontSize: '0.8rem', fontWeight: 800, color: '#A1A1AA', marginLeft: '0.4rem' }}>
            REDIS STREAM EVENT FEED // WS BROADCAST
          </span>
        </div>
        <button
          onClick={onClear}
          style={{ background: 'transparent', border: 'none', color: '#A1A1AA', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '0.3rem', fontSize: '0.75rem' }}
        >
          <Trash2 size={13} /> Clear Logs
        </button>
      </div>

      <div style={{ padding: '0.8rem', overflowY: 'auto', flex: 1, fontSize: '0.82rem', lineHeight: 1.6 }}>
        {events.length === 0 ? (
          <div style={{ color: '#71717A', fontStyle: 'italic', padding: '1rem' }}>
            Listening to WebSocket event stream... Run a workflow to view real-time state broadcasts.
          </div>
        ) : (
          events.map((evt, idx) => {
            const timeStr = new Date(evt.occurred_at || Date.now()).toLocaleTimeString();
            let eventColor = '#38BDF8';
            if (evt.event_type?.includes('succeeded')) eventColor = '#4ADE80';
            if (evt.event_type?.includes('failed') || evt.event_type?.includes('dlq')) eventColor = '#F87171';
            if (evt.event_type?.includes('ready') || evt.event_type?.includes('leased')) eventColor = '#FDE047';
            if (evt.event_type?.includes('reassigned')) eventColor = '#C084FC';

            return (
              <div key={idx} style={{ borderBottom: '1px solid #27272A', padding: '0.3rem 0', fontFamily: 'var(--font-mono)' }}>
                <span style={{ color: '#71717A', marginRight: '0.6rem' }}>[{timeStr}]</span>
                <span style={{ color: eventColor, fontWeight: 800, marginRight: '0.6rem' }}>
                  {evt.event_type}
                </span>
                {evt.task_run_id && (
                  <span style={{ color: '#E4E4E7', marginRight: '0.6rem' }}>
                    task:{evt.task_run_id.slice(0, 8)}...
                  </span>
                )}
                {evt.payload && (
                  <span style={{ color: '#A1A1AA' }}>
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
