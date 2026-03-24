// ============================================================
//  src/components/worker-panel.js
//  Worker status panel with heartbeat indicators
// ============================================================

export function renderWorkerPanel(container, workers) {
  if (!workers || workers.length === 0) {
    container.innerHTML = `
      <div class="panel">
        <div class="panel-header">
          <div class="panel-title"><span class="icon">⚙️</span> Workers</div>
        </div>
        <div class="empty-state">
          <div class="empty-icon">🔌</div>
          <div class="empty-text">Nenhum worker conectado.<br>Inicie workers com: <code>python -m worker.worker</code></div>
        </div>
      </div>
    `;
    return;
  }

  const cards = workers.map(w => {
    const isAlive = w.status === 'ALIVE';
    const statusClass = isAlive ? 'alive' : 'dead';
    const statusLabel = isAlive ? 'Online' : 'Offline';
    const elapsed = w.last_heartbeat
      ? `${Math.round(Date.now() / 1000 - w.last_heartbeat)}s atrás`
      : '—';

    return `
      <div class="worker-card">
        <div class="worker-card-header">
          <span class="worker-name">${w.worker_id}</span>
          <span class="worker-status-indicator ${statusClass}">
            <span class="worker-dot"></span>
            ${statusLabel}
          </span>
        </div>
        <div class="worker-details">
          <span class="worker-detail-item">📍 ${w.host}:${w.port}</span>
          <span class="worker-detail-item">📦 ${w.active_tasks?.length ?? 0} tarefas</span>
          <span class="worker-detail-item">💓 ${elapsed}</span>
        </div>
      </div>
    `;
  }).join('');

  container.innerHTML = `
    <div class="panel">
      <div class="panel-header">
        <div class="panel-title"><span class="icon">⚙️</span> Workers (${workers.length})</div>
      </div>
      <div class="worker-grid">
        ${cards}
      </div>
    </div>
  `;
}
