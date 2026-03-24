// ============================================================
//  src/components/task-table.js
//  Live task list table with status badges
// ============================================================

function statusBadge(status) {
  const s = (status || 'UNKNOWN').toUpperCase();
  const map = {
    COMPLETED: 'completed',
    RUNNING: 'running',
    PENDING: 'pending',
    FAILED: 'failed',
  };
  const cls = map[s] || 'pending';
  return `<span class="badge badge-${cls}"><span class="badge-dot"></span>${s}</span>`;
}

function formatResult(result) {
  if (result === null || result === undefined) return '—';
  if (Array.isArray(result)) return `[${result.join(', ')}]`;
  return String(result);
}

export function renderTaskTable(container, tasks) {
  if (!tasks || tasks.length === 0) {
    container.innerHTML = `
      <div class="panel">
        <div class="panel-header">
          <div class="panel-title"><span class="icon">📋</span> Tarefas</div>
          <div style="font-size:0.72rem;color:var(--text-muted)">Auto-refresh: 3s</div>
        </div>
        <div class="empty-state">
          <div class="empty-icon">📭</div>
          <div class="empty-text">Nenhuma tarefa submetida ainda.<br>Use o formulário ao lado para enviar suas primeiras tarefas.</div>
        </div>
      </div>
    `;
    return;
  }

  const rows = tasks.map(t => `
    <tr>
      <td><span class="task-id">${t.task_id}</span></td>
      <td>${statusBadge(t.status)}</td>
      <td class="task-result">${formatResult(t.result)}</td>
    </tr>
  `).join('');

  const statusCounts = {};
  tasks.forEach(t => {
    const s = (t.status || 'UNKNOWN').toUpperCase();
    statusCounts[s] = (statusCounts[s] || 0) + 1;
  });

  container.innerHTML = `
    <div class="panel">
      <div class="panel-header">
        <div class="panel-title"><span class="icon">📋</span> Tarefas (${tasks.length})</div>
        <div style="font-size:0.72rem;color:var(--text-muted)">
          <span class="loading-spinner" style="margin-right:6px"></span>
          Auto-refresh: 3s
        </div>
      </div>
      <div class="panel-body-scroll">
        <table class="task-table">
          <thead>
            <tr>
              <th>Task ID</th>
              <th>Status</th>
              <th>Resultado</th>
            </tr>
          </thead>
          <tbody>
            ${rows}
          </tbody>
        </table>
      </div>
    </div>
  `;
}
