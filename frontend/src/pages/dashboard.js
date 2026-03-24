// ============================================================
//  src/pages/dashboard.js
//  Main dashboard page with auto-refresh
// ============================================================

import { getUsername, listTasks, getSystemStatus, clearAuth } from '../api.js';
import { renderStatsBar } from '../components/stats-bar.js';
import { renderTaskForm } from '../components/task-form.js';
import { renderTaskTable } from '../components/task-table.js';
import { renderWorkerPanel } from '../components/worker-panel.js';
import { showToast } from '../components/toast.js';

let refreshInterval = null;

export function renderDashboard(app, onLogout) {
  const username = getUsername();

  app.innerHTML = `
    <div class="dashboard">
      <div class="topbar">
        <div class="topbar-brand">
          <div class="topbar-logo">🌐</div>
          <span class="topbar-title">Plataforma Distribuída</span>
        </div>
        <div class="topbar-right">
          <div class="topbar-status">
            <span class="pulse"></span>
            <span id="active-orchestrator">Conectado</span>
          </div>
          <div class="topbar-user">
            <span class="topbar-user-avatar">${(username || '?')[0]}</span>
            <span>${username}</span>
          </div>
          <button class="btn btn-secondary btn-sm" id="logout-btn">Sair</button>
        </div>
      </div>

      <div class="dashboard-content">
        <div class="stats-bar" id="stats-container"></div>

        <div class="main-grid">
          <div id="form-container"></div>
          <div id="table-container"></div>
        </div>

        <div id="workers-container"></div>
      </div>
    </div>
  `;

  // ── Logout ──
  app.querySelector('#logout-btn').addEventListener('click', () => {
    clearAuth();
    if (refreshInterval) clearInterval(refreshInterval);
    showToast('Sessão encerrada.', 'info');
    onLogout();
  });

  // ── Render form ──
  const formContainer = app.querySelector('#form-container');
  renderTaskForm(formContainer, () => refreshData());

  // ── Initial load ──
  refreshData();

  // ── Auto-refresh every 3s ──
  refreshInterval = setInterval(refreshData, 3000);

  async function refreshData() {
    try {
      const [data, system] = await Promise.all([
        listTasks(),
        getSystemStatus().catch(() => null),
      ]);
      const tasks = data.tasks || [];

      // Compute stats from tasks
      const stats = {
        workersAlive: '—',
        workersTotal: '—',
        pending: tasks.filter(t => t.status === 'PENDING').length,
        completed: tasks.filter(t => t.status === 'COMPLETED').length,
        failed: tasks.filter(t => t.status === 'FAILED').length,
      };

      const running = tasks.filter(t => t.status === 'RUNNING').length;
      stats.workersAlive = running > 0 ? `${running}+` : '—';
      stats.workersTotal = '—';

      const activeNodeEl = app.querySelector('#active-orchestrator');
      if (activeNodeEl && system?.orchestrator) {
        const node = system.orchestrator;
        activeNodeEl.textContent = `Ativo: ${node.host}:${node.port}`;
      }

      renderStatsBar(app.querySelector('#stats-container'), stats);
      renderTaskTable(app.querySelector('#table-container'), tasks);

      // Workers: we don't have a direct endpoint for this yet,
      // so show based on task info
      renderWorkerPanel(app.querySelector('#workers-container'), []);

    } catch (err) {
      // If auth failed, redirect to login
      if (err.message.includes('Token') || err.message.includes('401')) {
        clearAuth();
        if (refreshInterval) clearInterval(refreshInterval);
        onLogout();
        return;
      }
      console.error('Refresh error:', err);
    }
  }
}
