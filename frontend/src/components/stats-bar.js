// ============================================================
//  src/components/stats-bar.js
//  Statistics cards at the top of the dashboard
// ============================================================

export function renderStatsBar(container, stats) {
  container.innerHTML = `
    <div class="stat-card workers">
      <div class="stat-label">Workers Online</div>
      <div class="stat-value">${stats.workersAlive ?? '—'}</div>
      <div class="stat-detail">de ${stats.workersTotal ?? '?'} registrados</div>
    </div>
    <div class="stat-card pending">
      <div class="stat-label">Pendentes</div>
      <div class="stat-value">${stats.pending ?? 0}</div>
      <div class="stat-detail">aguardando processamento</div>
    </div>
    <div class="stat-card completed">
      <div class="stat-label">Concluídas</div>
      <div class="stat-value">${stats.completed ?? 0}</div>
      <div class="stat-detail">processadas com sucesso</div>
    </div>
    <div class="stat-card failed">
      <div class="stat-label">Falhas</div>
      <div class="stat-value">${stats.failed ?? 0}</div>
      <div class="stat-detail">erros no processamento</div>
    </div>
  `;
}
