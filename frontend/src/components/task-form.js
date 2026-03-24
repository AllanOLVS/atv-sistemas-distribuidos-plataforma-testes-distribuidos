// ============================================================
//  src/components/task-form.js
//  Task submission form (single + batch)
// ============================================================

import { submitTask, submitBatch } from '../api.js';
import { showToast } from './toast.js';

export function renderTaskForm(container, onTaskSubmitted) {
  container.innerHTML = `
    <div class="panel">
      <div class="panel-header">
        <div class="panel-title"><span class="icon">📤</span> Submeter Tarefa</div>
      </div>
      <div class="panel-body">
        <form id="task-form" class="task-form">
          <div class="form-group">
            <label for="operation">Operação</label>
            <select id="operation">
              <option value="sum">➕ Sum — Somar números</option>
              <option value="multiply">✖️ Multiply — Multiplicar números</option>
              <option value="sort">🔤 Sort — Ordenar lista</option>
              <option value="count">🔢 Count — Contar elementos</option>
              <option value="upper">🔠 Upper — Converter para maiúsculas</option>
            </select>
          </div>
          <div class="form-group">
            <label for="task-data">Dados</label>
            <textarea id="task-data" placeholder="10, 20, 30, 40" rows="2"></textarea>
            <div class="form-hint">Valores separados por vírgula</div>
          </div>
          <div class="form-actions">
            <button type="submit" class="btn btn-primary" id="submit-btn">
              Submeter Tarefa
            </button>
          </div>
        </form>

        <div class="batch-section">
          <div class="batch-title">⚡ Submissão em Lote</div>
          <div class="form-group">
            <label for="batch-count">Quantidade</label>
            <input type="number" id="batch-count" value="5" min="1" max="50" />
            <div class="form-hint">Gera tarefas aleatórias (1 a 50)</div>
          </div>
          <button class="btn btn-secondary btn-sm" id="batch-btn" style="width:100%">
            Submeter Lote
          </button>
        </div>
      </div>
    </div>
  `;

  // ── Single task submit ──
  const form = container.querySelector('#task-form');
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const btn = container.querySelector('#submit-btn');
    const op = container.querySelector('#operation').value;
    const rawData = container.querySelector('#task-data').value.trim();

    if (!rawData) {
      showToast('Informe os dados da tarefa', 'error');
      return;
    }

    const parts = rawData.split(',').map(s => s.trim()).filter(Boolean);
    let data;
    if (op === 'upper') {
      data = parts;
    } else {
      data = parts.map(s => {
        const n = Number(s);
        return isNaN(n) ? s : n;
      });
    }

    btn.disabled = true;
    btn.innerHTML = '<span class="loading-spinner"></span> Enviando…';

    try {
      const result = await submitTask(op, data);
      showToast(`Tarefa ${result.task_id} aceita!`, 'success');
      container.querySelector('#task-data').value = '';
      onTaskSubmitted();
    } catch (err) {
      showToast(`Erro: ${err.message}`, 'error');
    } finally {
      btn.disabled = false;
      btn.innerHTML = 'Submeter Tarefa';
    }
  });

  // ── Batch submit ──
  container.querySelector('#batch-btn').addEventListener('click', async () => {
    const btn = container.querySelector('#batch-btn');
    const count = parseInt(container.querySelector('#batch-count').value) || 5;

    const ops = ['sum', 'multiply', 'sort', 'count', 'upper'];
    const tasks = [];
    for (let i = 0; i < count; i++) {
      const op = ops[Math.floor(Math.random() * ops.length)];
      let data;
      if (op === 'upper') {
        data = Array.from({ length: 2 + Math.floor(Math.random() * 4) }, (_, j) => `texto${j}`);
      } else {
        data = Array.from({ length: 3 + Math.floor(Math.random() * 6) }, () => Math.floor(Math.random() * 100) + 1);
      }
      tasks.push({ operation: op, data });
    }

    btn.disabled = true;
    btn.innerHTML = '<span class="loading-spinner"></span> Enviando…';

    try {
      const result = await submitBatch(tasks);
      const accepted = result.results.filter(r => r.status === 'ACCEPTED').length;
      showToast(`${accepted}/${count} tarefas aceitas!`, 'success');
      onTaskSubmitted();
    } catch (err) {
      showToast(`Erro no lote: ${err.message}`, 'error');
    } finally {
      btn.disabled = false;
      btn.innerHTML = 'Submeter Lote';
    }
  });
}
