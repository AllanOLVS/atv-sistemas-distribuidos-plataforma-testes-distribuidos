// ============================================================
//  src/pages/login.js
//  Login page
// ============================================================

import { login } from '../api.js';
import { showToast } from '../components/toast.js';

export function renderLogin(app, onLoginSuccess) {
  app.innerHTML = `
    <div class="login-page">
      <div class="login-container">
        <div class="login-header">
          <div class="login-logo">🌐</div>
          <h1 class="login-title">Plataforma Distribuída</h1>
          <p class="login-subtitle">Sistema de processamento colaborativo de tarefas</p>
        </div>

        <div class="login-card">
          <div class="login-error" id="login-error"></div>

          <form id="login-form">
            <div class="form-group">
              <label for="username">Usuário</label>
              <input type="text" id="username" placeholder="Digite seu usuário" autocomplete="username" autofocus />
            </div>
            <div class="form-group">
              <label for="password">Senha</label>
              <input type="password" id="password" placeholder="Digite sua senha" autocomplete="current-password" />
            </div>
            <button type="submit" class="btn btn-primary" id="login-btn">
              Entrar
            </button>
          </form>

          <div class="login-users-hint">
            <strong>Usuários disponíveis:</strong><br>
            <code>alice</code> / <code>senha123</code> &nbsp;·&nbsp;
            <code>bob</code> / <code>abc456</code> &nbsp;·&nbsp;
            <code>carol</code> / <code>qwerty</code>
          </div>
        </div>
      </div>
    </div>
  `;

  const form = app.querySelector('#login-form');
  const errorEl = app.querySelector('#login-error');
  const btn = app.querySelector('#login-btn');

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const username = app.querySelector('#username').value.trim();
    const password = app.querySelector('#password').value.trim();

    if (!username || !password) {
      errorEl.textContent = 'Usuário e senha são obrigatórios.';
      errorEl.classList.add('visible');
      return;
    }

    errorEl.classList.remove('visible');
    btn.disabled = true;
    btn.innerHTML = '<span class="loading-spinner"></span> Entrando…';

    try {
      await login(username, password);
      showToast(`Bem-vindo, ${username}!`, 'success');
      onLoginSuccess();
    } catch (err) {
      errorEl.textContent = err.message || 'Erro ao fazer login.';
      errorEl.classList.add('visible');
    } finally {
      btn.disabled = false;
      btn.innerHTML = 'Entrar';
    }
  });
}
