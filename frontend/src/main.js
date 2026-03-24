// ============================================================
//  src/main.js
//  Entry point — simple client-side routing
// ============================================================

import './style.css';
import { loadAuth, isAuthenticated } from './api.js';
import { renderLogin } from './pages/login.js';
import { renderDashboard } from './pages/dashboard.js';

const app = document.getElementById('app');

function route() {
  loadAuth();

  if (isAuthenticated()) {
    renderDashboard(app, () => route());
  } else {
    renderLogin(app, () => route());
  }
}

// Boot
route();
