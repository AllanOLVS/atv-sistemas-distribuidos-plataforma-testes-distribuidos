// ============================================================
//  src/api.js
//  HTTP client for the REST API bridge
// ============================================================

const envBase = import.meta.env.VITE_API_BASE_URL;
const API_BASE = (envBase && String(envBase).trim() ? String(envBase).trim() : '/api').replace(/\/+$/, '');

let _token = null;
let _username = null;

export function setAuth(token, username) {
  _token = token;
  _username = username;
  localStorage.setItem('auth_token', token);
  localStorage.setItem('auth_user', username);
}

export function loadAuth() {
  _token = localStorage.getItem('auth_token');
  _username = localStorage.getItem('auth_user');
  return { token: _token, username: _username };
}

export function clearAuth() {
  _token = null;
  _username = null;
  localStorage.removeItem('auth_token');
  localStorage.removeItem('auth_user');
}

export function getToken() { return _token; }
export function getUsername() { return _username; }
export function isAuthenticated() { return !!_token; }

async function apiFetch(path, options = {}) {
  const headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };
  if (_token) {
    headers['Authorization'] = `Bearer ${_token}`;
  }
  const resp = await fetch(`${API_BASE}${path}`, { ...options, headers });
  const text = await resp.text();
  let data = {};
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = { error: text };
    }
  }
  if (!resp.ok) {
    throw new Error(data.error || `HTTP ${resp.status}`);
  }
  return data;
}

// ── Auth ──────────────────────────────────────────────────

export async function login(username, password) {
  const data = await apiFetch('/login', {
    method: 'POST',
    body: JSON.stringify({ username, password }),
  });
  setAuth(data.token, data.username);
  return data;
}

// ── Tasks ─────────────────────────────────────────────────

export async function submitTask(operation, taskData) {
  return apiFetch('/tasks', {
    method: 'POST',
    body: JSON.stringify({ operation, data: taskData }),
  });
}

export async function submitBatch(tasks) {
  return apiFetch('/tasks/batch', {
    method: 'POST',
    body: JSON.stringify({ tasks }),
  });
}

export async function listTasks() {
  return apiFetch('/tasks');
}

export async function getTask(taskId) {
  return apiFetch(`/tasks/${taskId}`);
}

// ── System ────────────────────────────────────────────────

export async function getSystemStatus() {
  return apiFetch('/system/status');
}
