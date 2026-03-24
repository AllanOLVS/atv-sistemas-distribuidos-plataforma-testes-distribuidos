# ============================================================
#  api/api_server.py
#  REST API Bridge — Flask server that wraps the TCP client
#
#  Para executar:
#      pip install flask flask-cors
#      python -m api.api_server
#
#  Endpoints:
#      POST /api/login          — Login e obter token
#      POST /api/tasks          — Submeter tarefa
#      GET  /api/tasks          — Listar tarefas submetidas
#      GET  /api/tasks/<id>     — Status de uma tarefa
#      POST /api/tasks/batch    — Submissão em lote
#      GET  /api/system/status  — Status do sistema
# ============================================================

import hashlib
import json
import os
import socket
import sys
import threading
import time
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from flask import Flask, request, jsonify
from flask_cors import CORS

from common.config import (
    ORCHESTRATOR_HOST, ORCHESTRATOR_PORT,
    BACKUP_HOST, BACKUP_PORT,
    ORCHESTRATOR_ENDPOINTS,
    USERS, TOKEN_SECRET,
)
from common.lamport import LamportClock
from common.protocol import (
    MSG_LOGIN_OK, MSG_TASK_ACCEPTED, MSG_TASK_STATUS, MSG_ERROR,
    msg_login, msg_submit_task, msg_query_status,
    send_msg, recv_msg,
)

app = Flask(__name__)
CORS(app)

# ── Estado global da API ──────────────────────────────────────

_clock = LamportClock()
_sessions = {}          # token → {username, socket, lock, tasks[]}
_sessions_lock = threading.Lock()
_endpoint_lock = threading.Lock()
_active_endpoint = (ORCHESTRATOR_HOST, ORCHESTRATOR_PORT)
_all_endpoints = [
    _active_endpoint,
    (BACKUP_HOST, BACKUP_PORT),
]
for endpoint in ORCHESTRATOR_ENDPOINTS:
    if endpoint not in _all_endpoints:
        _all_endpoints.append(endpoint)


# ── Helpers ───────────────────────────────────────────────────

def _generate_token(username: str) -> str:
    raw = f"{username}:{TOKEN_SECRET}"
    return "tok_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def _get_session(token: str):
    """Retorna sessão ou None."""
    with _sessions_lock:
        return _sessions.get(token)


def _create_connection():
    """Cria nova conexão TCP ao orquestrador."""
    global _active_endpoint
    with _endpoint_lock:
        preferred = _active_endpoint
    candidates = [preferred] + [e for e in _all_endpoints if e != preferred]

    last_error = None
    for host, port in candidates:
        try:
            sock = socket.create_connection((host, port), timeout=10)
            with _endpoint_lock:
                _active_endpoint = (host, port)
            return sock
        except Exception as e:
            last_error = e

    raise ConnectionError(f"Nenhum endpoint ativo: {last_error}")


def _get_active_endpoint():
    with _endpoint_lock:
        return _active_endpoint


def _safe_send_recv(session, msg):
    """Envia msg e recebe resposta usando o socket da sessão."""
    lock = session["lock"]
    sock = session["socket"]
    with lock:
        try:
            send_msg(sock, msg)
            resp = recv_msg(sock)
            if resp:
                _clock.receive(resp.get("lamport", 0))
            return resp
        except (ConnectionResetError, BrokenPipeError, OSError):
            # Reconecta
            try:
                sock.close()
            except OSError:
                pass
            new_sock = _create_connection()
            session["socket"] = new_sock
            # Re-autentica
            login_msg = msg_login(
                sender="API",
                lamport=_clock.send(),
                username=session["username"],
                password=session.get("password", ""),
            )
            send_msg(new_sock, login_msg)
            recv_msg(new_sock)  # consume login response
            # Retry original message
            send_msg(new_sock, msg)
            return recv_msg(new_sock)


# ── Endpoints ─────────────────────────────────────────────────

@app.route("/api/login", methods=["POST"])
def login():
    """Autentica usuário e cria sessão persistente."""
    data = request.get_json(force=True)
    username = data.get("username", "").strip()
    password = data.get("password", "").strip()

    if not username or not password:
        return jsonify({"error": "Usuário e senha são obrigatórios"}), 400

    # Valida credenciais localmente primeiro
    if USERS.get(username) != password:
        return jsonify({"error": "Credenciais inválidas"}), 401

    # Conecta ao orquestrador
    try:
        sock = _create_connection()
        login_msg = msg_login(
            sender="API",
            lamport=_clock.send(),
            username=username,
            password=password,
        )
        send_msg(sock, login_msg)
        resp = recv_msg(sock)

        if resp and resp.get("type") == MSG_LOGIN_OK:
            _clock.receive(resp.get("lamport", 0))
            token = resp.get("token", "")

            with _sessions_lock:
                # Fecha sessão antiga se existir
                if token in _sessions:
                    try:
                        _sessions[token]["socket"].close()
                    except OSError:
                        pass

                _sessions[token] = {
                    "username": username,
                    "password": password,
                    "socket": sock,
                    "lock": threading.Lock(),
                    "tasks": [],
                    "created_at": time.time(),
                }

            return jsonify({
                "token": token,
                "username": username,
            })
        else:
            sock.close()
            return jsonify({"error": "Login falhou no orquestrador"}), 401

    except Exception as e:
        return jsonify({"error": f"Erro de conexão: {e}"}), 503


@app.route("/api/tasks", methods=["POST"])
def submit_task():
    """Submete uma nova tarefa."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    session = _get_session(token)
    if not session:
        return jsonify({"error": "Token inválido ou sessão expirada"}), 401

    data = request.get_json(force=True)
    operation = data.get("operation", "")
    task_data = data.get("data", [])

    if not operation:
        return jsonify({"error": "Operação é obrigatória"}), 400

    task_id = f"T-{uuid.uuid4().hex[:8].upper()}"
    payload = {"operation": operation, "data": task_data}

    msg = msg_submit_task(
        sender="API",
        lamport=_clock.send(),
        token=token,
        task_id=task_id,
        payload=payload,
    )

    resp = _safe_send_recv(session, msg)
    if resp and resp.get("type") == MSG_TASK_ACCEPTED:
        session["tasks"].append(task_id)
        return jsonify({
            "task_id": task_id,
            "status": "ACCEPTED",
            "operation": operation,
        })
    else:
        reason = resp.get("reason", "Erro desconhecido") if resp else "Sem resposta"
        return jsonify({"error": reason}), 400


@app.route("/api/tasks/batch", methods=["POST"])
def batch_submit():
    """Submete múltiplas tarefas."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    session = _get_session(token)
    if not session:
        return jsonify({"error": "Token inválido"}), 401

    data = request.get_json(force=True)
    tasks_input = data.get("tasks", [])

    results = []
    for task_input in tasks_input:
        operation = task_input.get("operation", "sum")
        task_data = task_input.get("data", [])
        task_id = f"T-{uuid.uuid4().hex[:8].upper()}"
        payload = {"operation": operation, "data": task_data}

        msg = msg_submit_task(
            sender="API",
            lamport=_clock.send(),
            token=token,
            task_id=task_id,
            payload=payload,
        )

        resp = _safe_send_recv(session, msg)
        if resp and resp.get("type") == MSG_TASK_ACCEPTED:
            session["tasks"].append(task_id)
            results.append({"task_id": task_id, "status": "ACCEPTED"})
        else:
            reason = resp.get("reason", "?") if resp else "sem resposta"
            results.append({"task_id": task_id, "status": "REJECTED", "reason": reason})

    return jsonify({"results": results})


@app.route("/api/tasks", methods=["GET"])
def list_tasks():
    """Lista status de todas as tarefas da sessão."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    session = _get_session(token)
    if not session:
        return jsonify({"error": "Token inválido"}), 401

    tasks = []
    for tid in session["tasks"]:
        msg = msg_query_status(
            sender="API",
            lamport=_clock.send(),
            token=token,
            task_id=tid,
        )

        resp = _safe_send_recv(session, msg)
        if resp and resp.get("type") == MSG_TASK_STATUS:
            tasks.append({
                "task_id": tid,
                "status": resp.get("status", "UNKNOWN"),
                "result": resp.get("result"),
            })
        elif resp and resp.get("type") == MSG_ERROR:
            tasks.append({
                "task_id": tid,
                "status": "ERROR",
                "result": resp.get("reason"),
            })
        else:
            tasks.append({
                "task_id": tid,
                "status": "UNKNOWN",
                "result": None,
            })

    return jsonify({"tasks": tasks})


@app.route("/api/tasks/<task_id>", methods=["GET"])
def get_task(task_id):
    """Status de uma tarefa específica."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    session = _get_session(token)
    if not session:
        return jsonify({"error": "Token inválido"}), 401

    msg = msg_query_status(
        sender="API",
        lamport=_clock.send(),
        token=token,
        task_id=task_id,
    )

    resp = _safe_send_recv(session, msg)
    if resp and resp.get("type") == MSG_TASK_STATUS:
        return jsonify({
            "task_id": task_id,
            "status": resp.get("status", "UNKNOWN"),
            "result": resp.get("result"),
        })
    elif resp and resp.get("type") == MSG_ERROR:
        return jsonify({"error": resp.get("reason", "?")}), 404
    else:
        return jsonify({"error": "Sem resposta do orquestrador"}), 503


@app.route("/api/system/status", methods=["GET"])
def system_status():
    """Retorna estatísticas gerais do sistema (não requer auth)."""
    # Lê do estado global via uma conexão temporária
    # Para simplificar, retornamos o que sabemos das sessões ativas
    active_sessions = 0
    total_tasks = 0
    with _sessions_lock:
        active_sessions = len(_sessions)
        for s in _sessions.values():
            total_tasks += len(s["tasks"])

    active_host, active_port = _get_active_endpoint()

    return jsonify({
        "orchestrator": {
            "host": active_host,
            "port": active_port,
            "status": "ONLINE",
        },
        "candidate_orchestrators": [
            {"host": host, "port": port} for host, port in _all_endpoints
        ],
        "active_sessions": active_sessions,
        "total_tasks_submitted": total_tasks,
        "users_available": list(USERS.keys()),
    })


# ════════════════════════════════════════════════════════════
#  Ponto de entrada
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    print("╔══════════════════════════════════════╗")
    print("║  API REST — Plataforma Distribuída   ║")
    print("║  http://localhost:8080                ║")
    print("╚══════════════════════════════════════╝")
    app.run(host="0.0.0.0", port=8080, debug=False)
