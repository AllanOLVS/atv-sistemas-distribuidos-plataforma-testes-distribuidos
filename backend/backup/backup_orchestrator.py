# ============================================================
#  backup/backup_orchestrator.py
#  Orquestrador Secundário (Backup) com Failover
#
#  Para executar:
#      python -m backup.backup_orchestrator
#
#  Responsabilidades:
#    1. Escuta snapshots do principal via UDP Multicast
#    2. Mantém cópia sincronizada do estado global
#    3. Detecta falha do principal (timeout de sync)
#    4. Ao detectar falha, assume como orquestrador:
#       - Abre servidor TCP para clientes
#       - Inicia HeartbeatMonitor e Scheduler
#       - Loga evento FAILOVER
# ============================================================

import hashlib
import socket
import sys
import os
import threading
import time
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.config import (
    BACKUP_HOST, BACKUP_PORT,
    ORCHESTRATOR_HOST, ORCHESTRATOR_PORT,
    FAILOVER_RECOVERY_GRACE_SECONDS,
    USERS, TOKEN_SECRET,
)
from common.lamport  import LamportClock
from common.logger   import (
    get_logger, log_failover,
    log_task_submitted, log_task_completed, log_task_failed,
    log_task_recovery_marked, log_task_late_result_ignored,
)
from common.protocol import (
    MSG_LOGIN, MSG_SUBMIT_TASK, MSG_QUERY_STATUS, MSG_TASK_RESULT,
    MSG_ACK,
    TASK_COMPLETED, TASK_FAILED,
    msg_login_ok, msg_login_fail,
    msg_task_accepted, msg_task_rejected,
    msg_task_status, msg_error,
    send_msg, recv_msg, _base,
)
from orchestrator.state     import GlobalState
from orchestrator.scheduler import RoundRobinScheduler
from orchestrator.heartbeat import HeartbeatMonitor
from orchestrator.multicast import MulticastSender
from backup.sync            import SyncMonitor


# ── Autenticação (mesma lógica do principal) ─────────────────

def _generate_token(username: str) -> str:
    raw = f"{username}:{TOKEN_SECRET}"
    return "tok_" + hashlib.sha256(raw.encode()).hexdigest()[:16]

def _validate_token(token: str) -> str | None:
    for username in USERS:
        if _generate_token(username) == token:
            return username
    return None


class BackupOrchestrator:
    """
    Orquestrador secundário.

    Em modo standby:
        - Recebe snapshots de estado via multicast
        - Monitora se o principal está ativo

    Após failover:
        - Abre servidor TCP para atender clientes e workers
        - Inicia scheduler e heartbeat monitor
        - Funciona identicamente ao orquestrador principal
    """

    def __init__(self,
                 host: str = BACKUP_HOST,
                 port: int = BACKUP_PORT):
        self.host = host
        self.port = port

        self.clock  = LamportClock()
        self.state  = GlobalState()
        self.logger = get_logger("BACKUP", self.clock)

        self._sync_monitor = SyncMonitor(
            state=self.state,
            clock=self.clock,
            node_name="BACKUP",
        )

        self._is_primary   = False
        self._stop_event   = threading.Event()
        self._server_sock  = None

    # ── Controle ─────────────────────────────────────────────

    def start(self) -> None:
        os.makedirs("logs", exist_ok=True)

        # Registra callback de failover
        self._sync_monitor.on_primary_dead(self._do_failover)
        self._sync_monitor.start()

        self.logger.info(
            f"Backup Orchestrator em standby. "
            f"Escutando multicast para sincronização..."
        )

        # Fica em standby até ser promovido ou parado
        try:
            while not self._stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self) -> None:
        self.logger.info("Encerrando backup...")
        self._stop_event.set()
        self._sync_monitor.stop()
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass

    # ── Failover ─────────────────────────────────────────────

    def _do_failover(self) -> None:
        """Assume o papel de orquestrador principal."""
        ts = self.clock.tick()
        log_failover(self.logger, "ORCHESTRATOR-PRIMARY", "BACKUP", ts)

        self.logger.critical(
            "═══════════════════════════════════════════\n"
            "   🔄 FAILOVER: Backup assumindo como PRIMÁRIO!\n"
            "═══════════════════════════════════════════"
        )

        self._is_primary = True
        # Preferencia por transparência: assumir a porta do primário.
        # Se ainda estiver ocupada, usa a porta de backup.
        promoted_port = ORCHESTRATOR_PORT
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            test_sock.bind((self.host, promoted_port))
            self.port = promoted_port
        except OSError:
            self.port = BACKUP_PORT
            self.logger.warning(
                f"Porta {ORCHESTRATOR_PORT} indisponível; mantendo backup em {self.port}"
            )
        finally:
            try:
                test_sock.close()
            except OSError:
                pass

        marked = self.state.mark_running_tasks_for_recovery(
            FAILOVER_RECOVERY_GRACE_SECONDS
        )
        for item in marked:
            log_task_recovery_marked(
                self.logger,
                item["task_id"],
                item["worker_id"],
                item["deadline_at"],
                self.clock.tick(),
            )

        # Inicia subsistemas (mesmos do orquestrador principal)
        self.scheduler = RoundRobinScheduler(
            state=self.state,
            clock=self.clock,
            node_name="BACKUP-PRIMARY",
        )
        self.hb_monitor = HeartbeatMonitor(
            state=self.state,
            clock=self.clock,
            host=self.host,
            hb_port=self.port + 100,
            node_name="BACKUP-PRIMARY",
        )
        self.mc_sender = MulticastSender(
            state=self.state,
            clock=self.clock,
            node_name="BACKUP-PRIMARY",
        )

        # Callback de worker morto
        self.hb_monitor.on_worker_dead(self._on_worker_dead)

        # Inicia subsistemas
        self.hb_monitor.start()
        self.scheduler.start()
        self.mc_sender.start()

        # Abre servidor TCP
        threading.Thread(
            target=self._run_server,
            name="backup-server",
            daemon=True,
        ).start()

        self.logger.info(
            f"Backup agora escutando clientes em {self.host}:{self.port}"
        )

    def _on_worker_dead(self, worker_id: str) -> None:
        self.logger.warning(
            f"Worker {worker_id} declarado morto — reatribuindo tarefas"
        )
        self.scheduler.reassign_tasks_of_dead_worker(worker_id)
        self.mc_sender.send_now()

    # ── Servidor TCP (após failover) ─────────────────────────

    def _run_server(self) -> None:
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(50)
        self._server_sock.settimeout(2.0)

        while not self._stop_event.is_set():
            try:
                conn, addr = self._server_sock.accept()
                threading.Thread(
                    target=self._handle_connection,
                    args=(conn, addr),
                    daemon=True,
                ).start()
            except socket.timeout:
                continue
            except OSError:
                break

    def _handle_connection(self, conn: socket.socket, addr: tuple) -> None:
        """Mesma lógica do orquestrador principal."""
        try:
            with conn:
                while not self._stop_event.is_set():
                    msg = recv_msg(conn)
                    if msg is None:
                        break

                    self.clock.receive(msg.get("lamport", 0))
                    msg_type = msg.get("type", "")

                    if msg_type == MSG_LOGIN:
                        self._handle_login(conn, msg)
                    elif msg_type == MSG_SUBMIT_TASK:
                        self._handle_submit(conn, msg)
                    elif msg_type == MSG_QUERY_STATUS:
                        self._handle_query(conn, msg)
                    elif msg_type == MSG_TASK_RESULT:
                        self._handle_result(conn, msg)
                    else:
                        send_msg(conn, msg_error(
                            "BACKUP-PRIMARY", self.clock.send(),
                            f"Tipo desconhecido: {msg_type}"
                        ))
        except Exception:
            pass

    def _handle_login(self, conn, msg) -> None:
        username = msg.get("username", "")
        password = msg.get("password", "")
        ts = self.clock.tick()

        if USERS.get(username) == password:
            token = _generate_token(username)
            send_msg(conn, msg_login_ok("BACKUP-PRIMARY", self.clock.send(), token))
            self.logger.info(f"[LOGIN_OK] user={username} lamport={ts}")
        else:
            send_msg(conn, msg_login_fail("BACKUP-PRIMARY", self.clock.send()))
            self.logger.warning(f"[LOGIN_FAIL] user={username} lamport={ts}")

    def _handle_submit(self, conn, msg) -> None:
        token   = msg.get("token", "")
        task_id = msg.get("task_id") or f"T-{uuid.uuid4().hex[:8].upper()}"
        payload = msg.get("payload", {})

        auth_user = _validate_token(token)
        if not auth_user:
            send_msg(conn, msg_task_rejected(
                "BACKUP-PRIMARY", self.clock.send(),
                task_id, "Token inválido"
            ))
            return

        self.state.add_task(task_id, auth_user, payload)
        ts = self.clock.send()
        log_task_submitted(self.logger, task_id, auth_user, ts)
        send_msg(conn, msg_task_accepted("BACKUP-PRIMARY", ts, task_id))
        self.mc_sender.send_now()

    def _handle_query(self, conn, msg) -> None:
        token   = msg.get("token", "")
        task_id = msg.get("task_id", "")

        if not _validate_token(token):
            send_msg(conn, msg_error(
                "BACKUP-PRIMARY", self.clock.send(), "Token inválido"
            ))
            return

        task = self.state.get_task(task_id)
        if not task:
            send_msg(conn, msg_error(
                "BACKUP-PRIMARY", self.clock.send(),
                f"Tarefa {task_id} não encontrada"
            ))
            return

        send_msg(conn, msg_task_status(
            sender="BACKUP-PRIMARY",
            lamport=self.clock.send(),
            task_id=task_id,
            status=task.status,
            result=task.result if task.status == TASK_COMPLETED else None,
        ))

    def _handle_result(self, conn, msg) -> None:
        task_id   = msg.get("task_id", "")
        status    = msg.get("status", "")
        result    = msg.get("result")
        error     = msg.get("error", "")
        worker_id = msg.get("sender", "")
        ts        = self.clock.value

        can_apply, reason = self.state.can_accept_task_result(task_id, worker_id)
        if not can_apply:
            log_task_late_result_ignored(self.logger, task_id, worker_id, reason, ts)
            send_msg(conn, _base(MSG_ACK, "BACKUP-PRIMARY", self.clock.send()))
            return

        if status == TASK_COMPLETED:
            self.state.complete_task(task_id, result)
            log_task_completed(self.logger, task_id, worker_id, ts)
        elif status == TASK_FAILED:
            self.state.fail_task(task_id, error)
            log_task_failed(self.logger, task_id, worker_id, error, ts)

        send_msg(conn, _base(MSG_ACK, "BACKUP-PRIMARY", self.clock.send()))
        self.mc_sender.send_now()


# ════════════════════════════════════════════════════════════
#  Ponto de entrada
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    backup = BackupOrchestrator()
    try:
        backup.start()
    except KeyboardInterrupt:
        backup.stop()
        print("\nBackup encerrado.")