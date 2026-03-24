# lógica principal, aceita conexões
# ============================================================
#  orchestrator/orchestrator.py
#  Orquestrador Principal
#
#  Para executar:
#      python -m orchestrator.orchestrator
#
#  Responsabilidades:
#    1. Escuta conexões TCP de clientes (login + submissão de tarefas)
#    2. Autentica via token simples
#    3. Enfileira tarefas no GlobalState
#    4. Delega distribuição ao RoundRobinScheduler
#    5. Recebe resultados dos workers
#    6. Sincroniza estado com o Backup via UDP Multicast
#    7. Monitora heartbeat e dispara reatribuição ao detectar falha
# ============================================================
 
import hashlib
import socket
import threading
import time
import uuid
 
from common.config  import (
    ORCHESTRATOR_HOST, ORCHESTRATOR_PORT,
    USERS, TOKEN_SECRET,
)
from common.lamport import LamportClock
from common.logger  import (
    get_logger,
    log_task_submitted, log_task_completed, log_task_failed,
)
from common.protocol import (
    MSG_LOGIN, MSG_SUBMIT_TASK, MSG_QUERY_STATUS, MSG_TASK_RESULT,
    MSG_ACK, _base,
    TASK_COMPLETED, TASK_FAILED,
    msg_login_ok, msg_login_fail,
    msg_task_accepted, msg_task_rejected,
    msg_task_status, msg_error,
    send_msg, recv_msg,
)
from orchestrator.state     import GlobalState
from orchestrator.scheduler import RoundRobinScheduler
from orchestrator.heartbeat import HeartbeatMonitor
from orchestrator.multicast import MulticastSender
 
 
# ════════════════════════════════════════════════════════════
#  Autenticação
# ════════════════════════════════════════════════════════════
 
def _generate_token(username: str) -> str:
    """Gera um token simples baseado em username + segredo."""
    raw = f"{username}:{TOKEN_SECRET}"
    return "tok_" + hashlib.sha256(raw.encode()).hexdigest()[:16]
 
 
def _validate_token(token: str) -> str | None:
    """
    Valida token e retorna o username correspondente.
    Retorna None se inválido.
    """
    for username in USERS:
        if _generate_token(username) == token:
            return username
    return None
 
 
# ════════════════════════════════════════════════════════════
#  Orquestrador Principal
# ════════════════════════════════════════════════════════════
 
class Orchestrator:
    """
    Servidor TCP central do sistema distribuído.
 
    Cada cliente conecta, faz login, recebe um token
    e usa esse token para submeter tarefas e consultar status.
    Workers conectam para entregar resultados.
    """
 
    def __init__(self,
                 host: str = ORCHESTRATOR_HOST,
                 port: int = ORCHESTRATOR_PORT):
 
        self.host  = host
        self.port  = port
 
        # Componentes principais
        self.clock     = LamportClock()
        self.state     = GlobalState()
        self.logger    = get_logger("ORCHESTRATOR", self.clock)
 
        self.scheduler = RoundRobinScheduler(
            state     = self.state,
            clock     = self.clock,
            node_name = "ORCHESTRATOR",
        )
        self.hb_monitor = HeartbeatMonitor(
            state     = self.state,
            clock     = self.clock,
            host      = self.host,
            node_name = "ORCHESTRATOR",
        )
        self.mc_sender = MulticastSender(
            state     = self.state,
            clock     = self.clock,
            node_name = "ORCHESTRATOR",
        )
 
        self._server_sock = None
        self._stop_event  = threading.Event()
 
        # Callback: worker morreu → reatribuir tarefas
        self.hb_monitor.on_worker_dead(self._on_worker_dead)
 
    # Inicialização 
 
    def start(self) -> None:
        self._setup_server()
 
        # Inicia subsistemas em threads separadas
        self.hb_monitor.start()
        self.scheduler.start()
        self.mc_sender.start()
 
        self.logger.info(
            f"Orquestrador Principal iniciado em {self.host}:{self.port}"
        )
        self._accept_loop()
 
    def stop(self) -> None:
        self.logger.info("Encerrando orquestrador...")
        self._stop_event.set()
        self.scheduler.stop()
        self.hb_monitor.stop()
        self.mc_sender.stop()
        if self._server_sock:
            self._server_sock.close()
 
    def _setup_server(self) -> None:
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(50)
        self._server_sock.settimeout(2.0)
 
    # Loop de aceitação de conexões 
 
    def _accept_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                conn, addr = self._server_sock.accept()
                threading.Thread(
                    target=self._handle_connection,
                    args=(conn, addr),
                    daemon=True
                ).start()
            except socket.timeout:
                continue
            except OSError:
                break
 
    # Tratamento de conexão 
 
    def _handle_connection(self, conn: socket.socket, addr: tuple) -> None:
        """
        Cada conexão é tratada em thread própria.
        Suporta múltiplas mensagens por conexão (sessão).
        """
        client_label = f"{addr[0]}:{addr[1]}"
        self.logger.debug(f"Nova conexão de {client_label}")
        token_username = None   # autenticado durante a sessão
 
        try:
            with conn:
                while not self._stop_event.is_set():
                    msg = recv_msg(conn)
                    if msg is None:
                        break   # cliente encerrou conexão
 
                    # Sincroniza relógio lógico
                    self.clock.receive(msg.get("lamport", 0))
                    msg_type = msg.get("type", "")
 
                    if msg_type == MSG_LOGIN:
                        token_username = self._handle_login(conn, msg)
 
                    elif msg_type == MSG_SUBMIT_TASK:
                        self._handle_submit(conn, msg, token_username)
 
                    elif msg_type == MSG_QUERY_STATUS:
                        self._handle_query(conn, msg, token_username)
 
                    elif msg_type == MSG_TASK_RESULT:
                        self._handle_result(conn, msg)
 
                    else:
                        send_msg(conn, msg_error(
                            "ORCHESTRATOR", self.clock.send(),
                            f"Tipo de mensagem desconhecido: {msg_type}"
                        ))
 
        except Exception as e:
            self.logger.debug(f"Conexão {client_label} encerrada: {e}")
 
    # Handlers de mensagem 
 
    def _handle_login(self, conn, msg) -> str | None:
        """Autentica usuário e retorna token. Retorna None se falhar."""
        username = msg.get("username", "")
        password = msg.get("password", "")
        ts       = self.clock.tick()
 
        if USERS.get(username) == password:
            token = _generate_token(username)
            send_msg(conn, msg_login_ok("ORCHESTRATOR", self.clock.send(), token))
            self.logger.info(
                f"[LOGIN_OK] user={username} token={token[:12]}… lamport={ts}"
            )
            return username
        else:
            send_msg(conn, msg_login_fail("ORCHESTRATOR", self.clock.send()))
            self.logger.warning(
                f"[LOGIN_FAIL] user={username} lamport={ts}"
            )
            return None
 
    def _handle_submit(self, conn, msg, username: str | None) -> None:
        """Valida token e enfileira tarefa."""
        token   = msg.get("token", "")
        task_id = msg.get("task_id") or f"T-{uuid.uuid4().hex[:8].upper()}"
        payload = msg.get("payload", {})
 
        # Valida token
        auth_user = _validate_token(token)
        if not auth_user:
            send_msg(conn, msg_task_rejected(
                "ORCHESTRATOR", self.clock.send(),
                task_id, "Token inválido ou expirado"
            ))
            self.logger.warning(
                f"[SUBMIT_REJECTED] task={task_id} reason=invalid_token"
            )
            return
 
        # Enfileira no estado global
        self.state.add_task(task_id, auth_user, payload)
        ts = self.clock.send()
        log_task_submitted(self.logger, task_id, auth_user, ts)
 
        # Confirma ao cliente
        send_msg(conn, msg_task_accepted("ORCHESTRATOR", ts, task_id))
 
        # Força sincronização com backup imediatamente
        self.mc_sender.send_now()
 
    def _handle_query(self, conn, msg, username: str | None) -> None:
        """Retorna o status atual de uma tarefa."""
        token   = msg.get("token", "")
        task_id = msg.get("task_id", "")
 
        if not _validate_token(token):
            send_msg(conn, msg_error(
                "ORCHESTRATOR", self.clock.send(), "Token inválido"
            ))
            return
 
        task = self.state.get_task(task_id)
        if not task:
            send_msg(conn, msg_error(
                "ORCHESTRATOR", self.clock.send(),
                f"Tarefa {task_id} não encontrada"
            ))
            return
 
        send_msg(conn, msg_task_status(
            sender  = "ORCHESTRATOR",
            lamport = self.clock.send(),
            task_id = task_id,
            status  = task.status,
            result  = task.result if task.status == TASK_COMPLETED else None,
        ))
 
    def _handle_result(self, conn, msg) -> None:
        """Recebe resultado de um worker e atualiza o estado."""
        task_id   = msg.get("task_id", "")
        status    = msg.get("status", "")
        result    = msg.get("result")
        error     = msg.get("error", "")
        worker_id = msg.get("sender", "")
        ts        = self.clock.value
 
        if status == TASK_COMPLETED:
            self.state.complete_task(task_id, result)
            log_task_completed(self.logger, task_id, worker_id, ts)
        elif status == TASK_FAILED:
            self.state.fail_task(task_id, error)
            log_task_failed(self.logger, task_id, worker_id, error, ts)
 
        # Confirma recebimento ao worker
        send_msg(conn, _base(MSG_ACK, "ORCHESTRATOR", self.clock.send()))
 
        # Força sincronização com backup
        self.mc_sender.send_now()
 
        self.logger.debug(self.state.summary())
 
    # Callback: worker morreu 
 
    def _on_worker_dead(self, worker_id: str) -> None:
        """
        Chamado pelo HeartbeatMonitor ao detectar falha.
        Delega a reatribuição das tarefas ao Scheduler.
        """
        self.logger.warning(f"Worker {worker_id} declarado morto — reatribuindo tarefas")
        self.scheduler.reassign_tasks_of_dead_worker(worker_id)
        self.mc_sender.send_now()
 
 
# ════════════════════════════════════════════════════════════
#  Ponto de entrada
# ════════════════════════════════════════════════════════════
 
if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    ))
 
    os.makedirs("logs", exist_ok=True)
    orch = Orchestrator()
    try:
        orch.start()
    except KeyboardInterrupt:
        orch.stop()
        print("\nOrquestrador encerrado.")
 