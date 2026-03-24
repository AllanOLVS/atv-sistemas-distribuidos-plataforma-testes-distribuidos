# monitora workers (timeouts)
# ============================================================
#  orchestrator/heartbeat.py
#  Monitor de Heartbeat
#
#  Escuta heartbeats dos workers em uma porta UDP dedicada
#  e monitora timeouts para detectar falhas.
#
#  Fluxo:
#    Worker → envia MSG_HEARTBEAT a cada HEARTBEAT_INTERVAL s
#    Monitor → atualiza last_heartbeat no GlobalState
#    Monitor → verifica periodicamente se algum worker
#               ultrapassou HEARTBEAT_TIMEOUT sem bater
#    Monitor → ao detectar falha, notifica o Scheduler
# ============================================================
 
import socket
import threading
import time
 
from common.config   import (
    ORCHESTRATOR_HOST, ORCHESTRATOR_PORT,
    HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT,
)
from common.lamport  import LamportClock
from common.logger   import get_logger, log_worker_down, log_heartbeat
from common.protocol import (
    MSG_HEARTBEAT, msg_heartbeat_ack,
    decode, send_msg, recv_msg,
)
 
 
class HeartbeatMonitor:
    """
    Monitor de heartbeat que opera em duas threads:
 
    Thread 1 — receiver_thread:
        Recebe mensagens TCP de heartbeat dos workers.
        Atualiza o estado (last_heartbeat) e responde com ACK.
 
    Thread 2 — checker_thread:
        A cada HEARTBEAT_INTERVAL segundos verifica quais workers
        não mandaram heartbeat dentro do HEARTBEAT_TIMEOUT.
        Para cada worker morto, chama on_worker_dead().
    """
 
    def __init__(self, state, clock: LamportClock,
                 host: str = ORCHESTRATOR_HOST,
                 hb_port: int = None,
                 node_name: str = "ORCHESTRATOR"):
 
        self.state      = state
        self.clock      = clock
        self.logger     = get_logger(node_name, clock)
        self.host       = host
 
        # Porta de heartbeat = porta principal + 100
        # Ex: orquestrador em 5000 → heartbeat em 5100
        self.hb_port    = hb_port or (ORCHESTRATOR_PORT + 100)
 
        self._stop_event    = threading.Event()
        self._on_dead_cb    = None   # callback: fn(worker_id)
        self._receiver_thread = None
        self._checker_thread  = None
        self._server_sock     = None
 
    # Registro de callback 
 
    def on_worker_dead(self, callback) -> None:
        """
        Registra função chamada quando worker é detectado como morto.
        Assinatura: callback(worker_id: str)
        """
        self._on_dead_cb = callback
 
    # Controle 
 
    def start(self) -> None:
        self._setup_server()
 
        self._receiver_thread = threading.Thread(
            target=self._receive_loop,
            name="hb-receiver",
            daemon=True
        )
        self._checker_thread = threading.Thread(
            target=self._check_loop,
            name="hb-checker",
            daemon=True
        )
        self._receiver_thread.start()
        self._checker_thread.start()
        self.logger.info(
            f"HeartbeatMonitor escutando em {self.host}:{self.hb_port}"
        )
 
    def stop(self) -> None:
        self._stop_event.set()
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass
 
    # Thread 1: receber heartbeats 
 
    def _setup_server(self) -> None:
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.hb_port))
        self._server_sock.listen(20)
        self._server_sock.settimeout(2.0)
 
    def _receive_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                conn, addr = self._server_sock.accept()
                threading.Thread(
                    target=self._handle_heartbeat,
                    args=(conn, addr),
                    daemon=True
                ).start()
            except socket.timeout:
                continue
            except OSError:
                break
 
    def _handle_heartbeat(self, conn: socket.socket, addr: tuple) -> None:
        try:
            with conn:
                msg = recv_msg(conn)
                if not msg or msg.get("type") != MSG_HEARTBEAT:
                    return
 
                # Sincroniza relógio lógico
                ts = self.clock.receive(msg.get("lamport", 0))
 
                worker_id    = msg.get("worker_id", "")
                active_tasks = msg.get("active_tasks", [])
 
                # Registra worker automaticamente se for novo
                if not self.state.get_worker(worker_id):
                    # Prefere host informado pelo worker; fallback = IP da conexão
                    worker_host = msg.get("worker_host", addr[0])
                    worker_port = msg.get("worker_port", addr[1])
                    self.state.add_worker(worker_id, worker_host, worker_port)
                    self.logger.info(
                        f"Novo worker registrado: {worker_id} "
                        f"em {worker_host}:{worker_port}"
                    )
 
                self.state.update_heartbeat(worker_id, active_tasks)
                log_heartbeat(self.logger, worker_id, ts)
 
                # Responde com ACK
                ack = msg_heartbeat_ack(
                    sender="ORCHESTRATOR",
                    lamport=self.clock.send()
                )
                send_msg(conn, ack)
 
        except Exception as e:
            self.logger.debug(f"Erro ao processar heartbeat de {addr}: {e}")
 
    # Thread 2: verificar timeouts 
 
    def _check_loop(self) -> None:
        # Aguarda um pouco antes do primeiro check
        # para dar tempo dos workers se registrarem
        time.sleep(HEARTBEAT_TIMEOUT)
 
        while not self._stop_event.is_set():
            try:
                self._detect_dead_workers()
            except Exception as e:
                self.logger.error(f"Erro no check de heartbeat: {e}")
            time.sleep(HEARTBEAT_INTERVAL)
 
    def _detect_dead_workers(self) -> None:
        now     = time.time()
        alive   = self.state.get_alive_workers()
 
        for worker in alive:
            elapsed = now - worker.last_heartbeat
            if elapsed > HEARTBEAT_TIMEOUT:
                self.logger.warning(
                    f"Worker {worker.worker_id} sem heartbeat há "
                    f"{elapsed:.1f}s (timeout={HEARTBEAT_TIMEOUT}s)"
                )
                self.state.mark_worker_dead(worker.worker_id)
                ts = self.clock.tick()
                log_worker_down(self.logger, worker.worker_id, ts)
 
                # Notifica o scheduler para reatribuir tarefas
                if self._on_dead_cb:
                    try:
                        self._on_dead_cb(worker.worker_id)
                    except Exception as e:
                        self.logger.error(
                            f"Erro no callback on_worker_dead: {e}"
                        )