# ============================================================
#  worker/worker.py
#  Nó de Processamento (Worker)
#
#  Para executar:
#      python -m worker.worker --id worker-1 --index 0
#      python -m worker.worker --id worker-2 --index 1
#      python -m worker.worker --id worker-3 --index 2
#
#  Responsabilidades:
#    1. Escuta porta TCP (WORKER_BASE_PORT + index) para receber
#       tarefas do orquestrador (MSG_ASSIGN_TASK)
#    2. Executa tarefas em threads separadas via TaskRunner
#    3. Envia MSG_TASK_RESULT ao orquestrador ao concluir/falhar
#    4. Envia heartbeat periódico ao HeartbeatMonitor
#    5. Pode simular crash para testar redistribuição
# ============================================================

import argparse
import socket
import sys
import os
import threading
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.config import (
    ORCHESTRATOR_HOST, ORCHESTRATOR_PORT,
    BACKUP_HOST, BACKUP_PORT,
    ORCHESTRATOR_ENDPOINTS,
    WORKER_BASE_PORT,
    HEARTBEAT_INTERVAL,
)
from common.lamport  import LamportClock
from common.logger   import get_logger, log_task_completed, log_task_failed
from common.protocol import (
    MSG_ASSIGN_TASK, MSG_ACK,
    TASK_COMPLETED, TASK_FAILED,
    msg_heartbeat, msg_task_result,
    send_msg, recv_msg,
    _base,
)
from worker.task_runner import TaskRunner


class Worker:
    """
    Nó de processamento do sistema distribuído.

    Arquitetura de threads:
        - Main thread: aceita conexões TCP do scheduler
        - Heartbeat thread: envia heartbeat ao orquestrador
        - Task threads: uma por tarefa em execução
    """

    def __init__(self, worker_id: str, index: int,
                 host: str = "127.0.0.1",
                 simulate_crash_after: int = 0):

        self.worker_id = worker_id
        self.index     = index
        self.host      = host
        self.port      = WORKER_BASE_PORT + index

        self.clock  = LamportClock()
        self.logger = get_logger(worker_id.upper(), self.clock)
        self.runner = TaskRunner()

        self._active_tasks: list[str] = []
        self._tasks_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._server_sock = None

        # Simulação de crash
        self._simulate_crash_after = simulate_crash_after
        self._tasks_completed = 0
        self._orchestrator_endpoints = list(ORCHESTRATOR_ENDPOINTS)
        self._active_endpoint = (ORCHESTRATOR_HOST, ORCHESTRATOR_PORT)
        if self._active_endpoint not in self._orchestrator_endpoints:
            self._orchestrator_endpoints.insert(0, self._active_endpoint)

    # ── Controle ─────────────────────────────────────────────

    def start(self) -> None:
        os.makedirs("logs", exist_ok=True)

        self._setup_server()

        # Inicia thread de heartbeat
        hb_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"{self.worker_id}-hb",
            daemon=True,
        )
        hb_thread.start()

        self.logger.info(
            f"Worker {self.worker_id} iniciado em {self.host}:{self.port}"
        )

        self._accept_loop()

    def stop(self) -> None:
        self.logger.info(f"Encerrando worker {self.worker_id}...")
        self._stop_event.set()
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass

    # ── Servidor TCP ─────────────────────────────────────────

    def _setup_server(self) -> None:
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(10)
        self._server_sock.settimeout(2.0)

    def _accept_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                conn, addr = self._server_sock.accept()
                threading.Thread(
                    target=self._handle_connection,
                    args=(conn,),
                    daemon=True,
                ).start()
            except socket.timeout:
                continue
            except OSError:
                break

    def _handle_connection(self, conn: socket.socket) -> None:
        """Recebe MSG_ASSIGN_TASK do scheduler, responde ACK e processa."""
        try:
            with conn:
                msg = recv_msg(conn)
                if not msg:
                    return

                self.clock.receive(msg.get("lamport", 0))
                msg_type = msg.get("type", "")

                if msg_type == MSG_ASSIGN_TASK:
                    task_id = msg.get("task_id", "")
                    payload = msg.get("payload", {})
                    client  = msg.get("client", "")

                    # Responde com ACK imediatamente
                    ack = _base(MSG_ACK, self.worker_id, self.clock.send())
                    send_msg(conn, ack)

                    self.logger.info(
                        f"[TASK_RECEIVED] task={task_id} client={client}"
                    )

                    # Registra tarefa ativa
                    with self._tasks_lock:
                        self._active_tasks.append(task_id)

                    # Processa em thread separada
                    threading.Thread(
                        target=self._execute_task,
                        args=(task_id, payload),
                        daemon=True,
                    ).start()

        except Exception as e:
            self.logger.debug(f"Erro ao tratar conexão: {e}")

    # ── Execução de tarefa ───────────────────────────────────

    def _execute_task(self, task_id: str, payload: dict) -> None:
        """Executa tarefa via TaskRunner e envia resultado ao orquestrador."""
        self.logger.info(f"[TASK_STARTED] task={task_id}")

        success, result = self.runner.run(task_id, payload)

        # Remove da lista de tarefas ativas
        with self._tasks_lock:
            if task_id in self._active_tasks:
                self._active_tasks.remove(task_id)

        ts = self.clock.send()

        if success:
            status = TASK_COMPLETED
            msg = msg_task_result(
                sender=self.worker_id, lamport=ts,
                task_id=task_id, status=status, result=result,
            )
            log_task_completed(self.logger, task_id, self.worker_id, ts)
        else:
            status = TASK_FAILED
            msg = msg_task_result(
                sender=self.worker_id, lamport=ts,
                task_id=task_id, status=status, error=str(result),
            )
            log_task_failed(self.logger, task_id, self.worker_id, str(result), ts)

        # Envia resultado ao orquestrador principal
        self._send_result_to_orchestrator(msg)

        # Simulação de crash
        if self._simulate_crash_after > 0:
            self._tasks_completed += 1
            if self._tasks_completed >= self._simulate_crash_after:
                self.logger.warning(
                    f"💥 CRASH SIMULADO após {self._tasks_completed} tarefas!"
                )
                os._exit(1)

    def _send_result_to_orchestrator(self, msg: dict) -> None:
        """Conecta ao orquestrador e envia MSG_TASK_RESULT."""
        last_error = None
        for host, port in self._iter_endpoints(self._active_endpoint):
            try:
                with socket.create_connection((host, port), timeout=10) as sock:
                    send_msg(sock, msg)
                    ack = recv_msg(sock)
                    if ack and ack.get("type") == MSG_ACK:
                        self.clock.receive(ack.get("lamport", 0))
                        self._active_endpoint = (host, port)
                        self.logger.debug(
                            f"Resultado de {msg.get('task_id')} entregue em {host}:{port}"
                        )
                        return
            except Exception as e:
                last_error = e

        self.logger.error(
            f"Erro ao enviar resultado de {msg.get('task_id')}: {last_error}"
        )

    # ── Heartbeat ────────────────────────────────────────────

    def _heartbeat_loop(self) -> None:
        """Envia heartbeat periódico ao HeartbeatMonitor do orquestrador."""
        while not self._stop_event.is_set():
            try:
                with self._tasks_lock:
                    active = list(self._active_tasks)

                hb = msg_heartbeat(
                    sender=self.worker_id,
                    lamport=self.clock.send(),
                    worker_id=self.worker_id,
                    active_tasks=active,
                )
                # Inclui porta do worker para registro automático
                hb["worker_port"] = self.port
                hb["worker_host"] = self.host

                delivered = False
                for host, port in self._iter_endpoints(self._active_endpoint):
                    hb_port = port + 100
                    try:
                        with socket.create_connection((host, hb_port), timeout=5) as sock:
                            send_msg(sock, hb)
                            ack = recv_msg(sock)
                            if ack:
                                self.clock.receive(ack.get("lamport", 0))
                                self._active_endpoint = (host, port)
                                delivered = True
                                break
                    except Exception:
                        continue

                if not delivered:
                    self.logger.debug("Heartbeat nao entregue em nenhum endpoint")

            except Exception as e:
                self.logger.debug(f"Heartbeat falhou: {e}")

            time.sleep(HEARTBEAT_INTERVAL)

    def _iter_endpoints(self, preferred: tuple[str, int]):
        candidates = [preferred]
        for endpoint in self._orchestrator_endpoints:
            if endpoint not in candidates:
                candidates.append(endpoint)

        fallback_backup = (BACKUP_HOST, BACKUP_PORT)
        if fallback_backup not in candidates:
            candidates.append(fallback_backup)

        return candidates


# ════════════════════════════════════════════════════════════
#  Ponto de entrada
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Worker do sistema distribuído")
    parser.add_argument("--id",    default="worker-1",  help="ID do worker")
    parser.add_argument("--index", type=int, default=0, help="Índice (0, 1, 2…)")
    parser.add_argument(
        "--crash-after", type=int, default=0,
        help="Simula crash após N tarefas completadas (0 = sem crash)",
    )

    args = parser.parse_args()

    worker = Worker(
        worker_id=args.id,
        index=args.index,
        simulate_crash_after=args.crash_after,
    )

    try:
        worker.start()
    except KeyboardInterrupt:
        worker.stop()
        print(f"\nWorker {args.id} encerrado.")