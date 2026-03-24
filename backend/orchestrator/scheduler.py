# Round Robin + reatribuição
# ============================================================
#  orchestrator/scheduler.py
#  Scheduler Round Robin
#
#  Responsabilidades:
#    - Distribuir tarefas pendentes para workers via Round Robin
#    - Conectar ao worker via TCP e enviar MSG_ASSIGN_TASK
#    - Reatribuir tarefas de workers mortos
#    - Rodar em thread separada, consultando estado periodicamente
# ============================================================
 
import socket
import threading
import time
 
from common.config   import MAX_RETRIES
from common.lamport  import LamportClock
from common.logger   import get_logger, log_task_distributed, log_task_reassigned, log_task_failed
from common.protocol import (
    msg_assign_task, msg_task_result,
    send_msg, recv_msg,
    TASK_FAILED,
)
 
class RoundRobinScheduler:
    """
    Scheduler que roda em thread dedicada.
 
    Algoritmo Round Robin:
      - Mantém um ponteiro no GlobalState (_rr_index)
      - A cada ciclo busca tarefas PENDING e distribui
        para o próximo worker vivo na ordem circular
      - Se não há workers disponíveis, aguarda e tenta novamente
 
    Vantagens do Round Robin (para documentar no README):
      + Simplicidade de implementação e raciocínio
      + Distribuição uniforme quando tarefas têm custo similar
      + Sem overhead de monitoramento de carga
 
    Limitações (para documentar no README):
      - Não considera a carga atual de cada worker
      - Tarefas longas em um worker não reduzem sua prioridade
      - Solução: Least Load (extensão futura)
    """
 
    def __init__(self, state, clock: LamportClock,
                 node_name: str = "ORCHESTRATOR",
                 dispatch_interval: float = 0.5):
        self.state             = state
        self.clock             = clock
        self.logger            = get_logger(node_name, clock)
        self.dispatch_interval = dispatch_interval
        self._stop_event       = threading.Event()
        self._thread           = None
 
    # Ciclo principal 
 
    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._dispatch_loop,
            name="scheduler",
            daemon=True
        )
        self._thread.start()
        self.logger.info("Scheduler Round Robin iniciado")
 
    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=3)
 
    def _dispatch_loop(self) -> None:
        """
        Loop principal: a cada `dispatch_interval` segundos
        tenta distribuir todas as tarefas pendentes.
        """
        while not self._stop_event.is_set():
            try:
                pending = self.state.get_pending_tasks()
                for task in pending:
                    if self._stop_event.is_set():
                        break
                    self._try_dispatch(task)
            except Exception as e:
                self.logger.error(f"Erro no loop do scheduler: {e}")
            time.sleep(self.dispatch_interval)
 
    # Despacho de tarefa 
 
    def _try_dispatch(self, task) -> bool:
        """
        Tenta despachar uma tarefa para o próximo worker Round Robin.
        Retorna True se despachada com sucesso.
        """
        if task.retries >= MAX_RETRIES:
            self.state.fail_task(task.task_id, "Número máximo de tentativas atingido")
            log_task_failed(
                self.logger, task.task_id, task.worker_id or "N/A",
                "max_retries", self.clock.tick()
            )
            return False
 
        worker = self.state.next_worker_round_robin()
        if worker is None:
            self.logger.warning(
                f"Sem workers disponíveis para tarefa {task.task_id} — aguardando"
            )
            return False
 
        # Atualiza estado ANTES de enviar (checkpoint)
        self.state.assign_task(task.task_id, worker.worker_id)
        ts = self.clock.send()
 
        success = self._send_task_to_worker(task, worker, ts)
        if not success:
            # Worker não respondeu: retorna para pending
            self.state.mark_worker_dead(worker.worker_id)
            self.state.reset_task_to_pending(task.task_id)
            self.logger.warning(
                f"Falha ao enviar {task.task_id} para {worker.worker_id} "
                f"— worker marcado como DEAD, tarefa reenfileirada"
            )
            return False
 
        log_task_distributed(
            self.logger, task.task_id, worker.worker_id, ts
        )
        return True
 
    def _send_task_to_worker(self, task, worker, lamport: int) -> bool:
        """
        Abre conexão TCP com o worker e envia MSG_ASSIGN_TASK.
        Retorna True se o worker confirmou recebimento (MSG_ACK).
        """
        try:
            with socket.create_connection(
                (worker.host, worker.port), timeout=5
            ) as sock:
                msg = msg_assign_task(
                    sender   = "ORCHESTRATOR",
                    lamport  = lamport,
                    task_id  = task.task_id,
                    payload  = task.payload,
                    client   = task.client,
                )
                send_msg(sock, msg)
 
                # Aguarda ACK do worker
                ack = recv_msg(sock)
                if ack and ack.get("type") == "ACK":
                    # Sincroniza relógio com o timestamp do worker
                    self.clock.receive(ack.get("lamport", 0))
                    return True
                return False
 
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            self.logger.debug(
                f"Conexão com {worker.worker_id} ({worker.host}:{worker.port}) "
                f"falhou: {e}"
            )
            return False
 
    # Reatribuição após falha de worker 
 
    def reassign_tasks_of_dead_worker(self, worker_id: str) -> None:
        """
        Chamado pelo HeartbeatMonitor quando um worker é declarado morto.
        Retorna todas as tarefas do worker para a fila PENDING.
        """
        task_ids = self.state.get_running_tasks_of_worker(worker_id)
        if not task_ids:
            return
 
        for task_id in task_ids:
            self.state.reset_task_to_pending(task_id)
            ts = self.clock.tick()
            log_task_reassigned(
                self.logger, task_id, worker_id, "PENDING_QUEUE", ts
            )
 
        self.logger.warning(
            f"{len(task_ids)} tarefa(s) de {worker_id} devolvidas à fila"
        )