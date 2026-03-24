# estado global (tarefas, workers)
# ============================================================
#  orchestrator/state.py
#  Estado global do Orquestrador
#
#  Mantém em memória:
#    - Registro de todas as tarefas (fila, em execução, concluídas)
#    - Registro de todos os workers (vivos, mortos, carga)
#    - Índice do Round Robin
#
#  Thread-safe: todos os métodos usam RLock interno.
#  O método `snapshot()` serializa o estado para envio via multicast.
# ============================================================
 
import threading
import time
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional
 
from common.config import TASK_TIMEOUT
from common.protocol import (
    TASK_PENDING, TASK_RUNNING, TASK_COMPLETED, TASK_FAILED
)
 
 
# ════════════════════════════════════════════════════════════
#  Estruturas de dados
# ════════════════════════════════════════════════════════════
 
@dataclass
class TaskRecord:
    task_id:      str
    client:       str
    payload:      object
    status:       str   = TASK_PENDING
    worker_id:    str   = ""
    result:       object = None
    error:        str   = ""
    submitted_at: float = field(default_factory=time.time)
    started_at:   float = 0.0
    finished_at:  float = 0.0
    retries:      int   = 0
    in_recovery:  bool  = False
    recovery_deadline_at: float = 0.0
    recover_from_worker: str = ""
 
    def to_dict(self) -> dict:
        return asdict(self)
 
    @staticmethod
    def from_dict(d: dict) -> "TaskRecord":
        return TaskRecord(**d)
 
 
@dataclass
class WorkerRecord:
    worker_id:    str
    host:         str
    port:         int
    status:       str   = "ALIVE"        # ALIVE | DEAD
    active_tasks: List[str] = field(default_factory=list)
    last_heartbeat: float   = field(default_factory=time.time)
 
    def to_dict(self) -> dict:
        return asdict(self)
 
    @staticmethod
    def from_dict(d: dict) -> "WorkerRecord":
        return WorkerRecord(**d)
 
 
# ════════════════════════════════════════════════════════════
#  GlobalState
# ════════════════════════════════════════════════════════════
 
class GlobalState:
    """
    Repositório central de estado, thread-safe.
 
    Uso:
        state = GlobalState()
        state.add_worker("worker-1", "127.0.0.1", 6000)
        state.add_task("T-001", "alice", {"op": "sum", "data": [1,2,3]})
        next_w = state.next_worker_round_robin()
        state.assign_task("T-001", next_w)
    """
 
    def __init__(self):
        self._lock:    threading.RLock  = threading.RLock()
        self._tasks:   Dict[str, TaskRecord]   = {}
        self._workers: Dict[str, WorkerRecord] = {}
        self._rr_index: int = 0          # ponteiro do Round Robin
 
    # Workers 
 
    def add_worker(self, worker_id: str, host: str, port: int) -> None:
        with self._lock:
            self._workers[worker_id] = WorkerRecord(
                worker_id=worker_id, host=host, port=port
            )
 
    def remove_worker(self, worker_id: str) -> None:
        with self._lock:
            self._workers.pop(worker_id, None)
 
    def mark_worker_dead(self, worker_id: str) -> None:
        with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id].status = "DEAD"
                self._workers[worker_id].active_tasks.clear()
 
    def update_heartbeat(self, worker_id: str,
                         active_tasks: List[str] = None) -> None:
        with self._lock:
            if worker_id in self._workers:
                w = self._workers[worker_id]
                w.last_heartbeat = time.time()
                w.status         = "ALIVE"
                if active_tasks is not None:
                    w.active_tasks = active_tasks
 
    def get_alive_workers(self) -> List[WorkerRecord]:
        with self._lock:
            return [w for w in self._workers.values() if w.status == "ALIVE"]
 
    def get_worker(self, worker_id: str) -> Optional[WorkerRecord]:
        with self._lock:
            return self._workers.get(worker_id)
 
    # Round Robin
 
    def next_worker_round_robin(self) -> Optional[WorkerRecord]:
        """
        Retorna o próximo worker vivo em modo Round Robin.
        Avança o ponteiro e pula workers mortos.
        Retorna None se não houver workers disponíveis.
        """
        with self._lock:
            alive = [w for w in self._workers.values() if w.status == "ALIVE"]
            if not alive:
                return None
 
            # Ordena por worker_id para garantir ordem determinística
            alive.sort(key=lambda w: w.worker_id)
 
            selected = alive[self._rr_index % len(alive)]
            self._rr_index = (self._rr_index + 1) % len(alive)
            return selected
 
    # Tarefas 
 
    def add_task(self, task_id: str, client: str, payload: object) -> TaskRecord:
        with self._lock:
            record = TaskRecord(task_id=task_id, client=client, payload=payload)
            self._tasks[task_id] = record
            return record
 
    def assign_task(self, task_id: str, worker_id: str) -> None:
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].status    = TASK_RUNNING
                self._tasks[task_id].worker_id = worker_id
                self._tasks[task_id].started_at = time.time()
                self._tasks[task_id].in_recovery = False
                self._tasks[task_id].recovery_deadline_at = 0.0
                self._tasks[task_id].recover_from_worker = ""
            if worker_id in self._workers:
                if task_id not in self._workers[worker_id].active_tasks:
                    self._workers[worker_id].active_tasks.append(task_id)
 
    def complete_task(self, task_id: str, result: object) -> None:
        with self._lock:
            if task_id in self._tasks:
                t = self._tasks[task_id]
                t.status      = TASK_COMPLETED
                t.result      = result
                t.finished_at = time.time()
                t.in_recovery = False
                t.recovery_deadline_at = 0.0
                t.recover_from_worker = ""
                self._remove_task_from_worker(task_id, t.worker_id)
 
    def fail_task(self, task_id: str, error: str) -> None:
        with self._lock:
            if task_id in self._tasks:
                t = self._tasks[task_id]
                t.status      = TASK_FAILED
                t.error       = error
                t.finished_at = time.time()
                t.in_recovery = False
                t.recovery_deadline_at = 0.0
                t.recover_from_worker = ""
                self._remove_task_from_worker(task_id, t.worker_id)
 
    def reset_task_to_pending(self, task_id: str) -> None:
        """Reatribui tarefa para a fila (worker morreu)."""
        with self._lock:
            if task_id in self._tasks:
                t = self._tasks[task_id]
                old_worker    = t.worker_id
                t.status      = TASK_PENDING
                t.worker_id   = ""
                t.started_at  = 0.0
                t.retries    += 1
                t.in_recovery = False
                t.recovery_deadline_at = 0.0
                t.recover_from_worker = ""
                self._remove_task_from_worker(task_id, old_worker)

    def mark_running_tasks_for_recovery(self, grace_seconds: float) -> list[dict]:
        """
        Marca tarefas RUNNING para uma janela de recuperação pós-failover.
        Retorna metadados para logging.
        """
        with self._lock:
            now = time.time()
            deadline = now + grace_seconds
            marked = []
            for task in self._tasks.values():
                if task.status != TASK_RUNNING or not task.worker_id:
                    continue
                task.in_recovery = True
                task.recovery_deadline_at = deadline
                task.recover_from_worker = task.worker_id
                marked.append(
                    {
                        "task_id": task.task_id,
                        "worker_id": task.worker_id,
                        "deadline_at": deadline,
                    }
                )
            return marked

    def promote_expired_recoveries_to_pending(self, now: float | None = None) -> list[dict]:
        """
        Converte tarefas RUNNING em recuperação expiradas para PENDING.
        Retorna lista de tarefas expiradas para logging.
        """
        with self._lock:
            now = now or time.time()
            expired = []
            for task in self._tasks.values():
                if task.status != TASK_RUNNING:
                    continue
                if not task.in_recovery:
                    continue
                if task.recovery_deadline_at <= 0:
                    continue
                if now < task.recovery_deadline_at:
                    continue

                old_worker = task.worker_id
                task.status = TASK_PENDING
                task.worker_id = ""
                task.started_at = 0.0
                task.retries += 1
                task.in_recovery = False
                task.recovery_deadline_at = 0.0
                task.recover_from_worker = ""
                self._remove_task_from_worker(task.task_id, old_worker)

                expired.append({"task_id": task.task_id, "worker_id": old_worker})

            return expired

    def can_accept_task_result(self, task_id: str, worker_id: str) -> tuple[bool, str]:
        """
        Valida se um TASK_RESULT ainda é aplicável ao estado atual.
        Evita que resultado tardio sobrescreva estado já reatribuído/concluído.
        """
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False, "task_not_found"
            if task.status != TASK_RUNNING:
                return False, f"task_not_running:{task.status}"
            if task.worker_id != worker_id:
                return False, f"worker_mismatch:expected={task.worker_id}"
            return True, "ok"
 
    def get_task(self, task_id: str) -> Optional[TaskRecord]:
        with self._lock:
            return self._tasks.get(task_id)
 
    def get_pending_tasks(self) -> List[TaskRecord]:
        with self._lock:
            return [t for t in self._tasks.values() if t.status == TASK_PENDING]
 
    def get_running_tasks_of_worker(self, worker_id: str) -> List[str]:
        with self._lock:
            return [
                tid for tid, t in self._tasks.items()
                if t.status == TASK_RUNNING and t.worker_id == worker_id
            ]
 
    def get_timed_out_tasks(self) -> List[TaskRecord]:
        """Tarefas em RUNNING há mais de TASK_TIMEOUT segundos."""
        with self._lock:
            now = time.time()
            return [
                t for t in self._tasks.values()
                if t.status == TASK_RUNNING
                and (now - t.started_at) > TASK_TIMEOUT
            ]
 
    # Snapshot (para sincronização com backup) 
 
    def snapshot(self) -> dict:
        """Serializa o estado completo para envio via multicast."""
        with self._lock:
            return {
                "tasks":    {tid: t.to_dict() for tid, t in self._tasks.items()},
                "workers":  {wid: w.to_dict() for wid, w in self._workers.items()},
                "rr_index": self._rr_index,
            }
 
    def restore_snapshot(self, snap: dict) -> None:
        """Restaura estado a partir de um snapshot (usado pelo backup)."""
        with self._lock:
            self._tasks    = {
                tid: TaskRecord.from_dict(d) for tid, d in snap.get("tasks", {}).items()
            }
            self._workers  = {
                wid: WorkerRecord.from_dict(d) for wid, d in snap.get("workers", {}).items()
            }
            self._rr_index = snap.get("rr_index", 0)
 
    # Helpers internos 
 
    def _remove_task_from_worker(self, task_id: str, worker_id: str) -> None:
        if worker_id and worker_id in self._workers:
            try:
                self._workers[worker_id].active_tasks.remove(task_id)
            except ValueError:
                pass
 
    # Repr 
 
    def summary(self) -> str:
        with self._lock:
            total   = len(self._tasks)
            pending = sum(1 for t in self._tasks.values() if t.status == TASK_PENDING)
            running = sum(1 for t in self._tasks.values() if t.status == TASK_RUNNING)
            done    = sum(1 for t in self._tasks.values() if t.status == TASK_COMPLETED)
            failed  = sum(1 for t in self._tasks.values() if t.status == TASK_FAILED)
            workers_alive = sum(1 for w in self._workers.values() if w.status == "ALIVE")
            return (
                f"Tasks[total={total} pending={pending} running={running} "
                f"done={done} failed={failed}] "
                f"Workers[alive={workers_alive}/{len(self._workers)}] "
                f"RR_index={self._rr_index}"
            )