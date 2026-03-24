import time

from common.protocol import TASK_COMPLETED, TASK_PENDING
from orchestrator.state import GlobalState


def _build_running_task(state: GlobalState, task_id: str, worker_id: str) -> None:
    state.add_worker(worker_id, "127.0.0.1", 6000)
    state.add_task(task_id, "alice", {"operation": "sum", "data": [1, 2, 3]})
    state.assign_task(task_id, worker_id)


def test_running_task_kept_during_recovery_window():
    state = GlobalState()
    _build_running_task(state, "T-REC-001", "worker-1")

    marked = state.mark_running_tasks_for_recovery(grace_seconds=5)
    assert len(marked) == 1

    task = state.get_task("T-REC-001")
    assert task is not None
    assert task.in_recovery is True
    assert task.status == "RUNNING"

    # Resultado do worker original ainda deve ser aceito na janela de recuperacao.
    can_apply, reason = state.can_accept_task_result("T-REC-001", "worker-1")
    assert can_apply is True
    assert reason == "ok"


def test_expired_recovery_is_requeued_without_duplication():
    state = GlobalState()
    _build_running_task(state, "T-REC-002", "worker-1")

    state.mark_running_tasks_for_recovery(grace_seconds=0.01)
    time.sleep(0.03)

    expired = state.promote_expired_recoveries_to_pending()
    assert len(expired) == 1
    assert expired[0]["task_id"] == "T-REC-002"

    task = state.get_task("T-REC-002")
    assert task is not None
    assert task.status == TASK_PENDING
    assert task.worker_id == ""
    assert task.in_recovery is False

    # Resultado tardio do worker antigo nao pode sobrescrever estado.
    can_apply_old, reason_old = state.can_accept_task_result("T-REC-002", "worker-1")
    assert can_apply_old is False
    assert reason_old.startswith("task_not_running")

    # Reatribuicao para novo worker e conclusao unica.
    state.add_worker("worker-2", "127.0.0.1", 6001)
    state.assign_task("T-REC-002", "worker-2")

    can_apply_new, reason_new = state.can_accept_task_result("T-REC-002", "worker-2")
    assert can_apply_new is True
    assert reason_new == "ok"

    state.complete_task("T-REC-002", 6)

    done = state.get_task("T-REC-002")
    assert done is not None
    assert done.status == TASK_COMPLETED
    assert done.result == 6

    # Mesmo apos concluida, qualquer resultado tardio precisa ser ignorado.
    can_apply_late, reason_late = state.can_accept_task_result("T-REC-002", "worker-1")
    assert can_apply_late is False
    assert reason_late.startswith("task_not_running")
