from common.protocol import TASK_COMPLETED
from orchestrator.state import GlobalState


def test_late_result_from_old_worker_is_rejected_after_reassignment():
    state = GlobalState()

    state.add_worker("worker-old", "127.0.0.1", 6000)
    state.add_worker("worker-new", "127.0.0.1", 6001)

    state.add_task("T-LATE-001", "alice", {"operation": "sum", "data": [5, 5]})
    state.assign_task("T-LATE-001", "worker-old")

    # Simula perda do worker antigo e reatribuicao.
    state.reset_task_to_pending("T-LATE-001")
    state.assign_task("T-LATE-001", "worker-new")

    can_old, reason_old = state.can_accept_task_result("T-LATE-001", "worker-old")
    assert can_old is False
    assert reason_old.startswith("worker_mismatch")

    can_new, reason_new = state.can_accept_task_result("T-LATE-001", "worker-new")
    assert can_new is True
    assert reason_new == "ok"

    state.complete_task("T-LATE-001", 10)
    task = state.get_task("T-LATE-001")
    assert task is not None
    assert task.status == TASK_COMPLETED
    assert task.result == 10
