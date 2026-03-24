from common.protocol import TASK_PENDING
from common.lamport import LamportClock
from orchestrator.scheduler import RoundRobinScheduler
from orchestrator.state import GlobalState


def test_reassign_tasks_of_dead_worker_returns_running_to_pending():
    state = GlobalState()
    clock = LamportClock()

    state.add_worker("worker-1", "127.0.0.1", 6000)
    state.add_worker("worker-2", "127.0.0.1", 6001)

    state.add_task("T-REA-001", "alice", {"operation": "sum", "data": [1, 2]})
    state.assign_task("T-REA-001", "worker-1")

    scheduler = RoundRobinScheduler(state=state, clock=clock, dispatch_interval=1)
    scheduler.reassign_tasks_of_dead_worker("worker-1")

    task = state.get_task("T-REA-001")
    assert task is not None
    assert task.status == TASK_PENDING
    assert task.worker_id == ""
    assert task.retries >= 1
