# ============================================================
#  backup/sync.py
#  Sincronização do Backup via UDP Multicast
#
#  Escuta snapshots enviados pelo orquestrador principal.
#  Detecta quando o principal parou de enviar (timeout)
#  e dispara callback de failover.
# ============================================================

import threading
import time

from common.config  import MULTICAST_GROUP, MULTICAST_PORT, HEARTBEAT_TIMEOUT
from common.lamport import LamportClock
from common.logger  import get_logger
from orchestrator.multicast import MulticastReceiver


class SyncMonitor:
    """
    Monitora a sincronização com o orquestrador principal.

    Internamente usa MulticastReceiver para receber snapshots.
    Além disso, monitora o tempo desde o último sync recebido;
    se ultrapassar o timeout, dispara o callback de failover.

    Uso:
        sync = SyncMonitor(state, clock)
        sync.on_primary_dead(failover_callback)
        sync.start()
    """

    def __init__(self, state, clock: LamportClock,
                 group: str = MULTICAST_GROUP,
                 port: int = MULTICAST_PORT,
                 sync_timeout: float = None,
                 node_name: str = "BACKUP"):

        self.state   = state
        self.clock   = clock
        self.logger  = get_logger(node_name, clock)

        # Timeout = 3x o HEARTBEAT_TIMEOUT (dá margem ao principal)
        self.sync_timeout = sync_timeout or (HEARTBEAT_TIMEOUT * 3)

        self._receiver = MulticastReceiver(
            state=state, clock=clock,
            group=group, port=port,
            node_name=node_name,
        )

        self._last_sync      = time.time()
        self._on_dead_cb     = None
        self._stop_event     = threading.Event()
        self._checker_thread = None
        self._failover_done  = False

    def on_primary_dead(self, callback) -> None:
        """Registra callback chamado quando o principal é detectado como morto."""
        self._on_dead_cb = callback

    def start(self) -> None:
        # Monkey-patch o receiver para atualizar last_sync
        original_restore = self.state.restore_snapshot

        def patched_restore(snap):
            self._last_sync = time.time()
            original_restore(snap)

        self.state.restore_snapshot = patched_restore

        self._receiver.start()

        self._checker_thread = threading.Thread(
            target=self._check_loop,
            name="sync-checker",
            daemon=True,
        )
        self._checker_thread.start()
        self.logger.info(
            f"SyncMonitor iniciado (timeout={self.sync_timeout}s)"
        )

    def stop(self) -> None:
        self._stop_event.set()
        self._receiver.stop()

    def _check_loop(self) -> None:
        # Aguarda um período inicial para dar tempo do principal iniciar
        time.sleep(self.sync_timeout)

        while not self._stop_event.is_set():
            elapsed = time.time() - self._last_sync

            if elapsed > self.sync_timeout and not self._failover_done:
                self.logger.warning(
                    f"Orquestrador principal sem sync há {elapsed:.1f}s "
                    f"(timeout={self.sync_timeout}s) — iniciando failover"
                )
                self._failover_done = True
                if self._on_dead_cb:
                    try:
                        self._on_dead_cb()
                    except Exception as e:
                        self.logger.error(f"Erro no callback de failover: {e}")

            time.sleep(HEARTBEAT_TIMEOUT)