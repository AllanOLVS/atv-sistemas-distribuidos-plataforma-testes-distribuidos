# ============================================================
#  orchestrator/multicast.py
#  Sincronização de estado com o Backup via UDP Multicast
#
#  O orquestrador principal envia o snapshot do estado global
#  periodicamente (e após cada mudança relevante) para o
#  grupo multicast. O backup escuta esse grupo e atualiza
#  sua cópia local do estado.
# ============================================================
 
import socket
import struct
import threading
import time
 
from common.config   import MULTICAST_GROUP, MULTICAST_PORT, MULTICAST_TTL
from common.lamport  import LamportClock
from common.logger   import get_logger
from common.protocol import msg_state_sync, send_udp, encode
 
 
class MulticastSender:
    """
    Envia snapshots periódicos do estado global via UDP Multicast.
 
    Uso no orquestrador principal:
        sender = MulticastSender(state, clock)
        sender.start()
        # ... sistema rodando ...
        sender.send_now()  # força envio imediato (ex: após falha)
        sender.stop()
    """
 
    def __init__(self, state, clock: LamportClock,
                 group: str   = MULTICAST_GROUP,
                 port: int    = MULTICAST_PORT,
                 ttl: int     = MULTICAST_TTL,
                 interval: float = 2.0,
                 node_name: str  = "ORCHESTRATOR"):
 
        self.state     = state
        self.clock     = clock
        self.logger    = get_logger(node_name, clock)
        self.group     = group
        self.port      = port
        self.ttl       = ttl
        self.interval  = interval
 
        self._stop_event = threading.Event()
        self._thread     = None
        self._sock       = None
 
    def _create_socket(self) -> socket.socket:
        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
            socket.IPPROTO_UDP
        )
        sock.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_MULTICAST_TTL,
            struct.pack("b", self.ttl)
        )
        return sock
 
    def start(self) -> None:
        self._sock   = self._create_socket()
        self._thread = threading.Thread(
            target=self._send_loop,
            name="multicast-sender",
            daemon=True
        )
        self._thread.start()
        self.logger.info(
            f"MulticastSender iniciado → {self.group}:{self.port} "
            f"(intervalo={self.interval}s)"
        )
 
    def stop(self) -> None:
        self._stop_event.set()
        if self._sock:
            self._sock.close()
 
    def send_now(self) -> None:
        """Força um envio imediato do snapshot (ex: após failover ou falha)."""
        self._do_send()
 
    def _send_loop(self) -> None:
        while not self._stop_event.is_set():
            self._do_send()
            time.sleep(self.interval)
 
    def _do_send(self) -> None:
        try:
            snap    = self.state.snapshot()
            ts      = self.clock.send()
            message = msg_state_sync(
                sender  = "ORCHESTRATOR",
                lamport = ts,
                state   = snap
            )
            send_udp(self._sock, message, self.group, self.port)
            self.logger.debug(
                f"[STATE_SYNC] lamport={ts} "
                f"tasks={len(snap['tasks'])} "
                f"workers={len(snap['workers'])}"
            )
        except Exception as e:
            self.logger.error(f"Erro ao enviar multicast: {e}")
 
 
class MulticastReceiver:
    """
    Escuta snapshots de estado no grupo multicast.
    Usado pelo Backup para manter cópia sincronizada.
 
    Uso no orquestrador backup:
        receiver = MulticastReceiver(state, clock)
        receiver.start()
    """
 
    def __init__(self, state, clock: LamportClock,
                 group: str  = MULTICAST_GROUP,
                 port: int   = MULTICAST_PORT,
                 node_name: str = "BACKUP"):
 
        self.state    = state
        self.clock    = clock
        self.logger   = get_logger(node_name, clock)
        self.group    = group
        self.port     = port
 
        self._stop_event = threading.Event()
        self._thread     = None
        self._sock       = None
 
    def _create_socket(self) -> socket.socket:
        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
            socket.IPPROTO_UDP
        )
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", self.port))
 
        # Entrar no grupo multicast
        group_bin = socket.inet_aton(self.group)
        mreq = struct.pack("4sL", group_bin, socket.INADDR_ANY)
        sock.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            mreq
        )
        sock.settimeout(2.0)
        return sock
 
    def start(self) -> None:
        self._sock   = self._create_socket()
        self._thread = threading.Thread(
            target=self._receive_loop,
            name="multicast-receiver",
            daemon=True
        )
        self._thread.start()
        self.logger.info(
            f"MulticastReceiver escutando em {self.group}:{self.port}"
        )
 
    def stop(self) -> None:
        self._stop_event.set()
        if self._sock:
            self._sock.close()
 
    def _receive_loop(self) -> None:
        import json
        while not self._stop_event.is_set():
            try:
                data, addr = self._sock.recvfrom(65535)
                msg = json.loads(data.decode("utf-8").strip())
 
                ts = self.clock.receive(msg.get("lamport", 0))
 
                if msg.get("type") == "STATE_SYNC":
                    snap = msg.get("state", {})
                    self.state.restore_snapshot(snap)
                    self.logger.debug(
                        f"[STATE_SYNC] recebido de {addr[0]} lamport={ts} "
                        f"tasks={len(snap.get('tasks', {}))} "
                        f"workers={len(snap.get('workers', {}))}"
                    )
 
            except socket.timeout:
                continue
            except Exception as e:
                if not self._stop_event.is_set():
                    self.logger.error(f"Erro ao receber multicast: {e}")