# ============================================================
#  common/lamport.py
#  Relógio Lógico de Lamport
#
#  Regras implementadas:
#    1. A cada evento local:        clock += 1
#    2. Ao ENVIAR mensagem:         clock += 1  (inclui timestamp)
#    3. Ao RECEBER mensagem:        clock = max(local, recebido) + 1
# ============================================================
 
import threading
 
 
class LamportClock:
    """
    Relógio lógico de Lamport thread-safe.
 
    Uso típico:
        clock = LamportClock()
 
        # evento local
        ts = clock.tick()
 
        # ao enviar
        ts = clock.send()
 
        # ao receber (ts_msg = timestamp da mensagem recebida)
        ts = clock.receive(ts_msg)
    """
 
    def __init__(self, initial: int = 0):
        self._time  = initial
        self._lock  = threading.Lock()
 
    # API pública 
 
    def tick(self) -> int:
        """Evento local: incrementa e retorna o novo valor."""
        with self._lock:
            self._time += 1
            return self._time
 
    def send(self) -> int:
        """
        Preparar envio de mensagem.
        Incrementa o relógio e retorna o timestamp a incluir na mensagem.
        """
        return self.tick()
 
    def receive(self, received_ts: int) -> int:
        """
        Processar recebimento de mensagem com timestamp `received_ts`.
        Aplica: clock = max(clock, received_ts) + 1
        """
        with self._lock:
            self._time = max(self._time, received_ts) + 1
            return self._time
 
    @property
    def value(self) -> int:
        """Lê o valor atual sem modificar."""
        with self._lock:
            return self._time
 
    def __repr__(self) -> str:
        return f"LamportClock(time={self._time})"
 
 
# Utilitário: decorador de evento 
 
def lamport_event(clock: LamportClock, label: str = ""):
    """
    Decorador que envolve uma função em um evento local do relógio.
 
    Exemplo:
        @lamport_event(clock, "processar_tarefa")
        def processar(tarefa_id):
            ...
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            ts = clock.tick()
            if label:
                from common.logger import get_logger
                get_logger().debug(f"[Lamport:{ts}] {label}")
            return func(*args, **kwargs)
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator
 
 
# Teste rápido (python -m common.lamport) 
if __name__ == "__main__":
    c1 = LamportClock()
    c2 = LamportClock()
 
    # Processo 1 envia para processo 2
    ts_send = c1.send()
    print(f"P1 envia  → ts={ts_send},  C1={c1.value}")
 
    ts_recv = c2.receive(ts_send)
    print(f"P2 recebe ← ts={ts_recv},  C2={c2.value}")
 
    # Evento local em P2
    ts_local = c2.tick()
    print(f"P2 evento   ts={ts_local}, C2={c2.value}")
 
    # P2 envia de volta para P1
    ts_send2 = c2.send()
    ts_recv2 = c1.receive(ts_send2)
    print(f"P1 recebe ← ts={ts_recv2}, C1={c1.value}")
 
    # Esperado: C1 > C2 no início, C1 sincroniza ao receber
    assert c1.value > ts_send,  "Invariante violada"
    print("\nTodos os invariantes OK ✓")
 