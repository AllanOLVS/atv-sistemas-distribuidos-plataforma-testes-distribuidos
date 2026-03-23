# mensagens padrão (JSON/gRPC)
# ============================================================
#  common/protocol.py
#  Protocolo de mensagens JSON entre todos os nós
#
#  Toda mensagem trafega como JSON em uma única linha + "\n"
#  Campos obrigatórios em toda mensagem:
#    type      : tipo da mensagem (ver constantes abaixo)
#    lamport   : timestamp lógico do remetente
#    sender    : identificação do nó remetente
# ============================================================
 
import json
import socket
from typing import Any, Optional
 
 
# ════════════════════════════════════════════════════════════
#  Tipos de mensagem
# ════════════════════════════════════════════════════════════
 
# Cliente → Orquestrador
MSG_LOGIN          = "LOGIN"           # autenticar usuário
MSG_LOGIN_OK       = "LOGIN_OK"        # token retornado
MSG_LOGIN_FAIL     = "LOGIN_FAIL"      # credenciais inválidas
 
MSG_SUBMIT_TASK    = "SUBMIT_TASK"     # submeter nova tarefa
MSG_TASK_ACCEPTED  = "TASK_ACCEPTED"   # tarefa enfileirada
MSG_TASK_REJECTED  = "TASK_REJECTED"   # token inválido ou erro
 
MSG_QUERY_STATUS   = "QUERY_STATUS"    # consultar status de tarefa
MSG_TASK_STATUS    = "TASK_STATUS"     # resposta com status
 
# Orquestrador → Worker
MSG_ASSIGN_TASK    = "ASSIGN_TASK"     # atribuir tarefa ao worker
MSG_TASK_RESULT    = "TASK_RESULT"     # worker reporta resultado
 
# Worker → Orquestrador
MSG_HEARTBEAT      = "HEARTBEAT"       # sinal de vida periódico
MSG_HEARTBEAT_ACK  = "HEARTBEAT_ACK"   # confirmação do orquestrador
 
# Orquestrador → Backup (via UDP Multicast)
MSG_STATE_SYNC     = "STATE_SYNC"      # sincronização de estado global
 
# Backup → (broadcast interno)
MSG_FAILOVER       = "FAILOVER"        # backup anuncia que assumiu
 
# Genérico
MSG_ERROR          = "ERROR"           # erro genérico
MSG_ACK            = "ACK"             # confirmação simples
 
# Estados de tarefa
TASK_PENDING    = "PENDING"
TASK_RUNNING    = "RUNNING"
TASK_COMPLETED  = "COMPLETED"
TASK_FAILED     = "FAILED"
 
 
# ════════════════════════════════════════════════════════════
#  Construtores de mensagem
# ════════════════════════════════════════════════════════════
 
def _base(msg_type: str, sender: str, lamport: int, **kwargs) -> dict:
    """Monta o envelope padrão de qualquer mensagem."""
    return {"type": msg_type, "sender": sender, "lamport": lamport, **kwargs}
 
 
# Autenticação 
 
def msg_login(sender: str, lamport: int, username: str, password: str) -> dict:
    return _base(MSG_LOGIN, sender, lamport, username=username, password=password)
 
def msg_login_ok(sender: str, lamport: int, token: str) -> dict:
    return _base(MSG_LOGIN_OK, sender, lamport, token=token)
 
def msg_login_fail(sender: str, lamport: int, reason: str = "Credenciais inválidas") -> dict:
    return _base(MSG_LOGIN_FAIL, sender, lamport, reason=reason)
 
 
# Submissão de tarefas 
 
def msg_submit_task(sender: str, lamport: int, token: str,
                    task_id: str, payload: Any) -> dict:
    return _base(MSG_SUBMIT_TASK, sender, lamport,
                 token=token, task_id=task_id, payload=payload)
 
def msg_task_accepted(sender: str, lamport: int, task_id: str) -> dict:
    return _base(MSG_TASK_ACCEPTED, sender, lamport, task_id=task_id)
 
def msg_task_rejected(sender: str, lamport: int, task_id: str, reason: str) -> dict:
    return _base(MSG_TASK_REJECTED, sender, lamport, task_id=task_id, reason=reason)
 
 
# Consulta de status 
 
def msg_query_status(sender: str, lamport: int, token: str, task_id: str) -> dict:
    return _base(MSG_QUERY_STATUS, sender, lamport, token=token, task_id=task_id)
 
def msg_task_status(sender: str, lamport: int, task_id: str,
                    status: str, result: Any = None) -> dict:
    return _base(MSG_TASK_STATUS, sender, lamport,
                 task_id=task_id, status=status, result=result)
 
 
# Atribuição para worker 
 
def msg_assign_task(sender: str, lamport: int, task_id: str,
                    payload: Any, client: str) -> dict:
    return _base(MSG_ASSIGN_TASK, sender, lamport,
                 task_id=task_id, payload=payload, client=client)
 
def msg_task_result(sender: str, lamport: int, task_id: str,
                    status: str, result: Any = None, error: str = "") -> dict:
    return _base(MSG_TASK_RESULT, sender, lamport,
                 task_id=task_id, status=status, result=result, error=error)
 
 
# Heartbeat 
 
def msg_heartbeat(sender: str, lamport: int, worker_id: str,
                  active_tasks: list = None) -> dict:
    return _base(MSG_HEARTBEAT, sender, lamport,
                 worker_id=worker_id, active_tasks=active_tasks or [])
 
def msg_heartbeat_ack(sender: str, lamport: int) -> dict:
    return _base(MSG_HEARTBEAT_ACK, sender, lamport)
 
 
# Sincronização de estado (multicast) 
 
def msg_state_sync(sender: str, lamport: int, state: dict) -> dict:
    """
    `state` deve conter o estado global completo:
      {
        "tasks":   { task_id: {status, worker, client, payload, ...} },
        "workers": { worker_id: {status, host, port, active_tasks} },
        "rr_index": int
      }
    """
    return _base(MSG_STATE_SYNC, sender, lamport, state=state)
 
 
# Failover 
 
def msg_failover(sender: str, lamport: int,
                 new_host: str, new_port: int) -> dict:
    return _base(MSG_FAILOVER, sender, lamport,
                 new_host=new_host, new_port=new_port)
 
 
# Erro genérico 
 
def msg_error(sender: str, lamport: int, reason: str) -> dict:
    return _base(MSG_ERROR, sender, lamport, reason=reason)
 
 
# ════════════════════════════════════════════════════════════
#  Serialização / desserialização
# ════════════════════════════════════════════════════════════
 
def encode(message: dict) -> bytes:
    """Serializa dict → bytes JSON com newline final."""
    return (json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8")
 
 
def decode(data: bytes) -> dict:
    """Desserializa bytes JSON → dict."""
    return json.loads(data.decode("utf-8").strip())
 
 
# ════════════════════════════════════════════════════════════
#  Helpers de I/O para sockets TCP
# ════════════════════════════════════════════════════════════
 
def send_msg(sock: socket.socket, message: dict) -> None:
    """Envia uma mensagem por um socket TCP."""
    sock.sendall(encode(message))
 
 
def recv_msg(sock: socket.socket, buffer_size: int = 4096) -> Optional[dict]:
    """
    Recebe uma mensagem por um socket TCP.
    Retorna None se a conexão foi encerrada.
    """
    data = b""
    while b"\n" not in data:
        chunk = sock.recv(buffer_size)
        if not chunk:
            return None
        data += chunk
    return decode(data)
 
 
def recv_msg_file(conn_file) -> Optional[dict]:
    """
    Recebe mensagem via makefile() do socket.
    Útil em loops de leitura com conn.makefile('rb').
    """
    line = conn_file.readline()
    if not line:
        return None
    return json.loads(line.decode("utf-8").strip())
 
 
# ════════════════════════════════════════════════════════════
#  Helpers de I/O para UDP (multicast)
# ════════════════════════════════════════════════════════════
 
def send_udp(sock: socket.socket, message: dict,
             group: str, port: int) -> None:
    """Envia mensagem via UDP para um grupo/endereço."""
    sock.sendto(encode(message), (group, port))
 
 
def recv_udp(sock: socket.socket,
             buffer_size: int = 8192) -> tuple[Optional[dict], tuple]:
    """
    Recebe mensagem UDP.
    Retorna (mensagem_dict, (host, porta)) ou (None, ...)
    """
    data, addr = sock.recvfrom(buffer_size)
    return decode(data), addr
 
 
# Teste rápido 
if __name__ == "__main__":
    # Testa encode/decode round-trip
    original = msg_submit_task(
        sender="client-alice",
        lamport=5,
        token="tok_abc123",
        task_id="T-001",
        payload={"operation": "sum", "data": [1, 2, 3]}
    )
 
    encoded  = encode(original)
    decoded  = decode(encoded)
 
    assert decoded["type"]    == MSG_SUBMIT_TASK
    assert decoded["task_id"] == "T-001"
    assert decoded["lamport"] == 5
    print("encode/decode OK ✓")
 
    # Testa construção de todas as mensagens principais
    msgs = [
        msg_login("c1", 1, "alice", "senha123"),
        msg_login_ok("orch", 2, "tok_xyz"),
        msg_heartbeat("w1", 3, "worker-1", ["T-001"]),
        msg_state_sync("orch", 4, {"tasks": {}, "workers": {}, "rr_index": 0}),
        msg_failover("backup", 5, "127.0.0.1", 5000),
    ]
    for m in msgs:
        assert "type" in m and "lamport" in m and "sender" in m
    print(f"{len(msgs)} tipos de mensagem validados ✓")