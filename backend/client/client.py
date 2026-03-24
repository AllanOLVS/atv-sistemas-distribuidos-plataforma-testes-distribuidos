# ============================================================
#  client/client.py
#  Cliente interativo da plataforma distribuída
#
#  Para executar:
#      python -m client.client
#
#  Funcionalidades:
#    1. Login com usuário/senha → recebe token
#    2. Submeter tarefas autenticadas ao orquestrador
#    3. Consultar status de tarefas em tempo real
# ============================================================

import socket
import sys
import os
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.config  import ORCHESTRATOR_HOST, ORCHESTRATOR_PORT
from common.lamport import LamportClock
from common.logger  import get_logger
from common.protocol import (
    MSG_TASK_ACCEPTED, MSG_TASK_REJECTED,
    MSG_TASK_STATUS, MSG_ERROR,
    msg_submit_task, msg_query_status,
    send_msg, recv_msg,
)
from client.auth import Auth


class Client:
    """
    Cliente interativo com menu CLI.

    Mantém uma conexão TCP persistente com o orquestrador
    durante toda a sessão.
    """

    def __init__(self,
                 host: str = ORCHESTRATOR_HOST,
                 port: int = ORCHESTRATOR_PORT):
        self.host  = host
        self.port  = port
        self.clock = LamportClock()
        self.logger = get_logger("CLIENT", self.clock)
        self.auth  = Auth(self.clock, sender="CLIENT")
        self._sock = None
        self._submitted_tasks: list[str] = []

    # ── Conexão ──────────────────────────────────────────────

    def connect(self) -> bool:
        """Conecta ao orquestrador."""
        try:
            self._sock = socket.create_connection(
                (self.host, self.port), timeout=10
            )
            self.logger.info(
                f"Conectado ao orquestrador em {self.host}:{self.port}"
            )
            return True
        except Exception as e:
            print(f"\n✗ Erro ao conectar: {e}")
            return False

    def disconnect(self) -> None:
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    # ── Login ────────────────────────────────────────────────

    def do_login(self) -> bool:
        """Solicita credenciais e faz login."""
        print("\n── Login ──────────────────────────")
        username = input("  Usuário: ").strip()
        password = input("  Senha:   ").strip()

        if not username or not password:
            print("  ✗ Usuário e senha são obrigatórios.")
            return False

        token = self.auth.login(self._sock, username, password)
        if token:
            print(f"  ✓ Login bem-sucedido! Token: {token[:16]}…")
            self.logger.info(f"[LOGIN_OK] user={username}")
            return True
        else:
            print("  ✗ Credenciais inválidas.")
            self.logger.warning(f"[LOGIN_FAIL] user={username}")
            return False

    # ── Submissão de tarefa ──────────────────────────────────

    def do_submit(self) -> None:
        """Menu para submeter uma nova tarefa."""
        if not self.auth.is_authenticated():
            print("\n  ✗ Faça login primeiro!")
            return

        print("\n── Submeter Tarefa ────────────────")
        print("  Operações disponíveis:")
        print("    1. sum       - Somar lista de números")
        print("    2. multiply  - Multiplicar lista de números")
        print("    3. sort      - Ordenar lista")
        print("    4. count     - Contar elementos")
        print("    5. upper     - Converter textos para maiúsculo")

        ops = {"1": "sum", "2": "multiply", "3": "sort", "4": "count", "5": "upper"}
        choice = input("\n  Escolha (1-5): ").strip()
        operation = ops.get(choice)
        if not operation:
            print("  ✗ Opção inválida.")
            return

        data_str = input("  Dados (separados por vírgula): ").strip()
        if not data_str:
            print("  ✗ Dados não podem ser vazios.")
            return

        # Tenta converter para números, senão mantém como strings
        parts = [x.strip() for x in data_str.split(",")]
        try:
            data = [float(x) if "." in x else int(x) for x in parts]
        except ValueError:
            data = parts   # mantém como strings

        task_id = f"T-{uuid.uuid4().hex[:8].upper()}"
        payload = {"operation": operation, "data": data}

        msg = msg_submit_task(
            sender="CLIENT",
            lamport=self.clock.send(),
            token=self.auth.get_token(),
            task_id=task_id,
            payload=payload,
        )
        send_msg(self._sock, msg)

        # Aguarda resposta
        resp = recv_msg(self._sock)
        if resp is None:
            print("  ✗ Sem resposta do orquestrador.")
            return

        self.clock.receive(resp.get("lamport", 0))

        if resp.get("type") == MSG_TASK_ACCEPTED:
            self._submitted_tasks.append(task_id)
            print(f"  ✓ Tarefa {task_id} aceita e enfileirada!")
        elif resp.get("type") == MSG_TASK_REJECTED:
            print(f"  ✗ Tarefa rejeitada: {resp.get('reason', '?')}")
        else:
            print(f"  ✗ Resposta inesperada: {resp.get('type')}")

    # ── Consulta de status ───────────────────────────────────

    def do_query(self) -> None:
        """Menu para consultar status de uma tarefa."""
        if not self.auth.is_authenticated():
            print("\n  ✗ Faça login primeiro!")
            return

        if not self._submitted_tasks:
            print("\n  ✗ Nenhuma tarefa submetida nesta sessão.")
            return

        print("\n── Consultar Status ───────────────")
        print("  Tarefas submetidas nesta sessão:")
        for i, tid in enumerate(self._submitted_tasks, 1):
            print(f"    {i}. {tid}")
        print(f"    0. Digitar ID manualmente")

        choice = input("\n  Escolha: ").strip()

        if choice == "0":
            task_id = input("  Task ID: ").strip()
        else:
            try:
                idx = int(choice) - 1
                task_id = self._submitted_tasks[idx]
            except (ValueError, IndexError):
                print("  ✗ Opção inválida.")
                return

        msg = msg_query_status(
            sender="CLIENT",
            lamport=self.clock.send(),
            token=self.auth.get_token(),
            task_id=task_id,
        )
        send_msg(self._sock, msg)

        resp = recv_msg(self._sock)
        if resp is None:
            print("  ✗ Sem resposta do orquestrador.")
            return

        self.clock.receive(resp.get("lamport", 0))

        if resp.get("type") == MSG_TASK_STATUS:
            status = resp.get("status", "?")
            result = resp.get("result")
            print(f"\n  📋 Tarefa: {task_id}")
            print(f"     Status: {status}")
            if result is not None:
                print(f"     Resultado: {result}")
        elif resp.get("type") == MSG_ERROR:
            print(f"  ✗ Erro: {resp.get('reason', '?')}")
        else:
            print(f"  ✗ Resposta inesperada: {resp.get('type')}")

    # ── Submissão em lote ────────────────────────────────────

    def do_batch_submit(self) -> None:
        """Submete múltiplas tarefas de uma vez (para teste)."""
        if not self.auth.is_authenticated():
            print("\n  ✗ Faça login primeiro!")
            return

        print("\n── Submissão em Lote ──────────────")
        count_str = input("  Quantas tarefas? ").strip()
        try:
            count = int(count_str)
        except ValueError:
            print("  ✗ Número inválido.")
            return

        import random
        ops = ["sum", "multiply", "sort", "count", "upper"]

        for i in range(count):
            op = random.choice(ops)
            if op == "upper":
                data = [f"texto{j}" for j in range(random.randint(2, 5))]
            else:
                data = [random.randint(1, 100) for _ in range(random.randint(3, 8))]

            task_id = f"T-{uuid.uuid4().hex[:8].upper()}"
            payload = {"operation": op, "data": data}

            msg = msg_submit_task(
                sender="CLIENT",
                lamport=self.clock.send(),
                token=self.auth.get_token(),
                task_id=task_id,
                payload=payload,
            )
            send_msg(self._sock, msg)

            resp = recv_msg(self._sock)
            if resp and resp.get("type") == MSG_TASK_ACCEPTED:
                self._submitted_tasks.append(task_id)
                print(f"  ✓ [{i+1}/{count}] {task_id} ({op}) aceita")
                self.clock.receive(resp.get("lamport", 0))
            else:
                reason = resp.get("reason", "?") if resp else "sem resposta"
                print(f"  ✗ [{i+1}/{count}] {task_id} rejeitada: {reason}")

        print(f"\n  Total submetidas: {len(self._submitted_tasks)}")

    # ── Consulta de todas as tarefas ─────────────────────────

    def do_query_all(self) -> None:
        """Consulta o status de todas as tarefas submetidas."""
        if not self.auth.is_authenticated():
            print("\n  ✗ Faça login primeiro!")
            return

        if not self._submitted_tasks:
            print("\n  ✗ Nenhuma tarefa submetida nesta sessão.")
            return

        print("\n── Status de Todas as Tarefas ─────")
        for tid in self._submitted_tasks:
            msg = msg_query_status(
                sender="CLIENT",
                lamport=self.clock.send(),
                token=self.auth.get_token(),
                task_id=tid,
            )
            send_msg(self._sock, msg)
            resp = recv_msg(self._sock)

            if resp and resp.get("type") == MSG_TASK_STATUS:
                self.clock.receive(resp.get("lamport", 0))
                status = resp.get("status", "?")
                result = resp.get("result")
                line = f"  {tid}: {status}"
                if result is not None:
                    line += f" → {result}"
                print(line)
            else:
                print(f"  {tid}: ???")

    # ── Menu principal ───────────────────────────────────────

    def run(self) -> None:
        """Loop principal do menu interativo."""
        print("╔══════════════════════════════════════════════╗")
        print("║  Plataforma Distribuída — Cliente Interativo ║")
        print("╚══════════════════════════════════════════════╝")

        if not self.connect():
            return

        try:
            while True:
                print("\n── Menu ───────────────────────────")
                if self.auth.is_authenticated():
                    user = self.auth.get_username()
                    print(f"  Logado como: {user}")
                print("  1. Login")
                print("  2. Submeter tarefa")
                print("  3. Consultar status de tarefa")
                print("  4. Submissão em lote (teste)")
                print("  5. Status de todas as tarefas")
                print("  0. Sair")

                choice = input("\n  Opção: ").strip()

                if choice == "1":
                    self.do_login()
                elif choice == "2":
                    self.do_submit()
                elif choice == "3":
                    self.do_query()
                elif choice == "4":
                    self.do_batch_submit()
                elif choice == "5":
                    self.do_query_all()
                elif choice == "0":
                    print("\n  Até logo!")
                    break
                else:
                    print("  ✗ Opção inválida.")

        except KeyboardInterrupt:
            print("\n\n  Saindo...")
        except (ConnectionResetError, BrokenPipeError):
            print("\n  ✗ Conexão com o orquestrador perdida!")
        finally:
            self.disconnect()


# ════════════════════════════════════════════════════════════
#  Ponto de entrada
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    client = Client()
    client.run()