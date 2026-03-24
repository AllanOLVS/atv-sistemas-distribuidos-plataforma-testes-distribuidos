# ============================================================
#  client/auth.py
#  Helper de autenticação do cliente
#
#  Gerencia o fluxo de login: envia MSG_LOGIN ao orquestrador,
#  recebe token e o armazena para uso nas chamadas seguintes.
# ============================================================

from common.lamport  import LamportClock
from common.protocol import (
    MSG_LOGIN_OK,
    msg_login, send_msg, recv_msg,
)


class Auth:
    """
    Gerencia autenticação de um cliente.

    Uso:
        auth = Auth(clock)
        token = auth.login(sock, "alice", "senha123")
        if auth.is_authenticated():
            # submeter tarefas usando auth.get_token()
    """

    def __init__(self, clock: LamportClock, sender: str = "CLIENT"):
        self.clock   = clock
        self.sender  = sender
        self._token  = None
        self._username = None

    def login(self, sock, username: str, password: str) -> str | None:
        """
        Envia MSG_LOGIN e aguarda resposta.
        Retorna o token se bem-sucedido, None caso contrário.
        """
        msg = msg_login(
            sender=self.sender,
            lamport=self.clock.send(),
            username=username,
            password=password,
        )
        send_msg(sock, msg)

        resp = recv_msg(sock)
        if resp is None:
            return None

        self.clock.receive(resp.get("lamport", 0))

        if resp.get("type") == MSG_LOGIN_OK:
            self._token    = resp.get("token", "")
            self._username = username
            return self._token
        else:
            return None

    def is_authenticated(self) -> bool:
        return self._token is not None

    def get_token(self) -> str:
        return self._token or ""

    def get_username(self) -> str:
        return self._username or ""