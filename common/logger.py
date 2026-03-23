# log centralizado com timestamp Lamport
# ============================================================
#  common/logger.py
#  Logger centralizado — grava arquivo + console
#  Cada linha inclui: timestamp real | Lamport | nível | msg
# ============================================================
 
import logging
import os
import sys
from datetime import datetime
from typing import Optional
 
from common.config import LOG_FILE, LOG_LEVEL
 
 
# Formato de linha de log 
#  2024-11-01 14:32:05.123 | L:007 | ORCHESTRATOR | INFO  | Tarefa T-42 distribuída para worker-2
_FMT = "%(asctime)s | L:%(lamport)-3s | %(node)-14s | %(levelname)-5s | %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"
 
 
class LamportFilter(logging.Filter):
    """
    Injeta os campos extras 'lamport' e 'node' nos LogRecords
    para que o formatter possa usá-los.
    """
 
    def __init__(self, node_name: str, clock=None):
        super().__init__()
        self.node_name = node_name
        self.clock     = clock          # instância de LamportClock (opcional)
 
    def filter(self, record: logging.LogRecord) -> bool:
        record.lamport = self.clock.value if self.clock else "---"
        record.node    = self.node_name
        return True
 
 
# Fábrica de loggers
 
_loggers: dict = {}
 
 
def get_logger(
    node_name: str = "SYSTEM",
    clock=None,
    log_file: str = LOG_FILE,
    level: str = LOG_LEVEL,
) -> logging.Logger:
    """
    Retorna (ou cria) um logger nomeado.
 
    Parâmetros:
        node_name : identificação do nó (ex: "ORCHESTRATOR", "WORKER-1")
        clock     : instância de LamportClock para incluir o tempo lógico
        log_file  : caminho do arquivo de log (padrão: LOG_FILE em config.py)
        level     : nível mínimo de log ("DEBUG", "INFO", …)
 
    Exemplo:
        from common.logger import get_logger
        from common.lamport import LamportClock
 
        clock  = LamportClock()
        logger = get_logger("WORKER-1", clock)
        logger.info("Pronto para receber tarefas")
    """
    global _loggers
 
    if node_name in _loggers:
        return _loggers[node_name]
 
    logger = logging.getLogger(node_name)
    logger.setLevel(getattr(logging, level.upper(), logging.DEBUG))
    logger.propagate = False
 
    formatter = logging.Formatter(_FMT, datefmt=_DATE_FMT)
 
    # Handler: console (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
 
    # Handler: arquivo
    _ensure_log_dir(log_file)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
 
    # Filtro que injeta lamport e node
    lamport_filter = LamportFilter(node_name, clock)
    logger.addFilter(lamport_filter)
 
    _loggers[node_name] = logger
    return logger
 
 
def update_clock(node_name: str, clock) -> None:
    """
    Atualiza o relógio associado a um logger já criado.
    Útil quando o clock é criado depois do logger.
    """
    if node_name in _loggers:
        for f in _loggers[node_name].filters:
            if isinstance(f, LamportFilter):
                f.clock = clock
 
 
def _ensure_log_dir(path: str) -> None:
    """Cria o diretório de logs se não existir."""
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
 
 
# Funções de log de eventos específicos 
# Convenção: todo evento importante usa uma dessas funções
# para garantir formato uniforme nos logs de evidência.
 
def log_task_submitted(logger, task_id: str, client: str, lamport: int):
    logger.info(f"[TASK_SUBMITTED] task={task_id} client={client} lamport={lamport}")
 
def log_task_distributed(logger, task_id: str, worker: str, lamport: int):
    logger.info(f"[TASK_DISTRIBUTED] task={task_id} worker={worker} lamport={lamport}")
 
def log_task_completed(logger, task_id: str, worker: str, lamport: int):
    logger.info(f"[TASK_COMPLETED] task={task_id} worker={worker} lamport={lamport}")
 
def log_task_failed(logger, task_id: str, worker: str, reason: str, lamport: int):
    logger.warning(f"[TASK_FAILED] task={task_id} worker={worker} reason={reason} lamport={lamport}")
 
def log_task_reassigned(logger, task_id: str, old_worker: str, new_worker: str, lamport: int):
    logger.warning(f"[TASK_REASSIGNED] task={task_id} from={old_worker} to={new_worker} lamport={lamport}")
 
def log_worker_down(logger, worker_id: str, lamport: int):
    logger.error(f"[WORKER_DOWN] worker={worker_id} lamport={lamport}")
 
def log_failover(logger, from_node: str, to_node: str, lamport: int):
    logger.critical(f"[FAILOVER] from={from_node} to={to_node} lamport={lamport}")
 
def log_heartbeat(logger, worker_id: str, lamport: int):
    logger.debug(f"[HEARTBEAT] worker={worker_id} lamport={lamport}")
 
 
# Teste rápido 
if __name__ == "__main__":
    from common.lamport import LamportClock
 
    clock  = LamportClock()
    logger = get_logger("TEST-NODE", clock, log_file="logs/test.log")
 
    clock.tick()
    logger.debug("Evento de debug")
 
    clock.tick()
    logger.info("Sistema iniciado")
 
    log_task_submitted(logger, "T-001", "alice", clock.send())
    log_task_distributed(logger, "T-001", "worker-1", clock.send())
    log_task_completed(logger, "T-001", "worker-1", clock.send())
    log_worker_down(logger, "worker-2", clock.tick())
    log_task_reassigned(logger, "T-002", "worker-2", "worker-3", clock.tick())
    log_failover(logger, "ORCHESTRATOR-PRIMARY", "ORCHESTRATOR-BACKUP", clock.tick())
 
    print("\nLog gravado em logs/test.log ✓")