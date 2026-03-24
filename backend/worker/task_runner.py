# ============================================================
#  worker/task_runner.py
#  Simulador de processamento de tarefas
#
#  Suporta operações simples sobre dados numéricos/textuais.
#  Simula tempo de processamento e falha aleatória (~10%)
#  para testar redistribuição de tarefas.
# ============================================================

import random
import time


# Chance de falha simulada (0.0 a 1.0)
FAILURE_CHANCE = 0.10

# Tempo de processamento simulado (segundos)
MIN_PROC_TIME = 2.0
MAX_PROC_TIME = 5.0


class TaskRunner:
    """
    Executa uma tarefa simulada.

    Payload esperado:
        {
            "operation": "sum" | "multiply" | "sort" | "count" | "upper",
            "data": [...]   (lista de números ou strings)
        }

    Retorna:
        (True,  resultado)   em caso de sucesso
        (False, mensagem)    em caso de falha
    """

    OPERATIONS = {
        "sum":      lambda data: sum(data),
        "multiply": lambda data: _product(data),
        "sort":     lambda data: sorted(data),
        "count":    lambda data: len(data),
        "upper":    lambda data: [str(x).upper() for x in data],
    }

    def run(self, task_id: str, payload: dict,
            simulate_failure: bool = True) -> tuple:
        """
        Executa a tarefa e retorna (success, result_or_error).
        """
        operation = payload.get("operation", "")
        data      = payload.get("data", [])

        # Valida operação
        if operation not in self.OPERATIONS:
            return False, f"Operação desconhecida: {operation}"

        # Simula tempo de processamento
        proc_time = random.uniform(MIN_PROC_TIME, MAX_PROC_TIME)
        time.sleep(proc_time)

        # Simula falha aleatória
        if simulate_failure and random.random() < FAILURE_CHANCE:
            return False, f"Falha simulada durante processamento de {task_id}"

        # Executa operação
        try:
            result = self.OPERATIONS[operation](data)
            return True, result
        except Exception as e:
            return False, f"Erro na operação '{operation}': {e}"


def _product(data: list) -> float:
    """Multiplica todos os elementos da lista."""
    result = 1
    for x in data:
        result *= x
    return result