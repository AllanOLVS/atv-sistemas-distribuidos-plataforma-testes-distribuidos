# Cenarios formais de falha (evidencia)

## Objetivo
Validar continuidade de tarefas RUNNING durante failover sem perda/duplicacao.

## Pre-condicoes
- Primario, backup e 2+ workers ativos.
- API e frontend opcionais para observacao.

## Cenario A - Failover durante RUNNING
1. Submeter tarefa longa (ex.: operacao com atraso no worker).
2. Confirmar estado RUNNING.
3. Derrubar primario abruptamente.
4. Aguardar promocao do backup.
5. Verificar logs com eventos:
   - FAILOVER
   - TASK_RECOVERY_MARKED
   - (opcional) TASK_RECOVERY_EXPIRED
   - TASK_COMPLETED
6. Critico: para cada task_id, deve existir no maximo 1 conclusao final.

## Cenario B - Resultado tardio do worker antigo
1. Em failover, forcar reatribuicao apos expirar janela de recuperacao.
2. Fazer o worker antigo enviar resultado tardio.
3. Verificar log TASK_LATE_RESULT_IGNORED.
4. Confirmar que o estado final da tarefa nao foi sobrescrito.

## Cenario C - Falha de worker com reatribuicao
1. Submeter tarefa.
2. Matar worker dono da tarefa RUNNING.
3. Confirmar reatribuicao para PENDING e novo despacho.
4. Validar conclusao unica.

## Evidencias para anexar
- Saida de `pytest -q backend/tests`
- Trechos de `logs/system.log` com os eventos acima
- (Opcional) captura da tela do dashboard/status API
