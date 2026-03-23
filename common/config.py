# portas, IPs, timeouts, constantes

# Rede 
ORCHESTRATOR_HOST = "127.0.0.1"
ORCHESTRATOR_PORT = 5000          # TCP: clientes → orquestrador
 
BACKUP_HOST       = "127.0.0.1"
BACKUP_PORT       = 5001          # TCP: backup escuta (failover)
 
WORKER_BASE_PORT  = 6000          # Workers usam 6000, 6001, 6002 …
 
# UDP Multicast (orquestrador → backup) 
MULTICAST_GROUP   = "224.1.1.1"
MULTICAST_PORT    = 7000
MULTICAST_TTL     = 1             # Limita ao segmento local
 
# Heartbeat 
HEARTBEAT_INTERVAL   = 2          # segundos entre batimentos
HEARTBEAT_TIMEOUT    = 6          # sem resposta → worker morto
 
# Autenticação 
# Usuários pré-cadastrados  {usuario: senha}
USERS = {
    "alice": "senha123",
    "bob":   "abc456",
    "carol": "qwerty",
}

TOKEN_SECRET = "distributed-sys-secret-2024"
 
# Tarefas 
TASK_TIMEOUT      = 30            # segundos até considerar tarefa travada
MAX_RETRIES       = 3             # tentativas antes de marcar como falha
 
# Logs
LOG_FILE          = "logs/system.log"
LOG_LEVEL         = "DEBUG"       # DEBUG | INFO | WARNING | ERROR