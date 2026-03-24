# Plataforma Distribuída de Processamento de Tarefas

Um sistema distribuído completo construído do zero, focado em tolerância a falhas, balanceamento de carga e sincronização de estado. O backend principal é feito em Python puro usando sockets TCP/UDP, e o frontend é uma interface web responsiva e em tempo real feita com Vite/Vanilla JS.

## Arquitetura do Sistema

O projeto é dividido em três partes integradas:

1. **Backend (Python Sockets)**: O núcleo duro das regras distribuídas.
   - **Orquestrador Principal**: Recebe as tarefas, distribui para os workers via Round Robin e controla todo o estado das sessões em tempo real.
   - **Orquestrador Backup**: Fica escutando passivamente a rede via UDP Multicast. Caso o orquestrador principal caia abruptamente, ele assume instantaneamente (sistema ativo-passivo / Failover).
   - **Workers**: Nós de processamento pesado da rede que processam as requisições, reportam resultados e mandam "heartbeats" periódicos atestando que estão vivos.
2. **API Bridge (Flask)**: Uma camada extra que conecta a arquitetura bruta de sockets TCP com a web, expondo endpoints RESTful padronizados para o frontend consumir e gerenciando os pools de comunicação.
3. **Frontend Web (Vite + JS)**: Dashboard interativo para fazer login, submeter processos individuais ou engatilhar testes em lote pesados (batch submission), monitorando tudo atualizado na tela.

## Pré-requisitos

Para conseguir rodar esse projeto localmente você precisa ter instalado:
- **Python 3.10+**
- **Node.js 18+** (para subir o frontend)

## Instalação

Logo após clonar o repositório, você vai precisar instalar as dependências de biblioteca do backend para a Web Bridge e os pacotes NPM do front:

```bash
# 1. Instalar as dependências da API (Flask e CORS)
cd backend
pip install flask flask-cors

# 2. Instalar as dependências do dashboard visual
cd ../frontend
npm install
```

## Como Executar (Passo a Passo)

Por ser um sistema com vários nós conversando entre si, você precisa rodar cada parte em um terminal diferente (pode criar abas diferentes no VS Code ou no seu terminal de preferência). Fica algo assim:

### 1. Iniciar a Base da Rede TCP
No seu terminal, dentro do diretório `backend/`:

```bash
# Terminal 1 - Orquestrador Principal
python -m orchestrator.orchestrator

# Terminal 2 - Orquestrador Backup (pra gente poder testar a segurança)
python -m backup.backup_orchestrator

# Terminais 3, 4 e 5 - Liga os Workers 
# (Você pode ligar quantos achar necessário para aguentar o tranco)
python -m worker.worker --id worker-1 --index 0
python -m worker.worker --id worker-2 --index 1
python -m worker.worker --id worker-3 --index 2
```

### 2. Iniciar a API Bridge
Ainda no diretório `backend/`, em outra nova aba:
```bash
python -m api.api_server
```

### 3. Iniciar o Painel de Frontend
Por último, no diretório `frontend/`, roda o servidor do Vite com o projeto visual:
```bash
npm run dev
```

Tudo rodando, só abrir o seu navegador em: `http://localhost:5173/`

*(Nota: Caso goste de raízes, pode ignorar o frontend/API e usar o nosso cliente CLI rodando `python -m client.client` lá dentro da pasta do backend).*

## Exemplos Reais de Uso

1. **Testando Autenticação**: Acesse a interface web e logue com as nossas seeds do sistema. Exemplo: usuário `alice` com a senha `senha123` (outros possíveis: `bob/abc456` ou `carol/qwerty`).
2. **Estresse em Lote**: No formulário "Submissão em Lote", coloque um número alto como 30 ou 50 e submeta. O painel web criará requisições variando tarefas matemáticas ou manipulação de string e estressará a sua grid ao máximo.
3. **Visão ao Vivo**: A tabela do dashboard e os contadores fazem pooling inteligente. Se você pedir 30 tarefas pra base com 3 workers, começará a ver o status pular dinamicamente de "PENDING" pra "RUNNING" pra "COMPLETED".
4. **Simulando um Caos (Failover)**:
   - Com os workers e requisições engatilhados sendo processadas, vá na aba do seu terminal onde o **Orquestrador Principal** roda e feche sem dar tchau (matando o terminal ou `Ctrl+C`).
   - Olhe o terminal do **Orquestrador Backup**. Ele instantaneamente assumirá, identificará do último pacote de rede onde tudo parou e não perderá NENHUMA transação do sistema. O cliente final não notará.
5. **Matando Operários**: Envie uma tarefa e force o encerro de um Worker no meio do processamento. O orquestrador perceberá que o _heartbeat_ do nó sumiu, declarará ele como falho, e jogará o processo original na mesa de outro Worker para finalizar.

## Conceitos Teóricos de Distruibuição que Foram Aplicados

* **Relógio Lógico de Lamport**: Utilizado internamente para ordenação casual precisa dos eventos distribuídos. Você consegue analisar tudo organizadinho por timestamp de Lamport observando os logs unificados (`backend/logs/system.log`).
* **Sincronização de Visão do Estado Central (State Syncing)**: O nós e orquestrador ativo emitem a radiografia da pilha de conexões em tempo via infra UDP Multicast (endereço limpo na base 224.1.1.1:7000). O servidor redundante lê e se clona nisso.
* **Tolerância a Furos Na Malha**: Além do failover do servidor base mencionado, todo rastreio das pontas dos Worker roda timeouts hard-limit: tem 2s para pingarem sinal de vida - em falhas há retentativas automáticas e dead checks transparentes a UI principal.
* **Algoritmo Limitado por Carga Cíclica**: Os pacotes/tarefas sempre vão distribuídas usando Round Robin (cada pacote pra vez da fila entre nós). Nada complexo, mas muito escalável.
