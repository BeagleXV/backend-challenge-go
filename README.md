# Processamento Distribuído de Apostas

Serviço em Go (Uber Fx) que processa operações financeiras de provedores de jogos (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) sobre carteiras de jogadores, via HTTP e SQS, com garantias de idempotência, concorrência segura e recuperação de falhas.

Este arquivo documenta a **solução implementada**. O enunciado original do desafio está preservado em [`docs/DESAFIO.md`](docs/DESAFIO.md); as decisões de arquitetura e seus trade-offs, em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Pré-requisitos

- Go 1.27+
- Docker e Docker Compose v2 (`docker compose`, não `docker-compose`)
- [`golang-migrate`](https://github.com/golang-migrate/migrate) CLI, apenas se for aplicar migrations fora do Docker Compose (`brew install golang-migrate` ou baixar o binário)

## Subindo tudo com Docker Compose (recomendado)

```sh
docker compose up --build
```

Isso sobe, nesta ordem (via healthchecks e `depends_on`): Postgres, Keycloak (com o realm de desenvolvimento importado automaticamente, ver abaixo) e LocalStack (com as filas FIFO + DLQ + redrive já provisionadas por `deploy/localstack/init-queues.sh`); em seguida um serviço `migrate` aplica as migrations e sai; só então a API sobe. Nenhum passo manual é necessário.

A API fica em `http://localhost:8080` e as métricas Prometheus em `http://localhost:9090/metrics` (porta de host efêmera se você escalar réplicas — veja abaixo).

Para derrubar tudo (incluindo os volumes de dados):

```sh
docker compose down -v
```

### Múltiplas instâncias

```sh
docker compose up --build --scale app=3
```

As portas do serviço `app` não têm mapeamento fixo de host propositalmente (`127.0.0.1::8080`), então cada réplica recebe uma porta de host efêmera e não há conflito. Liste as portas de cada réplica com:

```sh
docker compose ps app
```

Cada réplica consome a mesma fila SQS e disputa o mesmo Postgres de forma segura — é a mesma propriedade provada com processos reais em `internal/integration/concurrency_test.go` (Fase 16).

### Obtendo um token para testar manualmente

O Keycloak do compose só aceita, para validação pela API, tokens pedidos pela **rede interna do compose** (`http://keycloak:8080/...`) — não pela porta publicada no host (`127.0.0.1:8081`, útil só para o console administrativo). O motivo e os detalhes estão em `docs/ARCHITECTURE.md`, seção 15.

Use o serviço auxiliar `cli` (nunca sobe com `up` simples — fica atrás do profile `tools`):

```sh
docker compose run --rm cli -c '
  curl -s -X POST http://keycloak:8080/realms/wagering/protocol/openid-connect/token \
    -d grant_type=client_credentials \
    -d client_id=internal-service \
    -d client_secret=internal-service-secret-local-only
'
```

Extraia o campo `access_token` da resposta JSON. Identidades de teste disponíveis (todas só no realm de desenvolvimento, nunca válidas contra um IdP real):

| `client_id` | `client_secret` | Uso |
| --- | --- | --- |
| `internal-service` | `internal-service-secret-local-only` | Único autorizado a abrir carteiras (`POST /wallets`) e rodar reconciliação |
| `provider-a` | `provider-a-secret-local-only` | Provedor externo de exemplo |
| `provider-b` | `provider-b-secret-local-only` | Segundo provedor, para testar isolamento entre provedores |
| `provider-a-shortlived` | `provider-a-shortlived-secret-local-only` | Como `provider-a`, mas o token expira em ~2s — para testar expiração real |

## Rodando a aplicação localmente (sem Docker para a app)

Suba só a infraestrutura:

```sh
docker compose up postgres keycloak localstack
```

Aplique as migrations (com a CLI `migrate` instalada localmente):

```sh
cp .env.example .env   # ajuste se necessário
export $(grep -v '^#' .env | xargs)
make migrate-up DATABASE_URL="postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$POSTGRES_PORT/$POSTGRES_DB?sslmode=$POSTGRES_SSLMODE"
```

Rode a API:

```sh
export $(grep -v '^#' .env | xargs)
make run
```

Nesse modo (app rodando no host), `OIDC_ISSUER_URL=http://localhost:8081/realms/wagering` em `.env.example` já é o endereço correto para pedir tokens — o container do Keycloak reflete o `Host` da requisição no `iss`, e tanto a app quanto o `curl` do host usam o mesmo `localhost:8081`.

## Variáveis de ambiente

Ver `.env.example` para a lista completa com valores de desenvolvimento. Resumo por área:

| Variável | Obrigatória | Descrição |
| --- | --- | --- |
| `APP_ENV`, `APP_HTTP_ADDR`, `APP_SHUTDOWN_TIMEOUT` | não (têm default) | Ambiente, endereço HTTP, timeout de shutdown gracioso |
| `POSTGRES_HOST`, `_PORT`, `_DB`, `_USER`, `_PASSWORD` | sim | Conexão com Postgres — sem default, uma app financeira não deve silenciosamente assumir um banco |
| `POSTGRES_SSLMODE`, `POSTGRES_MAX_CONNS` | não | `disable`/`10` por padrão |
| `OIDC_ISSUER_URL`, `OIDC_AUDIENCE` | sim | Issuer e audience esperados nos tokens — sem default, um endpoint financeiro nunca deve aceitar tokens de um issuer não configurado explicitamente |
| `AWS_REGION` | sim | Região SQS |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `SQS_ENDPOINT` | não | Vazios, o SDK usa a cadeia de credenciais padrão da AWS (produção); com LocalStack, `test`/`test` + endpoint local |
| `SQS_WAGER_TRANSACTIONS_QUEUE_URL`, `_DLQ_URL`, `SQS_EVENTS_QUEUE_URL` | sim | URLs completas das 3 filas |
| `LOG_LEVEL`, `METRICS_ADDR` | não | `info`/`:9090` por padrão |

A falha em qualquer variável obrigatória derruba o processo imediatamente, antes de montar o grafo do Fx, com uma mensagem clara — nunca uma falha tardia e confusa de conexão.

## Migrations

9 migrations versionadas em `migrations/` (`golang-migrate`, `NNNNNN_nome.up.sql`/`.down.sql`), independentes da versão de Go da aplicação:

```sh
migrate -path migrations -database "$DATABASE_URL" up      # ou: make migrate-up DATABASE_URL=...
migrate -path migrations -database "$DATABASE_URL" down 1  # reverte uma migration
```

No Docker Compose isso é automático (serviço `migrate`). Todas as 9 já foram validadas de ponta a ponta: `up` completo, `down` completo (schema volta a só ter `schema_migrations`), `up` reaplicado sem erro.

## Documentação da API (Swagger/OpenAPI)

O contrato HTTP completo está em [`docs/openapi.yaml`](docs/openapi.yaml) (OpenAPI 3.0 — rotas, schemas de request/response, códigos de erro e o esquema de autenticação). Para navegar num Swagger UI local:

```sh
docker compose --profile docs up -d swagger-ui
```

Abra `http://localhost:8082`. É só documentação — o serviço fica atrás do profile `docs` (nunca sobe com `docker compose up` simples) e não faz parte da imagem da aplicação.

```sh
docker compose --profile docs down   # derrubar só o swagger-ui
```

## Exemplos de chamadas (curl)

Com um token de `internal-service` em `$TOKEN` (veja acima) e a API em `http://localhost:8080`:

```sh
# Abrir uma carteira com saldo inicial
curl -s -X POST http://localhost:8080/wallets \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'
# -> {"id":"...","playerId":"...","balance":{"amount":"1000.00","currency":"BRL"},"version":1}

# Consultar a carteira
curl -s http://localhost:8080/wallets/<walletId> -H "Authorization: Bearer $TOKEN"

# Ledger paginado
curl -s "http://localhost:8080/wallets/<walletId>/ledger?limit=50" -H "Authorization: Bearer $TOKEN"

# Reconciliação (saldo armazenado vs. reconstruído do ledger)
curl -s -X POST http://localhost:8080/wallets/<walletId>/reconciliation -H "Authorization: Bearer $TOKEN"
```

Com um token de `provider-a` em `$PROVIDER_TOKEN`:

```sh
# Submeter uma aposta (200 PROCESSED, 202 PENDING_REFERENCE ou 422 REJECTED)
curl -s -X POST http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_TOKEN" -H "Content-Type: application/json" \
  -H "Idempotency-Key: provider-a:transaction-123" \
  -d '{
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "<walletId>",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": {"amount": "25.00", "currency": "BRL"}
  }'
# -> {"transactionId":"...","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}

# Consultar por id, ou por (providerId, externalTransactionId)
curl -s http://localhost:8080/wagering/transactions/<transactionId> -H "Authorization: Bearer $PROVIDER_TOKEN"
curl -s http://localhost:8080/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PROVIDER_TOKEN"
```

Reenviar a mesma chamada com o mesmo `Idempotency-Key` e corpo devolve o resultado original com `idempotentReplay: true`; o mesmo `Idempotency-Key` com corpo diferente retorna conflito (`409`). Para `REFUND`/`ROLLBACK`, acrescente `"referenceExternalTransactionId"` ao corpo.

### Enviando a mesma operação via SQS

O consumidor lê `wager-transactions.fifo` esperando o mesmo formato de operação, envelopado:

```json
{
  "messageId": "<uuid>",
  "type": "WagerTransactionSubmitted",
  "occurredAt": "2026-01-01T00:00:00Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "<uuid>",
    "walletId": "<uuid>",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": {"amount": "25.00", "currency": "BRL"},
    "referenceExternalTransactionId": ""
  }
}
```

`MessageGroupId` deve ser o `walletId` (garante ordenação FIFO por carteira) e `MessageDeduplicationId` o `messageId`. O mesmo `Handle` que a rota HTTP usa processa a mensagem — o resultado (inclusive idempotência) é idêntico entre os dois transportes.

### Health checks

```sh
curl -s http://localhost:8080/health/live    # processo de pé, sem tocar dependências
curl -s http://localhost:8080/health/ready   # 200 só se Postgres e SQS respondem
```

## Testes

```sh
go test ./...             # suíte completa de unidade (259 testes)
go test -race ./...       # mesma suíte, com o detector de race
go vet ./...
gofmt -l .                # deve não imprimir nada
```

### Testes de integração (build tag `integration`, infraestrutura real)

Requerem Docker rodando — sobem containers efêmeros de Postgres, Keycloak e LocalStack via testcontainers-go, nunca mocks:

```sh
go test -tags=integration ./...
```

Ou por área, se preferir não esperar a suíte inteira:

```sh
# Adapters isolados (Postgres: locks, constraints, imutabilidade do ledger, outbox, inbox; LocalStack: publicação SQS real)
go test -tags=integration ./internal/adapters/postgres/... ./internal/adapters/outboxpublisher/...

# Suíte full-stack: migrations up/down, composição Fx real, HTTP+SQS ponta a ponta, DLQ, restart
go test -tags=integration ./internal/integration/...
```

### Concorrência e recuperação (cenários obrigatórios da seção 13 do desafio)

Fazem parte da suíte de integração acima (mesma build tag). Destaques que valem rodar isoladamente:

```sh
# Disputa de duas apostas de 80.00 sobre saldo de 100.00
go test -tags=integration ./internal/integration/... -run TestConcurrency_TwoBetsDisputingInsufficientBalance_OneWinsOneRejected -v

# >= 3 processos reais do binário compilado (não goroutines) disputando a mesma aposta
go test -tags=integration ./internal/integration/... -run TestConcurrency_ThreeRealProcesses -v -timeout 5m

# Carteiras distintas não serializam entre si (prova determinística via canais, sem timing)
go test -tags=integration ./internal/adapters/postgres/... -run TestWalletRepository_GetForUpdate_DistinctWallets_DoNotSerializeAgainstEachOther -v

# Restart: REFUND deixado PENDING_REFERENCE por uma instância é resolvido por outra, nova
go test -tags=integration ./internal/integration/... -run TestConcurrency_RestartRecovery_PendingReferenceResolvedByFreshInstance -v -timeout 3m
```

O mapeamento completo dos 8 cenários obrigatórios para os testes que os provam está documentado em `docs/ARCHITECTURE.md`.

## Observabilidade

- Logs estruturados em JSON (zap) em stdout — nunca segredos, tokens ou payload financeiro completo.
- Métricas Prometheus em `/metrics` (porta `METRICS_ADDR`, separada da API de negócio).
- `GET /health/live` e `/health/ready` (públicos, sem autenticação).

Detalhes de quais métricas existem e por quê em `docs/ARCHITECTURE.md`, seção 14.

## Estrutura do projeto

Arquitetura hexagonal: `internal/domain` (regras de negócio puras, sem nenhuma dependência de Fx/HTTP/SQS/pgx) → `internal/application` (casos de uso, via interfaces em `ports`) → `internal/adapters` (Postgres, SQS, HTTP, IdP) → `internal/fxmodules` (única camada que conhece Fx). Ver `docs/ARCHITECTURE.md` para o detalhamento completo, decisão por decisão.
