# ARCHITECTURE.md

Decisões de arquitetura do serviço de processamento distribuído de apostas: representação de dinheiro, transações, idempotência, locks, referências pendentes, reversões, inbox/outbox, autenticação, autorização, uso do Uber Fx, shutdown, limitações e trabalho não concluído.

## Stack tecnológica

| Responsabilidade | Escolha | Motivo |
| --- | --- | --- |
| Roteador HTTP | [chi](https://github.com/go-chi/chi) | Compatível com `http.Handler` puro, mantendo o domínio isolado de framework HTTP; middlewares leves (RequestID, Recoverer) sem impor abstrações próprias sobre a requisição. |
| Acesso a banco | pgx v5, SQL explícito | Controle total sobre transação e locks, verificável linha a linha. |
| Representação de dinheiro | `int64` em unidades mínimas (centavos) | Elimina float de qualquer ponto do cálculo/parsing/persistência; overflow checável explicitamente. Escopo: BRL, USD, EUR (todas com escala 2 casas). |
| Migrations | [golang-migrate](https://github.com/golang-migrate/migrate) | CLI/imagem Docker independente da versão de Go da aplicação; up/down versionados. |
| Mensageria local | LocalStack (SQS) | Suporte a FIFO, DLQ e redrive policy. |
| IdP | Keycloak | OAuth2/OIDC com `client_credentials`, provisionado via realm export. |
| Validação de token | [go-oidc](https://github.com/coreos/go-oidc) + `golang-jwt/jwt/v5` | Discovery de issuer, cache de JWKS e validação de claims por biblioteca madura, reduzindo risco de bug de segurança em validação feita à mão. |
| Logging | [zap](https://github.com/uber-go/zap) | Logging estruturado em JSON. |
| Métricas | OpenTelemetry Metrics + exporter Prometheus | `/metrics` compatível com Prometheus, com a mesma inicialização servindo para tracing no futuro. |
| Testes | testify + testcontainers-go | Testes de integração contra Postgres, Keycloak e LocalStack reais, não mocks. |
| Concorrência por carteira | Lock pessimista `SELECT ... FOR UPDATE` | Serializa apenas a linha da carteira envolvida; carteiras diferentes continuam em paralelo. |
| Composição | Uber Fx | `fx.Module`/`fx.Provide`/`fx.Invoke`/`fx.Lifecycle`, com o domínio (`internal/domain`) livre de qualquer dependência de Fx, HTTP, SQS ou pgx. |

Organização de pacotes (arquitetura hexagonal): `internal/domain` contém as regras de negócio puras; `internal/application` orquestra o domínio através de interfaces (`ports`); `internal/adapters` implementa essas interfaces com bibliotecas concretas (Postgres, SQS, HTTP, IdP); `internal/fxmodules` é a única camada que conhece Fx.

## 1. Money

*A ser detalhado: representação (`int64` em centavos), moedas suportadas (BRL/USD/EUR), tratamento de overflow em soma/subtração/negação, validações de entrada (NaN, notação científica, escala excedente), normalização aplicada antes do hash de idempotência.*

## 2. Wallet e controle de concorrência

*A ser detalhado: invariantes do agregado, semântica do campo `version`, estratégia de lock pessimista e por que foi escolhida em vez de controle otimista.*

## 3. Transações SQL

*A ser detalhado: delimitação exata de onde cada transação começa/termina, por caso de uso.*

## 4. Máquina de estados de WagerTransaction

*A ser detalhado: transições válidas entre `PENDING`, `PENDING_REFERENCE`, `PROCESSED`, `REJECTED`, `FAILED`; distinção entre falha transitória e falha permanente.*

## 5. Reversões (REFUND / ROLLBACK)

*A ser detalhado: como o sistema impede dupla reversão da mesma referência e como combina REFUND e ROLLBACK sobre a mesma aposta.*

## 6. Referências pendentes

*A ser detalhado: política de backoff exponencial, número máximo de tentativas ou TTL, códigos de falha.*

## 7. Idempotência

*A ser detalhado: algoritmo de hash canônico, campos incluídos/excluídos, equivalência de comportamento entre entrada HTTP e SQS.*

## 8. Inbox / Outbox

*A ser detalhado: garantias de atomicidade entre alteração de domínio, inbox e outbox; disputa entre múltiplos publishers; recuperação de trabalho abandonado.*

## 9. Autenticação e autorização

*A ser detalhado: mapeamento da identidade do token para `providerId`, isolamento entre provedores, segregação de operações internas.*

## 10. Uber Fx e shutdown

*A ser detalhado: organização dos módulos, ordem de inicialização/encerramento, comportamento sob `SIGTERM`.*

## 11. Observabilidade

*A ser detalhado: o que é logado, quais métricas existem e o que cada uma mede.*

## 12. Limitações e trabalho não concluído

*A ser detalhado ao final: interpretações adotadas, escopo deliberadamente reduzido, itens não concluídos.*
