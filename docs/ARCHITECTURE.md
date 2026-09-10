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

## Schema do banco de dados

Seis migrations versionadas em `migrations/` (golang-migrate, `NNNNNN_nome.up.sql`/`.down.sql`), aplicadas via CLI/imagem Docker `migrate/migrate`, independente da versão de Go da aplicação:

| Migration | Tabela/objeto | Papel |
| --- | --- | --- |
| `000001` | `wallets` | Agregado de carteira |
| `000002` | `wager_transactions` | Operações (internas e externas) |
| `000003` | `wallet_ledger_entries` | Ledger append-only |
| `000004` | `inbox_messages` | Dedup de mensagens SQS |
| `000005` | `outbox_events` | Outbox transacional |
| `000006` | role `wagering_runtime` | Privilégios mínimos de runtime |

**Toda invariante financeira é imposta no próprio Postgres, não só em Go** — cada uma foi testada manualmente (inserts/updates de violação, confirmando o erro esperado) antes de fechar a fase:

- `wallets.balance >= 0` e `wallets.version >= 1` são `CHECK` constraints — uma tentativa de gravar saldo negativo é rejeitada pelo banco independentemente do código da aplicação.
- `wager_transactions` tem um `CHECK` cruzado (`wager_transactions_origin_fields`) que impede qualquer linha `INTERNAL` de carregar metadados externos e qualquer linha `EXTERNAL` de faltar algum — a distinção `OPENING` vs. externo (seção 4) é garantida pelo schema, não só pelos dois construtores de domínio separados.
- **Unicidade de idempotência**: índice único parcial em `idempotency_key` (só linhas `EXTERNAL`) e em `(provider_id, external_transaction_id)` — mesmo se a aplicação tivesse um bug de deduplicação, o banco rejeitaria a segunda tentativa.
- **Crédito inicial duplicado**: índice único parcial `(wallet_id) WHERE kind = 'OPENING'` — testado inserindo uma segunda `OPENING` para a mesma carteira, rejeitada com `duplicate key`.
- **Ledger imutável**: além de nenhuma coluna `updated_at`, um trigger (`wallet_ledger_entries_reject_mutation`) rejeita qualquer `UPDATE`/`DELETE` na tabela — testado diretamente via SQL, ambos falham com a mensagem do trigger. A consistência `balanceAfter = balanceBefore ± amount` é reforçada por `CHECK`, redundante com a validação já feita em `ledger.New` (Fase 1) — a mesma invariante é garantida duas vezes, em duas camadas independentes.
- **Dupla reversão bem-sucedida**: índice único parcial `(provider_id, reference_external_transaction_id, kind) WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK')`. Testado: um segundo `REFUND` `PROCESSED` sobre a mesma referência é rejeitado; um `ROLLBACK` `PROCESSED` sobre a mesma referência *é* permitido (kind diferente) — a combinação REFUND+ROLLBACK sobre a mesma aposta não é bloqueada pelo schema, então a coerência financeira entre os dois (ex. impedir que a soma devolvida exceda o valor original) é responsabilidade da camada de aplicação (Fase de idempotência/casos de uso), não do banco.
- **Role de runtime com privilégios mínimos** (`wagering_runtime`): `SELECT`/`INSERT`/`UPDATE` nas tabelas de domínio, mas **sem `UPDATE`/`DELETE` em `wallet_ledger_entries`** — reforço do trigger de imutabilidade numa camada diferente (privilégio de banco, não lógica). A role não tem senha própria versionada; ela é um "grupo de privilégios" (`NOLOGIN`), e o papel de login real que a aplicação usa é criado fora do controle de versão (provisionamento do ambiente), recebendo `GRANT wagering_runtime TO <role de login>`. Migrations sempre rodam como o dono do schema, nunca como essa role.

Todas as seis migrations foram validadas de ponta a ponta: `up` completo, `down -all` completo (schema volta a conter só `schema_migrations`), e `up` reaplicado sem erro — confirmando que os `down.sql` são funcionais, não só existem.

## 1. Money

`Money` (`internal/domain/money`) é um value object imutável: `int64` em unidades mínimas (centavos) mais `currency`. Nenhum `float32`/`float64` é usado em nenhuma etapa de parsing, aritmética ou serialização.

- **Moedas suportadas:** BRL, USD, EUR — todas com escala fixa de duas casas. Qualquer outro código ISO 4217 é rejeitado na construção (`ErrUnsupportedCurrency`).
- **Parsing (`New`):** aceita um padrão estrito `^-?[0-9]+(\.[0-9]{1,2})?$`. Isso rejeita, por construção (sem checagem extra), string vazia, `NaN`, `Infinity`, notação científica e escala acima de duas casas. Um valor inteiro puro (`"25"`) é aceito como equivalente a `"25.00"` — essa normalização acontece nesse ponto, antes de qualquer cálculo de hash de idempotência downstream (Fase de idempotência).
- **Sinal:** `New`/`Zero`/`FromMinorUnits` aceitam valores negativos, pois diferenças e cálculos internos podem ser negativos. Um construtor separado, `ParseExternalAmount`, rejeita negativos explicitamente — é o usado para qualquer entrada financeira vinda de HTTP/SQS. Validação adicional por tipo de operação (positivo estrito para BET/WIN/REFUND/ROLLBACK, exatamente zero para LOSS) fica em `RequirePositive`/`RequireZero`, usados pelo pacote `wagertransaction`.
- **Overflow:** checado explicitamente em `Add`, `Sub`, `Negate` e no próprio parsing (o produto `intValue*100` e a soma da parte fracionária são verificados contra `math.MaxInt64`/`math.MinInt64` antes de serem aplicados). `String()` também evita overflow ao formatar o valor mínimo de `int64` (cuja magnitude não cabe em `int64` positivo).
- **Contrato externo:** `MarshalJSON`/`UnmarshalJSON` implementam a forma `{"amount":"25.00","currency":"BRL"}`.
- **Persistência:** `MinorUnits()`/`FromMinorUnits` expõem a representação crua para os repositórios (Fase de adapters Postgres) mapearem para `BIGINT`.

## 2. Wallet e controle de concorrência

`Wallet` (`internal/domain/wallet`) é o agregado raiz financeiro. `New` cria com `version = 1`; `Rehydrate` reconstrói a partir do estado persistido sem reaplicar nenhuma movimentação.

- **Invariantes:** moeda do movimento deve coincidir com a moeda da carteira (`ErrCurrencyMismatch`); débito nunca deixa o saldo negativo (`ErrInsufficientBalance`, verificado após o cálculo — nenhuma mutação parcial ocorre se a operação for rejeitada); saldo inicial e saldo persistido nunca podem ser negativos.
- **`version`:** incrementa exatamente uma vez por chamada bem-sucedida de `Debit`/`Credit`, e apenas nesse caso — uma tentativa rejeitada (saldo insuficiente, moeda incompatível, valor não positivo) não altera `version` nem `balance`. Isso é o que a coluna `version` no banco vai usar para detectar disputa entre escritores concorrentes.
- **Estratégia de concorrência:** o agregado em si não implementa nenhum lock — ele só garante que `Debit`/`Credit` sejam atômicos *dentro do processo*. A exclusão mútua entre processos/instâncias concorrentes acontece na camada de persistência (Fase de adapters Postgres), via `SELECT ... FOR UPDATE` na linha da carteira antes de carregar o agregado, executado dentro da mesma transação SQL que grava o novo saldo e o lançamento no ledger. Essa escolha (pessimista, não otimista) foi decidida antes da implementação: como a carteira é bloqueada por linha, carteiras diferentes continuam avançando em paralelo, e o teste obrigatório de disputa (100 BRL, duas apostas de 80) fica trivial de provar corretamente sem precisar de um loop de retry.
- **`Debit`/`Credit`** retornam `(balanceBefore, balanceAfter, error)` para que o chamador monte o `ledger.Entry` correspondente na mesma transação, sem recalcular nada.

## 3. Transações SQL

*A ser detalhado na fase de adapters Postgres: delimitação exata de onde cada transação começa/termina, por caso de uso.*

## 4. Máquina de estados de WagerTransaction

`WagerTransaction` (`internal/domain/wagertransaction`) modela tanto a operação interna `OPENING` quanto as cinco operações externas.

- **Estados:** `PENDING → PENDING_REFERENCE | PROCESSED | REJECTED | FAILED`, e `PENDING_REFERENCE → PROCESSED | REJECTED | FAILED`. `PROCESSED`, `REJECTED` e `FAILED` são terminais — qualquer tentativa de transição a partir deles retorna `ErrInvalidTransition`, mesmo para o mesmo estado de destino (ex. `PENDING_REFERENCE -> PENDING_REFERENCE` também é rejeitado, evitando reprocessamento silencioso).
- **Falha transitória vs. permanente:** `REJECTED` é reservado para uma recusa de regra de negócio (ex. saldo insuficiente, referência não encontrada após expirar) — sempre com um `failureCode` estável. `FAILED` é reservado para falha permanente de infraestrutura registrada para auditoria — também exige `failureCode`. Nenhuma das duas transições é aceita sem um código de falha (`ErrFailureCodeRequired`).
- **Origem interna vs. externa:** `NewExternal` rejeita explicitamente `KindOpening` (`ErrInvalidKind`) — `OPENING` só pode ser criado via `NewInternalOpening`, que não aceita nem preenche `providerID`, `externalTransactionID`, `idempotencyKey`, `payloadHash`, `roundID`, `gameID` ou referência (permanecem no valor zero, e a distinção de origem — `OriginInternal`/`OriginExternal` — é um campo próprio, mapeado para uma constraint de banco na fase de schema).
- **Política de valor zero por tipo** (`validateAmountForKind`): `LOSS` exige exatamente `"0.00"`; `BET`, `WIN`, `REFUND` e `ROLLBACK` exigem valor estritamente positivo. `OPENING` aceita zero ou positivo (a decisão de não criar o registro quando o saldo inicial é zero fica a cargo do caso de uso, não do construtor de domínio).
- **Reidratação:** `Rehydrate` reconstrói o estado exatamente como persistido, sem validar transições nem reemitir eventos — uma transação terminal reidratada continua terminal e continua rejeitando novas transições.

## 5. Reversões (REFUND / ROLLBACK)

- `NewExternal` exige `referenceExternalTransactionID` para `REFUND`/`ROLLBACK` (`ErrReferenceRequired`) e rejeita esse campo para os demais tipos (`ErrReferenceNotApplicable`) — a validação é simétrica.
- `ResolveReference` registra o id interno da transação referenciada e só é aplicável a `REFUND`/`ROLLBACK` (`ErrReferenceNotApplicable` para os demais tipos). Ele não altera o status por si só — a transição para `PROCESSED`/`REJECTED`/`FAILED` continua explícita, feita pelo caso de uso depois de validar a referência.
- A prevenção de **dupla reversão bem-sucedida** da mesma referência (ex. dois REFUND processados sobre a mesma aposta) não é responsabilidade deste pacote — o domínio permite construir e resolver múltiplas tentativas; a garantia definitiva vem da constraint de unicidade no schema (Fase de schema/migrations) sobre `(reference_external_transaction_id, kind) WHERE status = 'PROCESSED'`, já que apenas o banco consegue arbitrar corretamente entre tentativas concorrentes.

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
