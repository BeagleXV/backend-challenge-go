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
| `000007` | `wager_transactions.result_balance_*` | Snapshot do saldo no momento do processamento (ver seção 4) |

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

`internal/adapters/postgres.UnitOfWork` implementa `ports.UnitOfWork` com uma transação pgx real: `WithinTx` abre a transação, injeta o `pgx.Tx` no `context.Context` (chave de pacote privada, `txKey`), executa a função do caso de uso, e comita ou reverte de acordo com o retorno — exatamente um `BEGIN`/`COMMIT` por chamada de caso de uso. Cada repositório resolve, via `q(ctx, pool)`, se deve executar contra a transação ativa ou (quando chamado fora de qualquer `WithinTx`, ex. um `GET` HTTP simples) diretamente contra o pool — os dois tipos satisfazem a mesma interface estrutural `querier` (`Exec`/`Query`/`QueryRow`).

**Delimitação por caso de uso:**

| Caso de uso | O que entra na mesma transação |
| --- | --- |
| `OpenWallet.Handle` | Insert da carteira; se saldo inicial > 0: insert do `OPENING`, insert do lançamento de crédito, update do `OPENING` para `PROCESSED`, insert dos dois eventos na outbox. |
| `ProcessWagerTransaction.Handle` | Insert opcional na inbox (SQS) → lookups de idempotência → `SELECT ... FOR UPDATE` da carteira → insert da `wager_transaction` → mutação da carteira + lançamento no ledger + update da transação para o estado final + eventos na outbox → conclusão da inbox. |
| `ProcessWagerTransaction.Resume` | `SELECT ... FOR UPDATE` da própria `wager_transaction` pendente → `SELECT ... FOR UPDATE` da carteira → mesmo caminho de resolução de reversão do `Handle`. |
| `Reconciliation.Reconcile` | Leitura da carteira + leitura de todos os lançamentos do ledger, sem nenhuma escrita. |

**Locks pessimistas implementados:**
- `WalletRepository.GetForUpdate`: `SELECT ... FOR UPDATE` na linha da carteira. Validado com dois goroutines, cada um com sua própria transação contra o mesmo pool (conexões de servidor genuinamente distintas) disputando um débito de 80 num saldo de 100 — exatamente um sucede, o outro observa o saldo já reduzido ao obter o lock.
- `WagerTransactionRepository.GetForUpdate`: mesmo princípio, na linha da própria `wager_transaction`, usado por `Resume` — necessário porque duas instâncias do worker de referências pendentes (fase futura) podem tentar resolver a mesma transação `PENDING_REFERENCE` ao mesmo tempo; sem esse lock, ambas poderiam aplicar o movimento da reversão.
- `WagerTransactionRepository.ListPendingReferenceForUpdate` e `OutboxRepository.ClaimBatch`: `SELECT ... FOR UPDATE SKIP LOCKED`, para que múltiplas instâncias concorrentes dividam o trabalho sem bloquear umas às outras.

**Duas race conditions reais encontradas ao ligar isso a um Postgres de verdade** (nenhuma das duas era visível contra os fakes em memória da Fase 3, que não replicam isolamento de transação):

1. **Retomada de referência pendente sem lock de linha próprio.** A primeira versão de `Resume` usava `GetByID` (sem lock) para carregar a `wager_transaction`. Duas instâncias retomando a mesma transação `PENDING_REFERENCE` simultaneamente serializavam no lock da carteira, mas não na leitura inicial — a segunda podia carregar o objeto em memória antes da primeira commitar, e depois aplicar o movimento uma segunda vez sobre um saldo já atualizado. Corrigido trocando para `GetForUpdate` (ver acima).
2. **Abort de transação após violação de constraint.** A primeira versão, ao perder a corrida de `INSERT` por `idempotency_key`/`(providerId, externalId)` duplicado, tentava consultar a linha vencedora *na mesma transação* para devolver o resultado como replay. No Postgres real isso nunca funciona: depois que uma instrução viola uma constraint, a transação inteira fica abortada e qualquer comando seguinte falha com `current transaction is aborted` até o `ROLLBACK`. A correção: `Insert` retorna um erro sentinela (`errInsertConflict`) que aborta a transação imediatamente (sem tentar mais nada nela), e `Handle` repete a operação inteira **numa transação nova** — a segunda tentativa sempre encontra a linha vencedora já commitada, porque o Postgres só libera o `INSERT` perdedor com erro depois que a transação vencedora termina. Uma única repetição é suficiente (nunca mais que isso é necessário, pela mesma razão).

Esse segundo ponto foi provado com um teste de integração que envia a mesma aposta 50 vezes em paralelo contra Postgres real (`TestProcessWagerTransaction_SameBetSentFiftyTimesInParallel_SingleDebit`): todas as 50 chamadas retornam `PROCESSED`, com o mesmo `transactionId` e o mesmo saldo — nunca um estado intermediário, porque nenhuma transação concorrente consegue observar a linha vencedora antes dela commitar por inteiro.

**Mapeamento de erros** (`internal/adapters/postgres/errors.go`): toda função de repositório passa o erro cru do pgx por `mapErr`, que traduz `pgx.ErrNoRows` → `ports.ErrNotFound`, `unique_violation` (23505) → `ports.ErrAlreadyExists`, e `check_violation`/`not_null_violation`/`foreign_key_violation` → `ErrConstraintViolation` (sentinela própria do pacote `postgres`, usada quando a violação indica um bug ou uma race não prevista pela aplicação, não uma entrada de negócio recusável). Nenhum `*pgconn.PgError` ou `pgx.ErrNoRows` cru escapa desse pacote para a camada de aplicação.

## 4. Máquina de estados de WagerTransaction

`WagerTransaction` (`internal/domain/wagertransaction`) modela tanto a operação interna `OPENING` quanto as cinco operações externas.

- **Estados:** `PENDING → PENDING_REFERENCE | PROCESSED | REJECTED | FAILED`, e `PENDING_REFERENCE → PROCESSED | REJECTED | FAILED`. `PROCESSED`, `REJECTED` e `FAILED` são terminais — qualquer tentativa de transição a partir deles retorna `ErrInvalidTransition`, mesmo para o mesmo estado de destino (ex. `PENDING_REFERENCE -> PENDING_REFERENCE` também é rejeitado, evitando reprocessamento silencioso).
- **Falha transitória vs. permanente:** `REJECTED` é reservado para uma recusa de regra de negócio (ex. saldo insuficiente, referência não encontrada após expirar) — sempre com um `failureCode` estável. `FAILED` é reservado para falha permanente de infraestrutura registrada para auditoria — também exige `failureCode`. Nenhuma das duas transições é aceita sem um código de falha (`ErrFailureCodeRequired`).
- **Origem interna vs. externa:** `NewExternal` rejeita explicitamente `KindOpening` (`ErrInvalidKind`) — `OPENING` só pode ser criado via `NewInternalOpening`, que não aceita nem preenche `providerID`, `externalTransactionID`, `idempotencyKey`, `payloadHash`, `roundID`, `gameID` ou referência (permanecem no valor zero, e a distinção de origem — `OriginInternal`/`OriginExternal` — é um campo próprio, mapeado para uma constraint de banco na fase de schema).
- **Política de valor zero por tipo** (`validateAmountForKind`): `LOSS` exige exatamente `"0.00"`; `BET`, `WIN`, `REFUND` e `ROLLBACK` exigem valor estritamente positivo. `OPENING` aceita zero ou positivo (a decisão de não criar o registro quando o saldo inicial é zero fica a cargo do caso de uso, não do construtor de domínio).
- **Reidratação:** `Rehydrate` reconstrói o estado exatamente como persistido, sem validar transições nem reemitir eventos — uma transação terminal reidratada continua terminal e continua rejeitando novas transições.
- **`ResultBalance`:** campo adicionado retroativamente (migration `000007`) para persistir "o resultado financeiro retornado ao provedor" — o saldo da carteira observado no exato momento em que a transação chegou a `PROCESSED` ou `REJECTED`. `SetResultBalance` é chamado pelo caso de uso antes de cada uma dessas duas transições (nunca para `PENDING_REFERENCE`, que ainda não tem resultado financeiro). É esse snapshot, não o saldo atual da carteira, que um replay idempotente devolve (seção 7).

## 5. Reversões (REFUND / ROLLBACK)

- `NewExternal` exige `referenceExternalTransactionID` para `REFUND`/`ROLLBACK` (`ErrReferenceRequired`) e rejeita esse campo para os demais tipos (`ErrReferenceNotApplicable`) — a validação é simétrica.
- `ResolveReference` registra o id interno da transação referenciada e só é aplicável a `REFUND`/`ROLLBACK` (`ErrReferenceNotApplicable` para os demais tipos). Ele não altera o status por si só — a transição para `PROCESSED`/`REJECTED`/`FAILED` continua explícita, feita pelo caso de uso depois de validar a referência.
- A prevenção de **dupla reversão bem-sucedida** da mesma referência (ex. dois REFUND processados sobre a mesma aposta) não é responsabilidade deste pacote — o domínio permite construir e resolver múltiplas tentativas; a garantia definitiva vem da constraint de unicidade no schema (seção "Schema do banco de dados" acima) sobre `(reference_external_transaction_id, kind) WHERE status = 'PROCESSED'`, já que apenas o banco consegue arbitrar corretamente entre tentativas concorrentes.
- A regra de negócio propriamente dita fica em `internal/application/processwagertransaction`, função `reversalDirection`: `REFUND` só é aceito referenciando um `BET` (crédito de volta); `ROLLBACK` desfaz um `BET` (crédito), ou um `WIN`/`REFUND` (débito) — nunca um `LOSS`, `OPENING` ou outro `ROLLBACK`. Qualquer outra combinação é rejeitada com `FailureCodeInvalidReferenceKind`.
- A operação e sua referência precisam concordar em carteira, jogador, rodada e moeda (`validateReferenceAgreement`) — divergência rejeita com `FailureCodeReferenceMismatch`. O valor precisa ser exatamente igual ao da referência (`FailureCodeReferenceAmountMismatch`) — reversões parciais não existem.
- Antes de aplicar o movimento, o caso de uso consulta `WagerTransactionRepository.HasSuccessfulReversal` (espelha a constraint do banco, mas devolve uma rejeição de negócio limpa em vez de deixar a violação de constraint SQL estourar) — uma segunda tentativa do mesmo tipo de reversão sobre a mesma referência é rejeitada com `FailureCodeReversalAlreadyProcessed`, **antes mesmo de tentar o movimento**.
- `ROLLBACK` que precisaria debitar mais que o saldo disponível é rejeitado com `FailureCodeRollbackInsufficientBalance` — um código diferente do usado por um `BET` sem saldo (`FailureCodeInsufficientBalance`), como exigido.
- Quando a referência existe mas terminou sem sucesso (`REJECTED`/`FAILED`), a reversão é rejeitada com `FailureCodeReferenceNotProcessed` — não tenta novamente e não fica pendente.

## 6. Referências pendentes

Quando a referência de um `REFUND`/`ROLLBACK` ainda não existe, ou existe mas está `PENDING`/`PENDING_REFERENCE` (ainda não concluída), a operação é persistida como `PENDING_REFERENCE` e um evento `WagerTransactionPendingReference` é enfileirado na outbox — nunca fica bloqueando a transação SQL esperando a referência aparecer.

A retomada (`processwagertransaction.Service.Resume`, exposta via `resolvependingreference.Service.Resolve`) reutiliza exatamente a mesma função de resolução de reversão (`applyReversal`) usada no caminho síncrono — não existe uma segunda implementação da regra "o que conta como resolvido". `resolvependingreference.Service.ListReady` expõe a consulta que localiza candidatos a retentativa.

A política de backoff exponencial e TTL/número máximo de tentativas — quando desistir e transicionar para `REJECTED` com `FailureCodeReferenceNotFound` — é responsabilidade do worker dedicado (fase seguinte), que chama `Resolve` repetidamente; esta fase só entrega o mecanismo de resolução em si, não o agendamento.

## 7. Idempotência

O algoritmo de hash canônico do payload (serialização determinística, quais campos entram/saem) ainda será implementado numa fase própria — por ora, `Request.IdempotencyKey` e `Request.PayloadHash` chegam já calculados pelo chamador (HTTP/SQS).

O que esta fase já implementa é o **uso** dessas informações para a detecção de conflito, igual para HTTP e SQS porque é o mesmo `processwagertransaction.Service.Handle` para ambos:

- Busca por `idempotencyKey`: se existe e o hash bate, devolve o resultado persistido com `IdempotentReplay: true` — incluindo o **saldo observado no processamento original** (`WagerTransaction.ResultBalance`, seção 4), nunca o saldo atual da carteira, que pode ter mudado. Testado explicitamente: uma segunda operação move a carteira entre o processamento original e o replay, e o replay ainda devolve o saldo antigo.
- Se existe e o hash diverge, retorna `ErrIdempotencyConflict`.
- Busca por `(providerId, externalTransactionId)`: se já existe uma transação com uma `idempotencyKey` diferente da recebida, retorna `ErrExternalIDReused` — uma operação financeira não pode ser reaplicada sob outra chave.
- Nenhuma dessas checagens depende de estado em memória do processo — tudo é lido do repositório a cada chamada.

## 8. Inbox / Outbox

Quando `Request.Inbox` é preenchido (entrada via SQS), `Handle` insere o registro de inbox (`(consumerName, messageId)`) na mesma `UnitOfWork` que tudo o mais. Se o registro já existe (reentrega), o processamento de domínio é pulado inteiramente e o resultado é obtido por busca de `idempotencyKey` — a mensagem nunca é reprocessada, e a resposta ainda assim reflete o resultado original. Para requisições HTTP, `Inbox` é `nil` e nada disso se aplica (a idempotência já cobre o caso HTTP sozinha).

`MarkCompleted` é chamado ao final de toda transação bem-sucedida, **inclusive** quando o resultado é `PENDING_REFERENCE` — a pendência já está persistida de forma durável nesse ponto, então a mensagem de entrada pode ser considerada tratada; o worker de referências pendentes assume a continuidade a partir daí, sem depender da mensagem original.

Todo evento de domínio (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WalletBalanceChanged`, `WagerTransactionPendingReference`) é serializado em JSON e gravado via `OutboxRepository.Enqueue` **dentro da mesma transação** que a mudança que o originou — nunca publicado diretamente. A disputa entre múltiplos publishers e a recuperação de trabalho abandonado (`locked_by`/`locked_at`, já presentes no schema) ficam para o worker de outbox, numa fase própria.

## 9. Autenticação, autorização e contratos HTTP

**IdP escolhido:** Keycloak (`quay.io/keycloak/keycloak:26.0`, modo `start-dev --import-realm`), conforme recomendado pelo desafio. Justificativa: suporta `client_credentials` nativamente, permite provisionamento 100% automático via import de realm (`deploy/keycloak/realm-export.json`, montado no compose) sem passos manuais, e expõe descoberta OIDC padrão (`/.well-known/openid-configuration` + JWKS) que `go-oidc` consome diretamente.

**Modelo de credenciais:** cada provedor externo é um client Keycloak `client_credentials` próprio (`provider-a`, `provider-b` no realm de exemplo) — não há usuário humano, é comunicação serviço-a-serviço. Um terceiro client, `internal-service`, representa o serviço interno autorizado a abrir carteiras e rodar reconciliação.

**Mapeamento de identidade → `providerId`:** como `client_credentials` não tem usuário final, não existe claim padrão para "quem é o provedor". Cada client de provedor carrega um *protocol mapper* `oidc-hardcoded-claim-mapper` que injeta `providerId` (string) no access token — `provider-a` sempre emite `providerId: "provider-a"`, nunca outro valor, e o middleware nunca precisa (nem deve) confiar em nada que o chamador possa influenciar além do token assinado pelo IdP. `internal-service` carrega, do mesmo jeito, `internal: true` (boolean) — nenhum client de provedor tem essa claim, e `internal-service` não tem `providerId`. Auditoria: `sub` e `azp` (client id) do token continuam disponíveis em `idp.Claims` mesmo sem serem usados para autorização.

**Audiência:** um client scope de realm (`wagering-api-audience`, com `oidc-audience-mapper` para `wagering-api`) é atribuído por padrão a todo client do realm, garantindo que `aud` sempre contenha `wagering-api` — `idp.NewVerifier` valida isso via `oidc.Config{ClientID: audience}`.

**Verificação (`internal/adapters/idp`):** `NewVerifier` busca o documento de descoberta e o JWKS do issuer de forma síncrona, na hora da construção (via fx, antes até do `OnStart`) — um Keycloak fora do ar ou mal configurado derruba o processo imediatamente, não silenciosamente na primeira requisição. `Verify` valida assinatura, `iss`, `aud` e expiração (delegado a `go-oidc`/`go-jose`) e decodifica `Claims{Subject, ClientID, ProviderID, Internal}`. Testado (`internal/adapters/idp/verifier_test.go`) contra um IdP fake local (servidor `httptest` servindo descoberta + JWKS reais, chave RSA gerada no teste) cobrindo: token válido aceito, expirado rejeitado, audiência errada rejeitada, token vazio/malformado rejeitado, issuer inalcançável falha rápido na construção. A integração com o Keycloak real do compose fica para a suite da Fase 15 (containers reais).

**Middlewares HTTP (`internal/adapters/httpapi`), nesta ordem:** `recoveryMiddleware` (panic → 500 + log com stack) → `correlationIDMiddleware` (honra `X-Correlation-Id` de entrada ou gera um, sempre ecoado na resposta) → `loggingMiddleware` (uma linha estruturada por requisição: método, path, status, duração, `correlationId` — nunca o header `Authorization`, o token ou o corpo) → limite de tempo por requisição (`http.TimeoutHandler`, 30s). Rotas de negócio acrescentam `authMiddleware` (exige `Authorization: Bearer <token>` válido) e então uma checagem de autorização específica da rota.

**Autorização por rota:**

| Rota | Exige | Checagem adicional |
| --- | --- | --- |
| `POST /wallets`, `GET /wallets/:id`, `GET /wallets/:id/ledger`, `POST /wallets/:id/reconciliation` | `internal: true` | — (nenhum client de provedor tem essa claim) |
| `POST /wagering/transactions` | `providerId` não vazio | `body.providerId == claims.providerId`; caso contrário `403` — o servidor nunca aceita processar em nome de outro provedor |
| `GET /wagering/transactions/:id` | `providerId` não vazio | carrega a transação e compara `tx.ProviderID() == claims.providerId`; divergência retorna **404**, não 403 — um provedor não deve conseguir distinguir "não é seu" de "não existe" |
| `GET /providers/:providerId/wagering/transactions/:externalId` | `providerId` não vazio | `path.providerId == claims.providerId`, mesma política de 404 em caso de divergência |
| `GET /health/live`, `GET /health/ready` | nada (público) | usado por probes, nunca carrega token |

**Mapeamento de erros → HTTP (`internal/adapters/httpapi/errors.go`):** `ports.ErrNotFound` → 404; `ports.ErrAlreadyExists`/`openwallet.ErrWalletAlreadyExists` → 409; `*.ErrInvalidRequest` e erros de validação do domínio (`wagertransaction.ErrInvalidKind`, `ErrInvalidWagerTransaction`, `ErrReferenceRequired`, `ErrReferenceNotApplicable`) → 400; `ErrIdempotencyConflict` → 409; `ErrExternalIDReused` → 409; qualquer outro erro → 500 genérico (detalhe real vai só para o log do servidor, nunca para o corpo da resposta). Fora do `err != nil`, o **resultado** de `processwagertransaction.Handle` (que não é erro) mapeia por `status`: `PROCESSED` → 200, `PENDING_REFERENCE` → 202, `REJECTED` → 422 (com `failureCode`).

**Paginação do ledger:** `GET /wallets/:id/ledger?cursor=&limit=` usa keyset pagination real sobre `(created_at, id)` — nunca `OFFSET`, que degrada e pode pular/repetir linhas sob escrita concorrente. `ports.LedgerRepository.ListByWalletPage` recebe `(afterCreatedAt, afterID, limit)` e o adapter Postgres filtra com `WHERE (created_at, id) > ($2, $3) ORDER BY created_at, id LIMIT $4` — o par `(zero time.Time, uuid.Nil)` naturalmente representa "desde o início", pois ambos ordenam abaixo de qualquer valor real. O cursor que o cliente vê é opaco: base64 de `{"createdAt","id"}`, nunca um offset ou id sequencial cru — não expõe estrutura interna nem permite adivinhar cardinalidade entre carteiras. O handler busca `limit+1` linhas para saber se há próxima página sem expor essa linha extra.

**Hash de payload (interino):** `POST /wagering/transactions` calcula `payloadHash` como SHA-256 do JSON do request (campos de negócio, `idempotencyKey` nunca incluso porque não faz parte da struct) — `encoding/json` serializa os campos de uma struct sempre na mesma ordem declarada, então o mesmo pedido semântico sempre produz o mesmo hash, o suficiente para a checagem de idempotência de `Handle` funcionar corretamente agora. Este é um substituto deliberadamente simples: a Fase 7 formaliza `CanonicalHash` com a lista de campos documentada e qualquer normalização decimal necessária, e troca esta implementação sem mudar o contrato externo.

**Health checks:** `/health/live` nunca toca uma dependência (só confirma que o processo está de pé); `/health/ready` faz `pool.Ping` com timeout de 2s e responde 503 real se o Postgres não responder — nunca um `200 OK` fixo. A verificação de SQS se junta a este endpoint na Fase 9.

**Testado manualmente** (ver histórico de sessão): `docker compose up -d` (Postgres + Keycloak), migrations aplicadas, tokens `client_credentials` obtidos para `provider-a`/`provider-b`/`internal-service`, e todo o fluxo exercitado via `curl` — abertura de carteira só com `internal`, rejeição 401/403 sem token/com token errado, aposta processada com débito correto, replay idempotente, provedor B bloqueado de submeter ou ler dados do provedor A (403/404), paginação do ledger com cursor real, reconciliação consistente.

## 10. Uber Fx e shutdown

Um `fx.Module` por camada, em `internal/fxmodules/`, agregados por `All(cfg)` e consumidos por `cmd/api/main.go`:

- `config` — não tem construtor; apenas `fx.Supply` do `*config.Config` já carregado e validado por `config.Load` **antes** de `fx.New` ser chamado. Isso significa que uma variável de ambiente obrigatória ausente falha o processo com uma mensagem simples em stderr, sem nem montar o grafo de dependências do dig — mais rápido de diagnosticar do que deixar o fail-fast acontecer dentro de um `OnStart`.
- `logging` — provê o `*zap.Logger` (nível e encoder derivados de `LOG_LEVEL`/`APP_ENV`) e registra `OnStop` para `logger.Sync()` (erro ignorado deliberadamente — sync de stdout/stderr em terminal/pipe no Linux costuma retornar ENOTTY/EINVAL, o que não é uma falha real).
- `postgres` — `newPool` abre o `*pgxpool.Pool` a partir de `cfg.Postgres.DSN()` e registra `OnStart` fazendo `pool.Ping(ctx)` (conecta e valida credenciais/rede no start, não na primeira requisição) e `OnStop` fazendo `pool.Close()`. Os cinco `ports.*Repository` e o `ports.UnitOfWork` são providos como as interfaces da camada de aplicação (nunca o tipo concreto do adapter), via funções `asXxx` — mantém a application layer livre de import de `internal/adapters/postgres`.
- `application` — provê `ports.Clock`/`ports.IDGenerator` reais (`internal/platform/clock`, `internal/platform/idgen`) e os quatro serviços de caso de uso (`openwallet`, `processwagertransaction`, `reconciliation`, `resolvependingreference`) como singletons compartilhados — é o mesmo `*processwagertransaction.Service` que HTTP (Fase 6) e o consumidor SQS (Fase 9) vão chamar.
- `idp` (Fase 6) — provê `*idp.Verifier`, construído buscando a descoberta OIDC do Keycloak de forma síncrona (fail-fast se o IdP estiver fora do ar).
- `httpapi` (Fase 6) — provê o `http.Handler` (`httpapi.NewRouter`, dependendo de todo o resto: verifier, pool, os quatro serviços de aplicação e os `ports.*Repository`) e registra `OnStart`/`OnStop` do `*http.Server`: `OnStart` faz `net.Listen` (erro de bind falha o processo imediatamente) e sobe `srv.Serve` em goroutine; `OnStop` chama `srv.Shutdown(ctx)` — para de aceitar conexões novas e espera as em andamento terminarem, respeitando o `ctx` que `fx.StopTimeout` limita.
- `bootstrap` — desde a Fase 6, o próprio `fx.Invoke` de `httpapi` já força a construção de `config`/`logging`/`postgres`/`idp`/`application` (o router depende de tudo isso). `bootstrap` ficou reduzido a um `fx.Invoke(func(*resolvependingreference.Service) {})`, só para alcançar esse serviço específico, que nenhum outro componente ainda usa (fica para o worker de referências pendentes, Fase 10). Quando esse worker existir, este invoke também deve ser removido.

Ordem de shutdown obtida (fx desfaz `OnStop` na ordem inversa em que os `OnStart`/construtores rodaram): servidor HTTP → pool Postgres → sync do logger — já na ordem que o desafio pede (listener HTTP para de aceitar requisições antes de qualquer conexão de infraestrutura fechar). Workers (SQS, outbox, referências pendentes) entram nessa cadeia nas fases 8-11, sempre antes das conexões que usam, pela mesma garantia de ordem por dependência.

`fx.StopTimeout(cfg.ShutdownTimeout)` limita quanto tempo o processo espera por todos os `OnStop` durante um `SIGTERM`/`SIGINT` — evita travar para sempre esperando um worker que nunca drena. Não há `fx.StartTimeout` explícito nesta fase (nenhum `OnStart` faz I/O de longa duração além do ping ao Postgres).

Testado manualmente subindo `docker compose up -d postgres`, rodando o binário compilado e enviando `SIGTERM`: log de `wagering-api started` após o ping do pool ter sucesso, e na sequência `wagering-api stopping` → pool fechado → logger sincronizado, saindo com código 0.

Domínio (`internal/domain/**`) confirmado sem nenhum import de `go.uber.org/fx`.

## 11. Observabilidade

*A ser detalhado: o que é logado, quais métricas existem e o que cada uma mede.*

## 12. Limitações e trabalho não concluído

*A ser detalhado ao final: interpretações adotadas, escopo deliberadamente reduzido, itens não concluídos.*
