//go:build integration

package integration_test

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/beaglexv/backend-challenge-go/internal/fxmodules"
	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
)

// envLookup adapts a plain map to config.LookupFunc, so a test can hand
// config.Load exactly the environment cmd/api/main.go would see, pointed
// at real ephemeral containers instead of a real deployment.
type envLookup map[string]string

func (e envLookup) lookup(key string) (string, bool) {
	v, ok := e[key]
	return v, ok
}

// buildConfig assembles the full process configuration from a
// postgresContainer, sqsInfra and keycloakContainer already started by
// the caller — the same shape config.Load produces from a real
// environment, never constructed by hand field-by-field, so this suite
// exercises the real validation/parsing path too.
func buildConfig(t *testing.T, pg postgresContainer, sqsInf sqsInfra, kc keycloakContainer, httpAddr, metricsAddr string) *config.Config {
	t.Helper()

	u, err := url.Parse(pg.dsn)
	require.NoError(t, err)
	password, _ := u.User.Password()

	env := envLookup{
		"APP_ENV":                          "test",
		"APP_HTTP_ADDR":                    httpAddr,
		"METRICS_ADDR":                     metricsAddr,
		"APP_SHUTDOWN_TIMEOUT":             "5s",
		"POSTGRES_HOST":                    u.Hostname(),
		"POSTGRES_PORT":                    u.Port(),
		"POSTGRES_DB":                      trimLeadingSlash(u.Path),
		"POSTGRES_USER":                    u.User.Username(),
		"POSTGRES_PASSWORD":                password,
		"POSTGRES_SSLMODE":                 "disable",
		"OIDC_ISSUER_URL":                  kc.issuerURL,
		"OIDC_AUDIENCE":                    "wagering-api",
		"AWS_REGION":                       "us-east-1",
		"AWS_ACCESS_KEY_ID":                "test",
		"AWS_SECRET_ACCESS_KEY":            "test",
		"SQS_ENDPOINT":                     sqsInf.endpoint,
		"SQS_WAGER_TRANSACTIONS_QUEUE_URL": sqsInf.txQueueURL,
		"SQS_WAGER_TRANSACTIONS_DLQ_URL":   sqsInf.dlqURL,
		"SQS_EVENTS_QUEUE_URL":             sqsInf.eventsURL,
	}

	cfg, err := config.Load(env.lookup)
	require.NoError(t, err)
	return cfg
}

func trimLeadingSlash(s string) string {
	if len(s) > 0 && s[0] == '/' {
		return s[1:]
	}
	return s
}

// freeAddr asks the OS for an ephemeral, currently-unused TCP port so
// concurrently-run tests in this package never collide on a fixed
// HTTP/metrics address.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().String()
}

// TestFxApp_StartAndStop_CleanShutdown boots the exact option list
// cmd/api/main.go uses — fxmodules.All(cfg) — against real Postgres,
// LocalStack and Keycloak containers, then starts and stops it. This is
// the "does the whole composed graph actually come up and go down
// cleanly against real infrastructure" proof the unit-level fx wiring
// tests from earlier phases cannot give: those never call
// app.Start/app.Stop against anything real.
//
// No goleak assertion here on purpose: every worker's Stop() already has
// its own dedicated unit test proving its goroutines exit (Fase 9-11), and
// goleak against a real HTTP server + SQS long-poll + Postgres pool is
// prone to reporting fd/timer noise from those libraries' own background
// bookkeeping that has nothing to do with this application's shutdown
// correctness — asserting Start/Stop both return nil is the reliable
// signal that every OnStart/OnStop hook ran and completed.
func TestFxApp_StartAndStop_CleanShutdown(t *testing.T) {
	pg := startPostgres(t)
	sqsInf := startLocalStack(t, 5)
	kc := startKeycloak(t)

	cfg := buildConfig(t, pg, sqsInf, kc, freeAddr(t), freeAddr(t))

	app := fx.New(fxmodules.All(cfg)...)

	startCtx, cancelStart := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStart()
	require.NoError(t, app.Start(startCtx), "app.Start must succeed against real infra")

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStop()
	require.NoError(t, app.Stop(stopCtx), "app.Stop must shut down every worker cleanly")
}
