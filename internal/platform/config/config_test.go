package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
)

func lookupFrom(env map[string]string) config.LookupFunc {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

func validEnv() map[string]string {
	return map[string]string{
		"POSTGRES_HOST":                    "localhost",
		"POSTGRES_PORT":                    "5432",
		"POSTGRES_DB":                      "wagering",
		"POSTGRES_USER":                    "wagering_app",
		"POSTGRES_PASSWORD":                "s3cret",
		"OIDC_ISSUER_URL":                  "http://localhost:8081/realms/wagering",
		"OIDC_AUDIENCE":                    "wagering-api",
		"AWS_REGION":                       "us-east-1",
		"SQS_WAGER_TRANSACTIONS_QUEUE_URL": "http://localhost:4566/000000000000/wager-transactions.fifo",
	}
}

func TestLoad_AppliesDefaults(t *testing.T) {
	cfg, err := config.Load(lookupFrom(validEnv()))
	require.NoError(t, err)

	assert.Equal(t, "local", cfg.AppEnv)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, ":8080", cfg.HTTPAddr)
	assert.Equal(t, 15*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, "disable", cfg.Postgres.SSLMode)
	assert.Equal(t, int32(0), cfg.Postgres.MaxConns)
}

func TestLoad_MissingRequiredField_FailsFast(t *testing.T) {
	for _, key := range []string{"POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "OIDC_ISSUER_URL", "OIDC_AUDIENCE", "AWS_REGION", "SQS_WAGER_TRANSACTIONS_QUEUE_URL"} {
		t.Run(key, func(t *testing.T) {
			env := validEnv()
			delete(env, key)

			_, err := config.Load(lookupFrom(env))
			require.Error(t, err)
			assert.Contains(t, err.Error(), key)
		})
	}
}

func TestLoad_InvalidShutdownTimeout_Fails(t *testing.T) {
	env := validEnv()
	env["APP_SHUTDOWN_TIMEOUT"] = "not-a-duration"

	_, err := config.Load(lookupFrom(env))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "APP_SHUTDOWN_TIMEOUT")
}

func TestLoad_InvalidMaxConns_Fails(t *testing.T) {
	env := validEnv()
	env["POSTGRES_MAX_CONNS"] = "not-a-number"

	_, err := config.Load(lookupFrom(env))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "POSTGRES_MAX_CONNS")
}

func TestPostgres_DSN_EscapesCredentials(t *testing.T) {
	pg := config.Postgres{
		Host:     "db.internal",
		Port:     "5432",
		DB:       "wagering",
		User:     "user@name",
		Password: "p@ss/word",
		SSLMode:  "require",
	}

	dsn := pg.DSN()
	assert.Equal(t, "postgres://user%40name:p%40ss%2Fword@db.internal:5432/wagering?sslmode=require", dsn)
}
