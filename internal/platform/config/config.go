// Package config loads process configuration from the environment. Loading
// happens once, before fx builds anything, so a missing or invalid secret
// fails the process immediately with a clear message instead of surfacing
// later as a confusing connection error.
package config

import (
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// Postgres carries the pieces needed to build a DSN. There is no default
// for User/Password/DB — a wagering database is not something we silently
// point at a guessed name.
type Postgres struct {
	Host     string
	Port     string
	DB       string
	User     string
	Password string
	SSLMode  string
	MaxConns int32
}

// DSN builds the libpq connection string pgxpool expects, URL-escaping the
// user and password so special characters in either can't corrupt the DSN.
func (p Postgres) DSN() string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(p.User, p.Password),
		Host:   p.Host + ":" + p.Port,
		Path:   "/" + p.DB,
	}
	q := url.Values{}
	q.Set("sslmode", p.SSLMode)
	u.RawQuery = q.Encode()
	return u.String()
}

// OIDC carries the settings needed to validate bearer tokens against an
// external IdP (Keycloak). There is no default issuer/audience — an API
// that silently accepted tokens from an unconfigured or wrong issuer would
// defeat the point of authentication.
type OIDC struct {
	IssuerURL string
	Audience  string
}

// SQS carries the settings needed to reach the wager-transactions and
// wager-events queues. AccessKeyID/SecretAccessKey/Endpoint are all
// optional: unset, the AWS SDK falls back to its default credential chain
// (IAM role, shared config, env vars it reads itself) and the real AWS
// endpoint for Region — exactly what a real deployment wants. Set, they
// point the client at LocalStack with its static test credentials for
// local development. Either way, credentials only ever come from
// config/environment, never a literal in source.
type SQS struct {
	Region                    string
	Endpoint                  string
	AccessKeyID               string
	SecretAccessKey           string
	WagerTransactionsQueueURL string
	WagerTransactionsDLQURL   string
	EventsQueueURL            string
}

// Config is every setting the process needs at startup.
type Config struct {
	AppEnv          string
	ShutdownTimeout time.Duration
	LogLevel        string
	HTTPAddr        string
	MetricsAddr     string
	Postgres        Postgres
	OIDC            OIDC
	SQS             SQS
}

// LookupFunc matches os.LookupEnv's signature, so tests can supply a fake
// environment without touching process-wide state.
type LookupFunc func(key string) (string, bool)

// Load reads and validates configuration via lookup (os.LookupEnv in
// production). It fails fast: any required value that is absent, or any
// present value that fails to parse, is returned as an error before the
// caller proceeds to build anything.
func Load(lookup LookupFunc) (*Config, error) {
	cfg := &Config{
		AppEnv:      getOr(lookup, "APP_ENV", "local"),
		LogLevel:    getOr(lookup, "LOG_LEVEL", "info"),
		HTTPAddr:    getOr(lookup, "APP_HTTP_ADDR", ":8080"),
		MetricsAddr: getOr(lookup, "METRICS_ADDR", ":9090"),
	}

	shutdownTimeout, err := parseDuration(lookup, "APP_SHUTDOWN_TIMEOUT", 15*time.Second)
	if err != nil {
		return nil, err
	}
	cfg.ShutdownTimeout = shutdownTimeout

	pgCfg, err := loadPostgres(lookup)
	if err != nil {
		return nil, err
	}
	cfg.Postgres = pgCfg

	oidcCfg, err := loadOIDC(lookup)
	if err != nil {
		return nil, err
	}
	cfg.OIDC = oidcCfg

	sqsCfg, err := loadSQS(lookup)
	if err != nil {
		return nil, err
	}
	cfg.SQS = sqsCfg

	return cfg, nil
}

func loadSQS(lookup LookupFunc) (SQS, error) {
	region, err := require(lookup, "AWS_REGION")
	if err != nil {
		return SQS{}, err
	}
	queueURL, err := require(lookup, "SQS_WAGER_TRANSACTIONS_QUEUE_URL")
	if err != nil {
		return SQS{}, err
	}
	dlqURL, err := require(lookup, "SQS_WAGER_TRANSACTIONS_DLQ_URL")
	if err != nil {
		return SQS{}, err
	}
	eventsQueueURL, err := require(lookup, "SQS_EVENTS_QUEUE_URL")
	if err != nil {
		return SQS{}, err
	}
	return SQS{
		Region:                    region,
		Endpoint:                  getOr(lookup, "SQS_ENDPOINT", ""),
		AccessKeyID:               getOr(lookup, "AWS_ACCESS_KEY_ID", ""),
		SecretAccessKey:           getOr(lookup, "AWS_SECRET_ACCESS_KEY", ""),
		WagerTransactionsQueueURL: queueURL,
		WagerTransactionsDLQURL:   dlqURL,
		EventsQueueURL:            eventsQueueURL,
	}, nil
}

func loadOIDC(lookup LookupFunc) (OIDC, error) {
	issuerURL, err := require(lookup, "OIDC_ISSUER_URL")
	if err != nil {
		return OIDC{}, err
	}
	audience, err := require(lookup, "OIDC_AUDIENCE")
	if err != nil {
		return OIDC{}, err
	}
	return OIDC{IssuerURL: issuerURL, Audience: audience}, nil
}

func loadPostgres(lookup LookupFunc) (Postgres, error) {
	host, err := require(lookup, "POSTGRES_HOST")
	if err != nil {
		return Postgres{}, err
	}
	port, err := require(lookup, "POSTGRES_PORT")
	if err != nil {
		return Postgres{}, err
	}
	db, err := require(lookup, "POSTGRES_DB")
	if err != nil {
		return Postgres{}, err
	}
	user, err := require(lookup, "POSTGRES_USER")
	if err != nil {
		return Postgres{}, err
	}
	password, err := require(lookup, "POSTGRES_PASSWORD")
	if err != nil {
		return Postgres{}, err
	}

	maxConns, err := parseInt32(lookup, "POSTGRES_MAX_CONNS", 0)
	if err != nil {
		return Postgres{}, err
	}

	return Postgres{
		Host:     host,
		Port:     port,
		DB:       db,
		User:     user,
		Password: password,
		SSLMode:  getOr(lookup, "POSTGRES_SSLMODE", "disable"),
		MaxConns: maxConns,
	}, nil
}

func require(lookup LookupFunc, key string) (string, error) {
	v, ok := lookup(key)
	if !ok || v == "" {
		return "", fmt.Errorf("config: required environment variable %s is not set", key)
	}
	return v, nil
}

func getOr(lookup LookupFunc, key, fallback string) string {
	if v, ok := lookup(key); ok && v != "" {
		return v
	}
	return fallback
}

func parseDuration(lookup LookupFunc, key string, fallback time.Duration) (time.Duration, error) {
	v, ok := lookup(key)
	if !ok || v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q is not a valid duration: %w", key, v, err)
	}
	return d, nil
}

func parseInt32(lookup LookupFunc, key string, fallback int32) (int32, error) {
	v, ok := lookup(key)
	if !ok || v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q is not a valid integer: %w", key, v, err)
	}
	return int32(n), nil
}
