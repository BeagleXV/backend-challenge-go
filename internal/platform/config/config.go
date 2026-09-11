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

// Config is every setting the process needs at startup. Fields belonging to
// layers not wired yet (HTTP, SQS, OIDC) are added by the phase that wires
// them, not speculatively here.
type Config struct {
	AppEnv          string
	ShutdownTimeout time.Duration
	LogLevel        string
	Postgres        Postgres
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
		AppEnv:   getOr(lookup, "APP_ENV", "local"),
		LogLevel: getOr(lookup, "LOG_LEVEL", "info"),
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

	return cfg, nil
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
