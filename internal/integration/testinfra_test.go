//go:build integration

// Package integration hosts the full-stack suite Fase 15 asks for: real
// Postgres, real Keycloak and real LocalStack, wired together the same way
// cmd/api/main.go wires them (via fxmodules.All), never a mock or fake for
// any of the three. Adapter-scoped integration tests already prove
// individual adapters against real infra (see
// internal/adapters/postgres/postgres_integration_test.go and
// internal/adapters/outboxpublisher/sqspublisher_integration_test.go);
// this package proves the composed system: HTTP+SQS end to end, migrations
// up/down, DLQ, and recovery after a simulated process restart.
package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tclocalstack "github.com/testcontainers/testcontainers-go/modules/localstack"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// --- Postgres ---

// postgresContainer starts a fresh Postgres container and applies every
// migration. It returns both the pool (for the app/tests to use) and the
// raw DSN and migrate.Migrate handle (for tests that need to run
// Up/Down themselves, e.g. TestMigrations_UpAndDown).
type postgresContainer struct {
	pool *pgxpool.Pool
	dsn  string
}

func startPostgres(t *testing.T) postgresContainer {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wagering"),
		tcpostgres.WithUsername("wagering_app"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	applyMigrations(t, dsn)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return postgresContainer{pool: pool, dsn: dsn}
}

// applyMigrations runs every up migration against dsn. golang-migrate's
// pgx/v5 driver registers under the "pgx5" URL scheme, not "postgres" —
// same DSN, different scheme prefix.
func applyMigrations(t *testing.T, dsn string) {
	t.Helper()
	migrateDSN := "pgx5" + dsn[len("postgres"):]
	m, err := migrate.New("file://../../migrations", migrateDSN)
	require.NoError(t, err)
	defer func() { _, _ = m.Close() }()
	require.NoError(t, m.Up())
}

// --- LocalStack / SQS ---

type sqsInfra struct {
	client     *sqs.Client
	endpoint   string
	txQueueURL string
	dlqURL     string
	eventsURL  string
	txQueueArn string
}

// startLocalStack provisions the same three FIFO queues
// deploy/localstack/init-queues.sh sets up for real deployments:
// wager-transactions.fifo (redrive to the DLQ after maxReceiveCount
// deliveries), wager-transactions-dlq.fifo and wager-events.fifo.
// maxReceiveCount is a parameter so the DLQ scenario test can use a small
// value and stay fast.
func startLocalStack(t *testing.T, maxReceiveCount int) sqsInfra {
	t.Helper()
	ctx := context.Background()

	container, err := tclocalstack.Run(ctx, "localstack/localstack:3")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	endpoint, err := container.PortEndpoint(ctx, "4566/tcp", "http")
	require.NoError(t, err)

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	dlqOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("wager-transactions-dlq.fifo"),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})
	require.NoError(t, err)

	dlqAttrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       dlqOut.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	dlqArn := dlqAttrs.Attributes["QueueArn"]

	redrivePolicy := fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":"%d"}`, dlqArn, maxReceiveCount)
	txOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("wager-transactions.fifo"),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
			"VisibilityTimeout":         "2",
			"RedrivePolicy":             redrivePolicy,
		},
	})
	require.NoError(t, err)

	txAttrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       txOut.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	eventsOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("wager-events.fifo"),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})
	require.NoError(t, err)

	return sqsInfra{
		client:     client,
		endpoint:   endpoint,
		txQueueURL: aws.ToString(txOut.QueueUrl),
		dlqURL:     aws.ToString(dlqOut.QueueUrl),
		eventsURL:  aws.ToString(eventsOut.QueueUrl),
		txQueueArn: txAttrs.Attributes["QueueArn"],
	}
}

// --- Keycloak ---

// keycloakContainer runs the exact image/import docker-compose.yml uses
// for local dev, via testcontainers' generic container support — there is
// no official Keycloak testcontainers-go module.
type keycloakContainer struct {
	issuerURL string
}

func startKeycloak(t *testing.T) keycloakContainer {
	t.Helper()
	ctx := context.Background()

	absRealmPath, err := filepath.Abs("../../deploy/keycloak/realm-export.json")
	require.NoError(t, err)

	req := testcontainers.ContainerRequest{
		Image:        "quay.io/keycloak/keycloak:26.0",
		Cmd:          []string{"start-dev", "--import-realm"},
		ExposedPorts: []string{"8080/tcp"},
		Env: map[string]string{
			"KC_BOOTSTRAP_ADMIN_USERNAME": "admin",
			"KC_BOOTSTRAP_ADMIN_PASSWORD": "changeme-local-only",
			"KC_HEALTH_ENABLED":           "true",
		},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      absRealmPath,
				ContainerFilePath: "/opt/keycloak/data/import/realm-export.json",
				FileMode:          0o444,
			},
		},
		WaitingFor: wait.ForHTTP("/realms/wagering/.well-known/openid-configuration").
			WithPort("8080/tcp").
			WithStartupTimeout(2 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	endpoint, err := container.PortEndpoint(ctx, "8080/tcp", "http")
	require.NoError(t, err)

	return keycloakContainer{issuerURL: endpoint + "/realms/wagering"}
}

// token performs the client_credentials grant against the real Keycloak
// container for clientID/secret and returns the raw access token. Every
// provider client in the realm export is a service account
// (serviceAccountsEnabled=true, no end user) — this is the same grant a
// real external provider integration would use.
func (k keycloakContainer) token(t *testing.T, clientID, clientSecret string) string {
	t.Helper()
	form := strings.NewReader(fmt.Sprintf(
		"grant_type=client_credentials&client_id=%s&client_secret=%s",
		clientID, clientSecret,
	))
	resp, err := http.Post(k.issuerURL+"/protocol/openid-connect/token", "application/x-www-form-urlencoded", form)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "client_credentials grant for %s", clientID)

	var body struct {
		AccessToken string `json:"access_token"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.NotEmpty(t, body.AccessToken)
	return body.AccessToken
}
