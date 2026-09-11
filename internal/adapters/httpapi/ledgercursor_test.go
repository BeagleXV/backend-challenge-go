package httpapi

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLedgerCursor_RoundTrips(t *testing.T) {
	original := ledgerCursor{CreatedAt: time.Now().UTC().Truncate(time.Microsecond), ID: uuid.New()}

	encoded, err := encodeLedgerCursor(original)
	require.NoError(t, err)

	decoded, err := decodeLedgerCursor(encoded)
	require.NoError(t, err)

	assert.True(t, original.CreatedAt.Equal(decoded.CreatedAt))
	assert.Equal(t, original.ID, decoded.ID)
}

func TestDecodeLedgerCursor_RejectsGarbage(t *testing.T) {
	_, err := decodeLedgerCursor("not-valid-base64!!!")
	assert.Error(t, err)
}
