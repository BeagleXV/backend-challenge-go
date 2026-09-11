package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/beaglexv/backend-challenge-go/internal/application/openwallet"
	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
)

func TestWriteApplicationError_MapsKnownErrorsToStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"not found", ports.ErrNotFound, http.StatusNotFound},
		{"already exists", ports.ErrAlreadyExists, http.StatusConflict},
		{"wallet already exists", openwallet.ErrWalletAlreadyExists, http.StatusConflict},
		{"invalid request", openwallet.ErrInvalidRequest, http.StatusBadRequest},
		{"idempotency conflict", processwagertransaction.ErrIdempotencyConflict, http.StatusConflict},
		{"external id reused", processwagertransaction.ErrExternalIDReused, http.StatusConflict},
		{"wallet not found", processwagertransaction.ErrWalletNotFound, http.StatusNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handled := writeApplicationError(rec, tc.err)
			assert.True(t, handled)
			assert.Equal(t, tc.want, rec.Code)
		})
	}
}

func TestWriteApplicationError_UnknownErrorReturnsFalse(t *testing.T) {
	rec := httptest.NewRecorder()
	handled := writeApplicationError(rec, errors.New("some unmapped infrastructure failure"))
	assert.False(t, handled)
}
