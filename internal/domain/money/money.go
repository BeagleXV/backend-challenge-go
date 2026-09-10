// Package money implements Money as an immutable value object backed by
// int64 minor units (cents). No float32/float64 is used anywhere in
// parsing, arithmetic or serialization.
package money

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Currency is an ISO 4217 currency code. Only currencies in the supported
// set below can be used to construct a valid Money value; any other code is
// rejected at construction time.
type Currency string

const (
	BRL Currency = "BRL"
	USD Currency = "USD"
	EUR Currency = "EUR"
)

var supportedCurrencies = map[Currency]struct{}{
	BRL: {},
	USD: {},
	EUR: {},
}

// Valid reports whether c is one of the currencies this system supports.
// All supported currencies use a fixed scale of two decimal places.
func (c Currency) Valid() bool {
	_, ok := supportedCurrencies[c]
	return ok
}

var (
	ErrInvalidMoney         = errors.New("money: invalid amount")
	ErrUnsupportedCurrency  = errors.New("money: unsupported currency")
	ErrCurrencyMismatch     = errors.New("money: currency mismatch")
	ErrOverflow             = errors.New("money: overflow")
	ErrNegativeNotAllowed   = errors.New("money: negative amount not allowed")
	ErrAmountMustBePositive = errors.New("money: amount must be greater than zero")
	ErrAmountMustBeZero     = errors.New("money: amount must be zero")
)

// amountPattern accepts an optional leading '-', one or more digits, and an
// optional '.' followed by one or two digits. This intentionally rejects
// empty strings, "NaN", "Infinity" and scientific notation by construction:
// none of those match the pattern. A bare integer part ("25") is accepted
// as equivalent to its two-decimal form ("25.00") — this normalization
// happens here, before any idempotency hash is computed downstream.
var amountPattern = regexp.MustCompile(`^-?[0-9]+(\.[0-9]{1,2})?$`)

// maxIntegerPart is the largest integer part that cannot overflow int64
// once multiplied by 100 (the fixed scale). The final two-decimal-digit
// remainder is still checked explicitly in New.
const maxIntegerPart = math.MaxInt64 / 100

// Money is an immutable value object: a signed amount in minor units (e.g.
// cents) paired with its currency. The zero value is not a valid Money and
// must never be used directly — always construct through New, Zero or
// FromMinorUnits.
type Money struct {
	minorUnits int64
	currency   Currency
}

// New parses a decimal string amount for the given currency. It rejects
// empty strings, NaN, Infinity, scientific notation and any scale beyond
// two decimal places. Negative amounts are accepted here (they are valid
// for internal differences and calculations) — callers that must reject
// negative external input should use ParseExternalAmount instead.
func New(amount string, currency Currency) (Money, error) {
	if !currency.Valid() {
		return Money{}, fmt.Errorf("%w: %q", ErrUnsupportedCurrency, currency)
	}
	if !amountPattern.MatchString(amount) {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidMoney, amount)
	}

	negative := false
	s := amount
	if s[0] == '-' {
		negative = true
		s = s[1:]
	}

	intPart, fracPart, hasFrac := strings.Cut(s, ".")
	if !hasFrac {
		fracPart = "00"
	}
	for len(fracPart) < 2 {
		fracPart += "0"
	}

	intValue, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil || intValue > maxIntegerPart {
		return Money{}, fmt.Errorf("%w: %q", ErrOverflow, amount)
	}
	fracValue, err := strconv.ParseInt(fracPart, 10, 64)
	if err != nil {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidMoney, amount)
	}

	base := intValue * 100 // safe: intValue <= maxIntegerPart guarantees base <= MaxInt64
	if fracValue > math.MaxInt64-base {
		return Money{}, fmt.Errorf("%w: %q", ErrOverflow, amount)
	}
	minorUnits := base + fracValue
	if negative {
		minorUnits = -minorUnits
	}

	return Money{minorUnits: minorUnits, currency: currency}, nil
}

// ParseExternalAmount parses a monetary value received from an external
// input (HTTP or SQS), where negative amounts are never accepted regardless
// of the specific wager transaction kind.
func ParseExternalAmount(amount string, currency Currency) (Money, error) {
	m, err := New(amount, currency)
	if err != nil {
		return Money{}, err
	}
	if m.IsNegative() {
		return Money{}, fmt.Errorf("%w: %q", ErrNegativeNotAllowed, amount)
	}
	return m, nil
}

// Zero returns the zero amount for the given currency.
func Zero(currency Currency) (Money, error) {
	if !currency.Valid() {
		return Money{}, fmt.Errorf("%w: %q", ErrUnsupportedCurrency, currency)
	}
	return Money{minorUnits: 0, currency: currency}, nil
}

// FromMinorUnits reconstructs a Money value from its persisted minor-unit
// representation (e.g. a BIGINT column). Used by repositories rehydrating
// from storage, never to reinterpret untrusted external input.
func FromMinorUnits(units int64, currency Currency) (Money, error) {
	if !currency.Valid() {
		return Money{}, fmt.Errorf("%w: %q", ErrUnsupportedCurrency, currency)
	}
	return Money{minorUnits: units, currency: currency}, nil
}

func (m Money) MinorUnits() int64  { return m.minorUnits }
func (m Money) Currency() Currency { return m.currency }
func (m Money) IsNegative() bool   { return m.minorUnits < 0 }
func (m Money) IsZero() bool       { return m.minorUnits == 0 }

// Add returns m + other. Both must share the same currency.
func (m Money) Add(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, other.currency)
	}
	sum := m.minorUnits + other.minorUnits
	if (other.minorUnits > 0 && sum < m.minorUnits) || (other.minorUnits < 0 && sum > m.minorUnits) {
		return Money{}, ErrOverflow
	}
	return Money{minorUnits: sum, currency: m.currency}, nil
}

// Sub returns m - other. Both must share the same currency.
func (m Money) Sub(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, other.currency)
	}
	diff := m.minorUnits - other.minorUnits
	if (other.minorUnits < 0 && diff < m.minorUnits) || (other.minorUnits > 0 && diff > m.minorUnits) {
		return Money{}, ErrOverflow
	}
	return Money{minorUnits: diff, currency: m.currency}, nil
}

// Negate returns -m.
func (m Money) Negate() (Money, error) {
	if m.minorUnits == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minorUnits: -m.minorUnits, currency: m.currency}, nil
}

// Equals reports whether m and other have the same currency and amount.
func (m Money) Equals(other Money) bool {
	return m.currency == other.currency && m.minorUnits == other.minorUnits
}

// Compare returns -1, 0 or 1 if m is less than, equal to or greater than
// other. Both must share the same currency.
func (m Money) Compare(other Money) (int, error) {
	if m.currency != other.currency {
		return 0, fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, other.currency)
	}
	switch {
	case m.minorUnits < other.minorUnits:
		return -1, nil
	case m.minorUnits > other.minorUnits:
		return 1, nil
	default:
		return 0, nil
	}
}

// RequirePositive returns an error unless m is strictly greater than zero.
func (m Money) RequirePositive() error {
	if m.minorUnits <= 0 {
		return fmt.Errorf("%w: got %s", ErrAmountMustBePositive, m.String())
	}
	return nil
}

// RequireZero returns an error unless m is exactly zero.
func (m Money) RequireZero() error {
	if m.minorUnits != 0 {
		return fmt.Errorf("%w: got %s", ErrAmountMustBeZero, m.String())
	}
	return nil
}

// String renders the amount with a fixed two-decimal scale, e.g. "25.00" or
// "-3.50". Implemented without float conversion to avoid any precision loss.
func (m Money) String() string {
	units := m.minorUnits
	negative := units < 0

	var abs uint64
	if negative {
		// Avoids overflow at math.MinInt64, whose magnitude has no
		// positive int64 representation.
		abs = uint64(-(units + 1)) + 1
	} else {
		abs = uint64(units)
	}

	whole := abs / 100
	frac := abs % 100
	if negative {
		return fmt.Sprintf("-%d.%02d", whole, frac)
	}
	return fmt.Sprintf("%d.%02d", whole, frac)
}

type jsonMoney struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// MarshalJSON serializes Money using the external contract shape:
// {"amount":"25.00","currency":"BRL"}.
func (m Money) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonMoney{Amount: m.String(), Currency: string(m.currency)})
}

// UnmarshalJSON parses the external contract shape. It accepts negative
// amounts (Money itself is sign-agnostic); callers that must reject
// negative external input do so explicitly via ParseExternalAmount or
// RequirePositive/RequireZero after unmarshaling.
func (m *Money) UnmarshalJSON(data []byte) error {
	var raw jsonMoney
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMoney, err)
	}
	parsed, err := New(raw.Amount, Currency(raw.Currency))
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
