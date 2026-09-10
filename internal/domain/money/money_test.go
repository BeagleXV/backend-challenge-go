package money_test

import (
	"errors"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

func TestNew_ValidAmounts(t *testing.T) {
	cases := []struct {
		amount   string
		currency money.Currency
		want     int64
	}{
		{"25.00", money.BRL, 2500},
		{"25", money.BRL, 2500}, // bare integer normalizes to two decimals
		{"25.5", money.BRL, 2550},
		{"0.00", money.USD, 0},
		{"0", money.USD, 0},
		{"-3.50", money.EUR, -350},
		{"1000000.99", money.BRL, 100000099},
	}
	for _, c := range cases {
		t.Run(c.amount+"_"+string(c.currency), func(t *testing.T) {
			m, err := money.New(c.amount, c.currency)
			require.NoError(t, err)
			assert.Equal(t, c.want, m.MinorUnits())
			assert.Equal(t, c.currency, m.Currency())
		})
	}
}

func TestNew_InvalidAmounts(t *testing.T) {
	cases := []string{
		"",
		"NaN",
		"Infinity",
		"-Infinity",
		"1e10",
		"1E10",
		"25.999", // scale > 2
		"25.001", // scale > 2
		"abc",
		"25,00",
		"25.",
		".25",
		"25 00",
		"+25.00",
	}
	for _, amount := range cases {
		t.Run(amount, func(t *testing.T) {
			_, err := money.New(amount, money.BRL)
			require.Error(t, err)
			assert.True(t, errors.Is(err, money.ErrInvalidMoney), "expected ErrInvalidMoney, got %v", err)
		})
	}
}

func TestNew_UnsupportedCurrency(t *testing.T) {
	_, err := money.New("10.00", money.Currency("XXX"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, money.ErrUnsupportedCurrency))
}

func TestNew_OverflowOnParse(t *testing.T) {
	huge := strconv.FormatInt(math.MaxInt64, 10) + ".99"
	_, err := money.New(huge, money.BRL)
	require.Error(t, err)
	assert.True(t, errors.Is(err, money.ErrOverflow))
}

func TestParseExternalAmount_RejectsNegative(t *testing.T) {
	_, err := money.ParseExternalAmount("-1.00", money.BRL)
	require.Error(t, err)
	assert.True(t, errors.Is(err, money.ErrNegativeNotAllowed))
}

func TestParseExternalAmount_AcceptsZeroAndPositive(t *testing.T) {
	_, err := money.ParseExternalAmount("0.00", money.BRL)
	require.NoError(t, err)
	_, err = money.ParseExternalAmount("25.00", money.BRL)
	require.NoError(t, err)
}

func TestAdd_Sub_Negate(t *testing.T) {
	a, err := money.New("10.00", money.BRL)
	require.NoError(t, err)
	b, err := money.New("2.50", money.BRL)
	require.NoError(t, err)

	sum, err := a.Add(b)
	require.NoError(t, err)
	assert.Equal(t, "12.50", sum.String())

	diff, err := a.Sub(b)
	require.NoError(t, err)
	assert.Equal(t, "7.50", diff.String())

	neg, err := diff.Negate()
	require.NoError(t, err)
	assert.Equal(t, "-7.50", neg.String())

	backToPositive, err := neg.Negate()
	require.NoError(t, err)
	assert.True(t, backToPositive.Equals(diff))
}

func TestAdd_Overflow(t *testing.T) {
	max, err := money.FromMinorUnits(math.MaxInt64, money.BRL)
	require.NoError(t, err)
	one, err := money.New("0.01", money.BRL)
	require.NoError(t, err)

	_, err = max.Add(one)
	require.Error(t, err)
	assert.True(t, errors.Is(err, money.ErrOverflow))
}

func TestSub_Overflow(t *testing.T) {
	min, err := money.FromMinorUnits(math.MinInt64, money.BRL)
	require.NoError(t, err)
	one, err := money.New("0.01", money.BRL)
	require.NoError(t, err)

	_, err = min.Sub(one)
	require.Error(t, err)
	assert.True(t, errors.Is(err, money.ErrOverflow))
}

func TestNegate_OverflowAtMinInt64(t *testing.T) {
	min, err := money.FromMinorUnits(math.MinInt64, money.BRL)
	require.NoError(t, err)

	_, err = min.Negate()
	require.Error(t, err)
	assert.True(t, errors.Is(err, money.ErrOverflow))
}

func TestAdd_Sub_CurrencyMismatch(t *testing.T) {
	brl, err := money.New("10.00", money.BRL)
	require.NoError(t, err)
	usd, err := money.New("10.00", money.USD)
	require.NoError(t, err)

	_, err = brl.Add(usd)
	require.Error(t, err)
	assert.True(t, errors.Is(err, money.ErrCurrencyMismatch))

	_, err = brl.Sub(usd)
	require.Error(t, err)
	assert.True(t, errors.Is(err, money.ErrCurrencyMismatch))

	_, err = brl.Compare(usd)
	require.Error(t, err)
	assert.True(t, errors.Is(err, money.ErrCurrencyMismatch))
}

func TestCompareAndEquals(t *testing.T) {
	a, _ := money.New("10.00", money.BRL)
	b, _ := money.New("20.00", money.BRL)

	cmp, err := a.Compare(b)
	require.NoError(t, err)
	assert.Equal(t, -1, cmp)

	cmp, err = b.Compare(a)
	require.NoError(t, err)
	assert.Equal(t, 1, cmp)

	c, _ := money.New("10.00", money.BRL)
	cmp, err = a.Compare(c)
	require.NoError(t, err)
	assert.Equal(t, 0, cmp)
	assert.True(t, a.Equals(c))
	assert.False(t, a.Equals(b))
}

func TestRequirePositive_RequireZero(t *testing.T) {
	zero, _ := money.Zero(money.BRL)
	require.NoError(t, zero.RequireZero())
	require.Error(t, zero.RequirePositive())

	positive, _ := money.New("0.01", money.BRL)
	require.NoError(t, positive.RequirePositive())
	require.Error(t, positive.RequireZero())

	negative, _ := money.New("-0.01", money.BRL)
	require.Error(t, negative.RequirePositive())
	require.Error(t, negative.RequireZero())
}

func TestJSON_RoundTrip(t *testing.T) {
	m, err := money.New("25.00", money.BRL)
	require.NoError(t, err)

	data, err := m.MarshalJSON()
	require.NoError(t, err)
	assert.JSONEq(t, `{"amount":"25.00","currency":"BRL"}`, string(data))

	var decoded money.Money
	require.NoError(t, decoded.UnmarshalJSON(data))
	assert.True(t, m.Equals(decoded))
}

func TestJSON_UnmarshalInvalid(t *testing.T) {
	var m money.Money
	err := m.UnmarshalJSON([]byte(`{"amount":"NaN","currency":"BRL"}`))
	require.Error(t, err)
	assert.True(t, errors.Is(err, money.ErrInvalidMoney))
}

func TestString_NegativeFormatting(t *testing.T) {
	m, err := money.New("-1234.56", money.BRL)
	require.NoError(t, err)
	assert.Equal(t, "-1234.56", m.String())
}
