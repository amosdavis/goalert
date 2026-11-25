package objstore

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type clocked struct {
	Store
	ClockWatch
}

func TestClockWatch(t *testing.T) {
	const limit = 15 * time.Second
	start := time.Date(2026, 11, 25, 12, 0, 0, 300e6, time.UTC)
	end := start.Add(200 * time.Millisecond)
	s := &clocked{Store: NewMem()}
	require.NoError(t, ClockError(s, limit), "no reply yet")
	_, _, ok := s.ClockOffset()
	assert.False(t, ok)

	// The server's clock is a minute ahead. Its date has whole seconds.
	s.Observe(start, end, start.Add(time.Minute).Truncate(time.Second))
	lo, hi, ok := s.ClockOffset()
	require.True(t, ok)
	assert.True(t, lo <= time.Minute && time.Minute <= hi, "%v to %v", lo, hi)
	assert.Less(t, hi-lo, 2*time.Second)
	assert.ErrorContains(t, ClockError(s, limit), "behind the storage's")

	s.ObserveHeader(start, end, http.Header{"Date": {start.Add(-time.Minute).Format(http.TimeFormat)}})
	assert.ErrorContains(t, ClockError(s, limit), "ahead of the storage's")

	// Within a second, the date can't say more than that the clocks agree.
	s.Observe(start, end, start.Truncate(time.Second))
	require.NoError(t, ClockError(s, limit))
	assert.NoError(t, ClockError(s, 2*time.Second), "a whole second of doubt is no skew")

	// Replies that say nothing, or too little, are ignored.
	s.Observe(start, end, start.Add(time.Hour))
	s.ObserveHeader(start, end, http.Header{})
	s.ObserveHeader(start, end, http.Header{"Date": {"yesterday"}})
	s.Observe(start, start.Add(10*time.Second), start)
	s.Observe(end, start, start)
	lo, hi, _ = s.ClockOffset()
	assert.True(t, lo > 59*time.Minute && hi < 61*time.Minute, "the last that counted: %v to %v", lo, hi)
	assert.NoError(t, ClockError(s, -1), "the check can be off")
	assert.NoError(t, ClockError(NewMem(), limit), "a store that can't tell")
}
