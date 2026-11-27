package filestore

import (
	"sync"
	"time"
)

// A Clock supplies the time used for transaction timestamps. One clock owns
// time for the whole store (F13).
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// RealClock returns a Clock that reads the system clock.
func RealClock() Clock { return realClock{} }

// FakeClock is a Clock for tests. Its time can be frozen, sped up or moved
// forward. Time runs as base + realElapsed*speed, the model the smoke test
// harness's Postgres clock (pgmocktime) had, so the tests kept their timing
// when they moved to this backend.
type FakeClock struct {
	mu    sync.Mutex
	base  time.Time
	ref   time.Time // real time at which base was set
	speed float64
}

// NewFakeClock returns a FakeClock that starts at start and runs at normal
// speed.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{base: start, ref: time.Now(), speed: 1}
}

func (c *FakeClock) nowLocked() time.Time {
	elapsed := time.Since(c.ref) // monotonic
	return c.base.Add(time.Duration(float64(elapsed) * c.speed))
}

// Now returns the clock's current time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nowLocked()
}

// SetSpeed changes how fast time runs from now on. 0 freezes it and 1 is
// normal speed. The clock keeps its current reading, so to freeze it at an
// exact time, call Set after SetSpeed(0).
func (c *FakeClock) SetSpeed(speed float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.base, c.ref, c.speed = c.nowLocked(), time.Now(), speed
}

// Advance moves the clock forward by d.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.base, c.ref = c.nowLocked().Add(d), time.Now()
}

// Set moves the clock to t.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.base, c.ref = t, time.Now()
}
