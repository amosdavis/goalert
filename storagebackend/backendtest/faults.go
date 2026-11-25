package backendtest

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Faults makes storage unreliable, the way a network and a busy service are. Of
// the requests that go through it, it refuses some as a busy server does, so they
// are not carried out; fails some before they are sent; and carries some out but
// loses the reply, so that the client can't tell what happened (F32). It also
// delays requests a little, so that pods' requests interleave differently.
//
// It is an HTTP transport (RoundTrip), and an Azure SDK transport (Do).
type Faults struct {
	// Next sends requests on. Nil means http.DefaultTransport.
	Next http.RoundTripper

	rate     float64
	maxDelay time.Duration

	mu  sync.Mutex
	rnd *rand.Rand

	refused, failed, lost atomic.Int64
}

// NewFaults returns Faults that fail a share rate of requests, from 0 to 1,
// and delay each by up to maxDelay. The same seed gives the same choices.
func NewFaults(seed uint64, rate float64, maxDelay time.Duration) *Faults {
	return &Faults{rate: rate, maxDelay: maxDelay, rnd: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
}

// What happens to a request.
const (
	sent = iota
	refuse
	failBeforeSending
	loseReply
)

func (f *Faults) roll() (fate int, delay time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.maxDelay > 0 {
		delay = time.Duration(f.rnd.Int64N(int64(f.maxDelay)))
	}
	if f.rnd.Float64() >= f.rate {
		return sent, delay
	}
	return refuse + f.rnd.IntN(3), delay
}

// RoundTrip sends a request on, unless it fails.
func (f *Faults) RoundTrip(req *http.Request) (*http.Response, error) {
	closeBody := func() {
		if req.Body != nil {
			_, _ = io.Copy(io.Discard, req.Body)
			_ = req.Body.Close()
		}
	}
	fate, delay := f.roll()
	if delay > 0 {
		t := time.NewTimer(delay)
		select {
		case <-req.Context().Done():
			t.Stop()
			closeBody()
			return nil, req.Context().Err()
		case <-t.C:
		}
	}
	next := f.Next
	if next == nil {
		next = http.DefaultTransport
	}
	switch fate {
	case refuse:
		f.refused.Add(1)
		closeBody()
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable, Status: "503 Server Busy",
			Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Header:  http.Header{"X-Ms-Error-Code": {"ServerBusy"}, "Content-Length": {"0"}},
			Body:    http.NoBody,
			Request: req,
		}, nil
	case failBeforeSending:
		f.failed.Add(1)
		closeBody()
		return nil, errors.New("backendtest: the connection broke before the request was sent")
	case loseReply:
		resp, err := next.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		f.lost.Add(1)
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("backendtest: the reply to %s %s was lost", req.Method, req.URL.Path)
	}
	return next.RoundTrip(req)
}

// Do is RoundTrip, for the Azure SDK.
func (f *Faults) Do(req *http.Request) (*http.Response, error) { return f.RoundTrip(req) }

// Counts returns how many requests were refused, failed before they were sent,
// and lost their reply after they were carried out.
func (f *Faults) Counts() (refused, failed, lost int64) {
	return f.refused.Load(), f.failed.Load(), f.lost.Load()
}
