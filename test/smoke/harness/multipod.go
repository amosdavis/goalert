package harness

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/target/goalert/app"
)

// deltaPodSet is several app pods on a harness's one Delta table, behind a
// round-robin proxy.
type deltaPodSet struct {
	h     *Harness
	apps  []*app.App
	urls  []*url.URL
	hits  []int
	stop  []bool
	next  int
	mx    sync.Mutex
	proxy *httptest.Server
	close sync.Once
}

// runBackend runs an app and reports an unexpected error while the test runs.
func (h *Harness) runBackend(a *app.App) {
	go func() { assert.NoError(h.t, a.Run(context.Background())) }()
}

func (h *Harness) shutdownBackend(ctx context.Context, a *app.App) error {
	return a.Shutdown(ctx)
}

// StartDeltaPods starts count pods, including the harness's running one. Defer
// its Close after deferring Harness.Close so that it runs first.
func (h *Harness) StartDeltaPods(count int) *deltaPodSet {
	h.t.Helper()
	s, ok := h.storage.(*deltaStorage)
	if !ok {
		h.t.Skip("multiple pods require Delta storage")
	}
	if count < 2 {
		h.t.Fatalf("at least two pods are needed, got %d", count)
	}

	// The first pod takes the lease before any other starts.
	h.Trigger()
	p := &deltaPodSet{h: h}
	defer func() {
		if p.proxy == nil {
			p.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	add := func(a *app.App) {
		u, err := url.Parse(a.URL())
		if err != nil {
			h.t.Fatalf("parse pod URL: %v", err)
		}
		p.apps, p.urls = append(p.apps, a), append(p.urls, u)
		p.hits, p.stop = append(p.hits, 0), append(p.stop, false)
	}
	add(h.backend)
	for len(p.apps) < count {
		a, err := s.NewApp(ctx, h.appCfg)
		if err != nil {
			h.t.Fatalf("start pod %d: %v", len(p.apps), err)
		}
		add(a)
		h.runBackend(a)
		if err := a.WaitForStartup(ctx); err != nil {
			h.t.Fatalf("wait for pod %d: %v", len(p.apps)-1, err)
		}
	}
	p.proxy = httptest.NewServer(p)
	h.deltaPods = p
	h.SetURL(p.proxy.URL)
	return p
}

// active returns every running pod.
func (p *deltaPodSet) active() []*app.App {
	p.mx.Lock()
	defer p.mx.Unlock()
	var apps []*app.App
	for i, a := range p.apps {
		if !p.stop[i] {
			apps = append(apps, a)
		}
	}
	return apps
}

// ServeHTTP sends each request to the next running pod.
func (p *deltaPodSet) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	p.mx.Lock()
	var running []int
	for i, stopped := range p.stop {
		if !stopped {
			running = append(running, i)
		}
	}
	if len(running) == 0 {
		p.mx.Unlock()
		http.Error(w, "no running pods", http.StatusServiceUnavailable)
		return
	}
	idx := running[p.next%len(running)]
	p.next++
	p.hits[idx]++
	u := p.urls[idx]
	p.mx.Unlock()

	httputil.NewSingleHostReverseProxy(u).ServeHTTP(w, req)
}

// Hits returns how many requests each pod served.
func (p *deltaPodSet) Hits() []int {
	p.mx.Lock()
	defer p.mx.Unlock()
	return append([]int(nil), p.hits...)
}

// Stop shuts down a pod and removes it from the proxy. If it was the harness's
// backend, another pod takes that role.
func (p *deltaPodSet) Stop(index int) {
	p.h.t.Helper()
	p.mx.Lock()
	if index < 0 || index >= len(p.apps) {
		p.mx.Unlock()
		p.h.t.Fatalf("no pod %d", index)
	}
	if p.stop[index] {
		p.mx.Unlock()
		return
	}
	p.stop[index] = true
	if p.h.backend == p.apps[index] {
		for i, a := range p.apps {
			if !p.stop[i] {
				p.h.backend = a
				break
			}
		}
	}
	a := p.apps[index]
	p.mx.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := p.h.shutdownBackend(ctx, a); err != nil {
		p.h.t.Errorf("shut down pod %d: %v", index, err)
	}
}

// Close stops the extra pods and the proxy. The harness closes its current
// backend, which is the only one left running.
func (p *deltaPodSet) Close() {
	p.close.Do(func() {
		p.mx.Lock()
		var stop []*app.App
		for i, a := range p.apps {
			if p.stop[i] || a == p.h.backend {
				continue
			}
			p.stop[i] = true
			stop = append(stop, a)
		}
		p.mx.Unlock()
		for i, a := range stop {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := p.h.shutdownBackend(ctx, a); err != nil {
				p.h.t.Errorf("shut down extra pod %d: %v", i, err)
			}
			cancel()
		}
		p.h.SetURL("")
		if p.h.deltaPods == p {
			p.h.deltaPods = nil
		}
		if p.proxy != nil {
			p.proxy.Close()
		}
	})
}

var _ http.Handler = (*deltaPodSet)(nil)
