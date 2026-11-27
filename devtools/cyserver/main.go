// cyserver runs GoAlert and the controls Cypress needs in one process. Its
// in-memory Delta table replaces the external database, fixture shell, and
// database clock that the browser tests used before the Delta cutover.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	stdlog "log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"

	sloglogrus "github.com/samber/slog-logrus/v2"
	"github.com/target/goalert/app"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/expflag"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/test/smoke/harness/fixture"
	utillog "github.com/target/goalert/util/log"
)

type deltaServer struct {
	cfg app.Config

	mx            sync.Mutex
	store         *objstore.Mem
	clock         *filestore.FakeClock
	fixtureDB     *filestore.DeltaDB
	fixtureStores *deltastores.Stores
	app           *app.App
	done          chan error
	pod           int
}

func (s *deltaServer) backend(writer string) *storagebackend.Backend {
	return &storagebackend.Backend{Store: s.store, Log: deltalog.New(s.store, writer)}
}

func (s *deltaServer) options(writer string) filestore.DeltaOptions {
	return filestore.DeltaOptions{
		Clock: s.clock, AllowTimeTravel: true, WriterID: writer,
		PollInterval: 20 * time.Millisecond, MaxPollInterval: 100 * time.Millisecond,
		Logf: stdlog.Printf,
	}
}

// stopLocked stops the app, but leaves the fixture pod and its data available.
func (s *deltaServer) stopLocked(ctx context.Context) error {
	if s.app == nil {
		return nil
	}
	a, done := s.app, s.done
	s.app, s.done = nil, nil
	err := a.Shutdown(ctx)
	if done != nil {
		select {
		case runErr := <-done:
			err = errors.Join(err, runErr)
		case <-ctx.Done():
			err = errors.Join(err, ctx.Err())
		}
	}
	return err
}

func (s *deltaServer) stop(ctx context.Context) error {
	s.mx.Lock()
	defer s.mx.Unlock()
	return s.stopLocked(ctx)
}

func (s *deltaServer) closeFixtureLocked(ctx context.Context) error {
	stores, db := s.fixtureStores, s.fixtureDB
	s.fixtureStores, s.fixtureDB = nil, nil
	var err error
	if stores != nil {
		err = errors.Join(err, stores.Close(ctx))
	}
	if db != nil {
		err = errors.Join(err, db.Close())
	}
	return err
}

func (s *deltaServer) close(ctx context.Context) error {
	s.mx.Lock()
	defer s.mx.Unlock()
	return errors.Join(s.stopLocked(ctx), s.closeFixtureLocked(ctx))
}

// resetLocked gives Cypress a new, empty installation and loads its initial
// users. The app must see the same object store and clock as the fixture pod.
func (s *deltaServer) resetLocked(ctx context.Context, initialSQL string) error {
	if err := s.stopLocked(ctx); err != nil {
		return err
	}
	if err := s.closeFixtureLocked(ctx); err != nil {
		return err
	}

	s.store = objstore.NewMem()
	s.clock = filestore.NewFakeClock(time.Now())
	ctx = permission.SystemContext(ctx, "cypress")
	if err := deltastores.Create(ctx, s.backend("create"), s.options("create")); err != nil {
		return fmt.Errorf("create Delta storage: %w", err)
	}
	db, tables, err := deltastores.Open(ctx, s.backend("fixtures"), s.options("fixtures"))
	if err != nil {
		return fmt.Errorf("open fixture storage: %w", err)
	}
	s.fixtureDB = db
	stores, err := deltastores.New(ctx, db, tables, deltastores.Options{
		Logger: utillog.NewLogger(), Registry: nfydest.NewRegistry(), NoSecrets: true,
	})
	if err != nil {
		return errors.Join(err, s.closeFixtureLocked(ctx))
	}
	s.fixtureStores = stores
	if initialSQL != "" {
		db := fixture.DB{Delta: s.fixtureDB, Stores: s.fixtureStores}
		if err := db.Exec(ctx, initialSQL); err != nil {
			return err
		}
	}
	return nil
}

func (s *deltaServer) reset(ctx context.Context, initialSQL string) error {
	s.mx.Lock()
	defer s.mx.Unlock()
	return s.resetLocked(ctx, initialSQL)
}

func startConfig(cfg app.Config, args []string) (app.Config, error) {
	cfg.ExpFlags, cfg.APIOnly = nil, false
	for _, arg := range args {
		switch {
		case arg == "--api-only":
			cfg.APIOnly = true
		case strings.HasPrefix(arg, "--experimental="):
			flags := strings.Split(strings.TrimPrefix(arg, "--experimental="), ",")
			if len(flags) == 0 || len(flags) > 10 {
				return cfg, errors.New("invalid experimental flags")
			}
			for _, f := range flags {
				cfg.ExpFlags = append(cfg.ExpFlags, expflag.Flag(strings.TrimSpace(f)))
			}
		default:
			return cfg, fmt.Errorf("invalid extra-arg %q", arg)
		}
	}
	return cfg, nil
}

func (s *deltaServer) startLocked(ctx context.Context, args []string) error {
	if err := s.stopLocked(ctx); err != nil {
		return err
	}
	if s.store == nil {
		if err := s.resetLocked(ctx, ""); err != nil {
			return err
		}
	}
	cfg, err := startConfig(s.cfg, args)
	if err != nil {
		return err
	}
	s.pod++
	name := fmt.Sprintf("cypress-%d", s.pod)
	ctx = permission.SystemContext(ctx, "cypress")
	db, tables, err := deltastores.Open(ctx, s.backend(name), s.options(name))
	if err != nil {
		return fmt.Errorf("open app storage: %w", err)
	}
	a, err := app.NewStorageApp(cfg, &app.Storage{DB: db, Tables: tables, PodName: name})
	if err != nil {
		return errors.Join(err, db.Close())
	}
	done := make(chan error, 1)
	s.app, s.done = a, done
	go func() { done <- a.Run(context.Background()) }()
	if err := a.WaitForStartup(ctx); err != nil {
		_ = a.Shutdown(context.Background())
		s.app, s.done = nil, nil
		return err
	}
	return nil
}

func (s *deltaServer) start(ctx context.Context, args []string) error {
	s.mx.Lock()
	defer s.mx.Unlock()
	return s.startLocked(ctx, args)
}

func (s *deltaServer) fixture(ctx context.Context, sql string) error {
	s.mx.Lock()
	defer s.mx.Unlock()
	if s.fixtureDB == nil {
		return errors.New("storage is not initialized")
	}
	db := fixture.DB{Delta: s.fixtureDB, Stores: s.fixtureStores}
	return db.Exec(permission.SystemContext(ctx, "cypress"), sql)
}

func (s *deltaServer) setClock(req clockRequest) error {
	s.mx.Lock()
	defer s.mx.Unlock()
	if s.clock == nil {
		return errors.New("storage is not initialized")
	}
	if req.Reset {
		s.clock.SetSpeed(1)
		s.clock.Set(time.Now())
	}
	if req.Speed != nil {
		if *req.Speed < 0 {
			return errors.New("clock speed can't be negative")
		}
		s.clock.SetSpeed(*req.Speed)
	}
	if req.Advance != "" {
		d, err := time.ParseDuration(req.Advance)
		if err != nil {
			return err
		}
		s.clock.Advance(d)
	}
	return nil
}

type clockRequest struct {
	Reset   bool     `json:"reset"`
	Speed   *float64 `json:"speed"`
	Advance string   `json:"advance"`
}

func result(w http.ResponseWriter, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func body(w http.ResponseWriter, req *http.Request) (string, error) {
	data, err := io.ReadAll(http.MaxBytesReader(w, req.Body, 4<<20))
	return string(data), err
}

func (s *deltaServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stop", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), 30*time.Second)
		defer cancel()
		result(w, s.stop(ctx))
	})
	mux.HandleFunc("GET /start", func(w http.ResponseWriter, req *http.Request) {
		_ = req.ParseForm()
		ctx, cancel := context.WithTimeout(req.Context(), 30*time.Second)
		defer cancel()
		result(w, s.start(ctx, req.Form["extra-arg"]))
	})
	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, req *http.Request) {
		initialSQL, err := body(w, req)
		if err == nil {
			ctx, cancel := context.WithTimeout(req.Context(), 30*time.Second)
			defer cancel()
			err = s.reset(ctx, initialSQL)
		}
		result(w, err)
	})
	mux.HandleFunc("POST /fixture", func(w http.ResponseWriter, req *http.Request) {
		sql, err := body(w, req)
		if err == nil {
			err = s.fixture(req.Context(), sql)
		}
		result(w, err)
	})
	mux.HandleFunc("POST /clock", func(w http.ResponseWriter, req *http.Request) {
		var clock clockRequest
		err := json.NewDecoder(http.MaxBytesReader(w, req.Body, 64<<10)).Decode(&clock)
		if err == nil {
			err = s.setClock(clock)
		}
		result(w, err)
	})
	mux.HandleFunc("GET /signal", func(w http.ResponseWriter, req *http.Request) {
		if req.FormValue("sig") != "SIGUSR2" {
			http.Error(w, "unsupported signal", http.StatusBadRequest)
			return
		}
		s.mx.Lock()
		a := s.app
		s.mx.Unlock()
		if a == nil || a.Engine == nil {
			http.Error(w, "engine is not running", http.StatusServiceUnavailable)
			return
		}
		go a.Engine.Trigger()
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func main() {
	stdlog.SetPrefix("cyserver: ")
	stdlog.SetFlags(stdlog.Lshortfile)
	cfg := app.Defaults()
	legacy := utillog.NewLogger()
	cfg.LegacyLogger = legacy
	cfg.Logger = slog.New(sloglogrus.Option{Logger: legacy.Logrus()}.NewLogrusHandler())
	addr := flag.String("addr", "127.0.0.1:3033", "Cypress control listen address")
	flag.StringVar(&cfg.ListenAddr, "listen", "127.0.0.1:3042", "GoAlert listen address")
	flag.StringVar(&cfg.ListenAddr, "l", "127.0.0.1:3042", "GoAlert listen address (shorthand)")
	flag.StringVar(&cfg.PublicURL, "public-url", "", "GoAlert public URL")
	flag.StringVar(&cfg.UIDir, "ui-dir", "", "directory of UI files")
	flag.BoolVar(&cfg.StubNotifiers, "stub-notifiers", false, "stub notification senders")
	flag.BoolVar(&cfg.EnableSecureHeaders, "enable-secure-headers", false, "enable secure headers")
	logErrorsOnly := flag.Bool("log-errors-only", false, "only log errors")
	flag.Parse()
	if *logErrorsOnly {
		legacy.ErrorsOnly()
	}

	s := &deltaServer{cfg: cfg}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := s.reset(ctx, "")
	if err == nil {
		err = s.start(ctx, nil)
	}
	cancel()
	if err != nil {
		stdlog.Fatal(err)
	}

	srv := &http.Server{Addr: *addr, Handler: s.routes()}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	stdlog.Println("Cypress controls listening on", *addr)
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	select {
	case <-sigCtx.Done():
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = errors.Join(srv.Shutdown(ctx), s.close(ctx))
	case err := <-errCh:
		_ = s.close(context.Background())
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			stdlog.Fatal(err)
		}
	}
}
