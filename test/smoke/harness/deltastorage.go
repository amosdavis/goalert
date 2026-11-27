package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/target/goalert/app"
	"github.com/target/goalert/app/lifecycle"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/test/smoke/harness/fixture"
	"github.com/target/goalert/util/log"
)

// deltaStorage keeps a test's data in a Delta table in memory. It has a pod of
// its own for fixtures, so GoAlert sees them like any other pod's commits.
type deltaStorage struct {
	t     *testing.T
	store objstore.Store
	clock *filestore.FakeClock

	mu   sync.Mutex
	pods []*app.App
	done bool // the test has ended, and t can't be used any more

	pod    *filestore.DeltaDB
	stores *deltastores.Stores
}

var _ storage = (*deltaStorage)(nil)

// newDeltaStorage creates an empty table for one test, with its clock frozen.
func newDeltaStorage(ctx context.Context, t *testing.T) (_ *deltaStorage, err error) {
	t.Helper()
	s := &deltaStorage{t: t, store: objstore.NewMem(), clock: filestore.NewFakeClock(time.Now())}
	s.clock.SetSpeed(0)
	ctx = permission.SystemContext(ctx, "harness")
	if err := deltastores.Create(ctx, s.backend("create"), s.options("create")); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}
	var tables *deltastores.Tables
	s.pod, tables, err = deltastores.Open(ctx, s.backend("fixtures"), s.options("fixtures"))
	if err != nil {
		return nil, fmt.Errorf("open table: %w", err)
	}
	l := log.NewLogger()
	l.ErrorsOnly()
	s.stores, err = deltastores.New(log.WithLogger(context.WithoutCancel(ctx), l), s.pod, tables, deltastores.Options{
		Logger: l, Registry: nfydest.NewRegistry(), NoSecrets: true,
	})
	if err != nil {
		return nil, fmt.Errorf("open stores: %w", errors.Join(err, s.pod.Close()))
	}
	return s, nil
}

func (s *deltaStorage) String() string { return "a Delta table in memory" }

func (s *deltaStorage) backend(writer string) *storagebackend.Backend {
	return &storagebackend.Backend{Store: s.store, Log: deltalog.New(s.store, writer)}
}

func (s *deltaStorage) options(writer string) filestore.DeltaOptions {
	return filestore.DeltaOptions{
		Clock: s.clock, WriterID: writer,
		PollInterval: 20 * time.Millisecond, MaxPollInterval: 100 * time.Millisecond,
		Logf: s.logf,
	}
}

func (s *deltaStorage) logf(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.done {
		s.t.Logf(format, args...)
	}
}

func (s *deltaStorage) Exec(ctx context.Context, sql string) error {
	db := fixture.DB{Delta: s.pod, Stores: s.stores}
	return db.Exec(ctx, sql)
}

func (s *deltaStorage) Now(ctx context.Context) (time.Time, error) { return s.clock.Now(), nil }

func (s *deltaStorage) SetClockSpeed(ctx context.Context, speed float64) error {
	s.clock.SetSpeed(speed)
	return nil
}

// AdvanceClock moves the clock between the engine's cycles: otherwise a message
// that is being sent would seem to take that long.
func (s *deltaStorage) AdvanceClock(ctx context.Context, d time.Duration) error {
	s.mu.Lock()
	pods := slices.Clone(s.pods)
	s.mu.Unlock()
	for _, a := range pods {
		if err := a.WaitForStartup(ctx); err != nil || a.Engine == nil {
			continue
		}
		err := a.Engine.Pause(ctx)
		if errors.Is(err, lifecycle.ErrShutdown) || errors.Is(err, lifecycle.ErrAlreadyStarted) {
			continue
		}
		if err != nil {
			return fmt.Errorf("pause engine: %w", err)
		}
		defer func() { _ = a.Engine.Resume(ctx) }()
	}
	s.clock.Advance(d)
	return nil
}

func (s *deltaStorage) NewApp(ctx context.Context, cfg app.Config) (*app.App, error) {
	s.mu.Lock()
	name := fmt.Sprintf("pod-%d", len(s.pods)+1)
	s.mu.Unlock()

	db, tables, err := deltastores.Open(ctx, s.backend(name), s.options(name))
	if err != nil {
		return nil, fmt.Errorf("open table: %w", err)
	}
	a, err := app.NewStorageApp(cfg, &app.Storage{DB: db, Tables: tables, PodName: name})
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	s.mu.Lock()
	s.pods = append(s.pods, a)
	s.mu.Unlock()
	return a, nil
}

// Dump writes the rows of every table, as JSON.
func (s *deltaStorage) Dump(ctx context.Context, w io.Writer) error {
	// Catch up, for which any version that is not there yet will do.
	_ = s.pod.WaitFor(ctx, int64(s.pod.View().Index())+1)
	schema, view := s.pod.Schema(), s.pod.View()
	for _, name := range schema.TableNames() {
		rows, _ := schema.Rows(name)
		n := 0
		var err error
		rows.Ascend(view, func(row any) bool {
			if n++; n == 1 {
				_, err = fmt.Fprintf(w, "\n-- %s\n", name)
			}
			var data []byte
			if err == nil {
				data, err = json.Marshal(dumpRow(row))
			}
			if err == nil {
				_, err = fmt.Fprintf(w, "%s\n", data)
			}
			return err == nil
		})
		if err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, "\n-- Last Timestamp: %s\n", s.clock.Now().Format(time.RFC3339Nano))
	return err
}

// dumpRow is a row by the names of its columns, with JSON columns as they are.
func dumpRow(row any) map[string]any {
	v := reflect.ValueOf(row).Elem()
	m := make(map[string]any, v.NumField())
	for i := range v.NumField() {
		name, _, _ := strings.Cut(v.Type().Field(i).Tag.Get("parquet"), ",")
		val := v.Field(i).Interface()
		if data, ok := val.([]byte); ok && json.Valid(data) {
			val = json.RawMessage(data)
		}
		m[name] = val
	}
	return m
}

func (s *deltaStorage) Close(ctx context.Context) error {
	s.mu.Lock()
	s.done = true
	s.mu.Unlock()
	return errors.Join(s.stores.Close(ctx), s.pod.Close())
}
