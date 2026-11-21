package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/target/goalert/app"
	"github.com/target/goalert/devtools/pgdump-lite"
	"github.com/target/goalert/devtools/pgmocktime"
	"github.com/target/goalert/migrate"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/util/sqlutil"
)

// storage is where a test's GoAlert keeps its data. Everything the harness
// does to that data, other than through GoAlert itself, goes through this
// interface, so the suite can run against another backend
// (docs/filestore-plan.md, phase 0).
type storage interface {
	// Migrate brings the schema to the named migration, or to the latest
	// one if target is empty. MigrateAll applies every pending migration.
	// A backend without migrations accepts only the latest schema.
	Migrate(ctx context.Context, target string) error
	MigrateAll(ctx context.Context) error

	// Exec runs fixture SQL: INSERT ... VALUES, UPDATE ... SET ... WHERE
	// column = value, and DELETE.
	Exec(ctx context.Context, sql string) error

	// Now reads the storage's clock. SetClockSpeed sets how fast it runs
	// (0 freezes it, 1 is real time) and AdvanceClock moves it forward.
	Now(ctx context.Context) (time.Time, error)
	SetClockSpeed(ctx context.Context, speed float64) error
	AdvanceClock(ctx context.Context, d time.Duration) error

	// PendingJobs counts jobs in the given queues that are, or are about to
	// be, worked. CountJobs counts jobs of a kind in a queue whose args
	// contain argsContains, optionally limited to some states. EnqueueJob
	// adds a job the same way the database triggers do.
	PendingJobs(ctx context.Context, queues []string) (int, error)
	CountJobs(ctx context.Context, queue, kind string, argsContains map[string]any, states []string) (int, error)
	EnqueueJob(ctx context.Context, queue, kind, id string, args map[string]any) error

	// ConfigureApp points an app config at this storage. NewApp starts
	// GoAlert on it; it can be called again to restart.
	ConfigureApp(cfg *app.Config)
	NewApp(ctx context.Context, cfg app.Config) (*app.App, error)

	// Dump writes all stored data to w, for debugging a failed test.
	Dump(ctx context.Context, w io.Writer) error

	// Close releases the storage and deletes its data.
	Close(ctx context.Context) error

	// String describes where the data is, for log messages.
	String() string
}

// postgresStorage keeps a test's data in its own Postgres database, with
// time controlled by pgmocktime.
type postgresStorage struct {
	t      *testing.T
	name   string
	url    string
	pgTime *pgmocktime.Mocker

	// pool is created by the first NewApp and shared with GoAlert, which is
	// how the harness has always queried the database.
	pool *pgxpool.Pool
}

var _ storage = (*postgresStorage)(nil)

// newPostgresStorage creates an empty database for one test, with its
// clock frozen.
func newPostgresStorage(ctx context.Context, t *testing.T) (*postgresStorage, error) {
	t.Helper()
	t.Logf("Using DB URL: %s", dbURL)
	name := strings.ReplaceAll("smoketest_"+time.Now().Format("2006_01_02_15_04_05")+uuid.New().String(), "-", "")

	conn, err := pgx.Connect(ctx, DBURL(""))
	if err != nil {
		return nil, fmt.Errorf("connect to db: %w", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "create database "+sqlutil.QuoteID(name))
	if err != nil {
		return nil, fmt.Errorf("create db: %w", err)
	}
	conn.Close(ctx)
	t.Logf("created test database '%s': %s", name, dbURL)

	pgTime, err := pgmocktime.New(ctx, DBURL(name))
	if err != nil {
		return nil, fmt.Errorf("create pgmocktime: %w", err)
	}
	s := &postgresStorage{t: t, name: name, url: DBURL(name), pgTime: pgTime}
	if err := pgTime.Inject(ctx); err != nil {
		return nil, err
	}
	if err := pgTime.SetSpeed(ctx, 0); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *postgresStorage) String() string { return s.url }

func migrateLogger(ctx context.Context) context.Context {
	l := log.NewLogger()
	l.ErrorsOnly()
	return log.WithLogger(ctx, l)
}

func (s *postgresStorage) Migrate(ctx context.Context, target string) error {
	_, err := migrate.Up(migrateLogger(ctx), s.url, target)
	return err
}

func (s *postgresStorage) MigrateAll(ctx context.Context) error {
	_, err := migrate.ApplyAll(migrateLogger(ctx), s.url)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Errorf("%#v", pgErr)
	}
	return err
}

func (s *postgresStorage) Exec(ctx context.Context, sql string) error {
	return ExecSQLBatch(ctx, s.url, sql)
}

func (s *postgresStorage) Now(ctx context.Context) (time.Time, error) {
	var now time.Time
	err := s.pool.QueryRow(ctx, "SELECT NOW()").Scan(&now)
	return now, err
}

func (s *postgresStorage) SetClockSpeed(ctx context.Context, speed float64) error {
	return s.pgTime.SetSpeed(ctx, speed)
}

func (s *postgresStorage) AdvanceClock(ctx context.Context, d time.Duration) error {
	return s.pgTime.AdvanceTime(ctx, d)
}

// pendingJobsQuery counts jobs in the given queues that are, or are about to
// be, worked. Rows in 'scheduled' or 'retryable' whose scheduled_at has passed
// are counted because River only promotes them to 'available' on its scheduler
// interval (5s), so right after a FastForward they are due but not yet picked
// up. now() here is the mocked DB clock (see devtools/pgmocktime), so it
// follows FastForward.
const pendingJobsQuery = `
	select count(*)
	from river_job
	where queue = any($1)
	and (
		state in ('available', 'running', 'pending')
		or (state in ('scheduled', 'retryable') and scheduled_at <= now())
	)
`

func (s *postgresStorage) PendingJobs(ctx context.Context, queues []string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, pendingJobsQuery, queues).Scan(&n)
	return n, err
}

func (s *postgresStorage) CountJobs(ctx context.Context, queue, kind string, argsContains map[string]any, states []string) (int, error) {
	if argsContains == nil {
		argsContains = map[string]any{}
	}
	args, err := json.Marshal(argsContains)
	if err != nil {
		return 0, fmt.Errorf("marshal job args: %w", err)
	}
	if len(states) == 0 {
		states = nil // encodes as NULL
	}

	var n int
	err = s.pool.QueryRow(ctx, `
		select count(*)
		from river_job
		where queue = $1
		and kind = $2
		and args @> $3::jsonb
		and ($4::text[] is null or state::text = any($4::text[]))
	`, queue, kind, string(args), states).Scan(&n)
	return n, err
}

// EnqueueJob inserts a River job using fn_util_river_job, the same SQL
// function DB triggers use to enqueue work.
//
// The call is made as a single statement in its own (implicit) transaction:
// fn_util_river_job dedupes by (queue, id) within a transaction via a
// transaction-local set_config guard, so each call always inserts.
func (s *postgresStorage) EnqueueJob(ctx context.Context, queue, kind, id string, args map[string]any) error {
	if args == nil {
		args = map[string]any{}
	}
	data, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("marshal job args: %w", err)
	}
	_, err = s.pool.Exec(ctx, `select fn_util_river_job($1, $2, $3, $4::jsonb)`, queue, kind, id, string(data))
	return err
}

func (s *postgresStorage) ConfigureApp(cfg *app.Config) {
	cfg.ForceRiverDBTime = true
	cfg.DBURL = s.url
	cfg.DBMaxOpen = 5
}

func (s *postgresStorage) NewApp(ctx context.Context, cfg app.Config) (*app.App, error) {
	if s.pool == nil {
		poolCfg, err := pgxpool.ParseConfig(s.url)
		if err != nil {
			return nil, fmt.Errorf("failed to parse db url: %w", err)
		}
		poolCfg.MaxConns = 5

		s.pool, err = pgxpool.NewWithConfig(ctx, poolCfg)
		if err != nil {
			return nil, fmt.Errorf("create pgx pool: %w", err)
		}
	}
	return app.NewApp(cfg, s.pool)
}

func (s *postgresStorage) Dump(ctx context.Context, w io.Writer) error {
	conn, err := pgx.Connect(ctx, s.url)
	if err != nil {
		return fmt.Errorf("failed to get db connection: %w", err)
	}
	defer conn.Close(ctx)

	var t time.Time
	err = conn.QueryRow(ctx, "select now()").Scan(&t)
	if err != nil {
		return fmt.Errorf("failed to get current timestamp: %w", err)
	}

	err = pgdump.DumpData(ctx, conn, w, nil)
	if err != nil {
		return fmt.Errorf("failed to dump database '%s': %w", s.name, err)
	}

	_, err = fmt.Fprintf(w, "\n-- Last Timestamp: %s\n", t.Format(time.RFC3339Nano))
	return err
}

func (s *postgresStorage) Close(ctx context.Context) error {
	s.pgTime.Close()
	if s.pool != nil {
		s.pool.Close()
	}

	conn, err := pgx.Connect(ctx, DBURL(""))
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "drop database "+sqlutil.QuoteID(s.name))
	if err != nil {
		return fmt.Errorf("failed to drop database '%s': %w", s.name, err)
	}
	return nil
}
