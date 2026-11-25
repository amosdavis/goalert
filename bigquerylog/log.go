// Package bigquerylog is a tablelog.Log whose commits BigQuery decides. The
// data files and the Delta log are in a bucket, and the Delta table can be
// registered in BigQuery, where it is queried.
//
// What decides is the ID of a job. A client names the job it inserts, and
// BigQuery "rejects any job creation request that reuses a previous job ID"
// (https://cloud.google.com/bigquery/docs/reliability-read), so of the jobs
// named after a version of a table one is made. Its labels say for which commit.
// The job itself does nothing: it costs nothing, and whether or when it runs
// does not matter. The rest is managedlog's: staging a commit, and copying it
// into the Delta log once it is decided.
//
// BigQuery keeps jobs for six months. Only the newest decision needs its job,
// and only until its commit is in the Delta log, moments later
// (docs/filestore-failure-modes.md, F50).
package bigquerylog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/managedlog"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/tablelog"
	bigquery "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
)

const (
	// requestTimeout bounds one request, so that a service that does not answer
	// can't hold up a commit for long.
	requestTimeout = 30 * time.Second

	// jobHistory is how long a job can surely be found. BigQuery keeps jobs for
	// six months.
	jobHistory = 150 * 24 * time.Hour

	// settleTime is how long after a commit its job may not be found yet,
	// without that meaning anything.
	settleTime = time.Minute

	// The labels of a job that decides a version.
	labelStore   = "goalert_table"
	labelVersion = "goalert_version"
	labelCommit  = "goalert_commit"
	labelFormat  = "goalert_format"
)

// Config says who decides.
type Config struct {
	Service *bigquery.Service

	// Project and Location are where the jobs are made. They are part of the
	// table's metadata, and can't change: another project's, or location's, jobs
	// would be decisions of their own (F43).
	Project, Location string

	// Store holds the table's files.
	Store objstore.Store

	// Register, if set, is where the Delta table is registered in BigQuery when
	// it is created.
	Register *Registration

	// Warn, if set, receives problems that fail nothing (F21).
	Warn func(error)
}

// Log is one writer's handle to a table.
type Log struct {
	*managedlog.Log
	a *arbiter
}

var _ tablelog.Log = (*Log)(nil)

// New returns a Log. It does no I/O.
func New(cfg Config, writerID string) (*Log, error) {
	a, err := newArbiter(cfg)
	if err != nil {
		return nil, err
	}
	l, err := managedlog.New(managedlog.Config{Arbiter: a, Store: cfg.Store, Warn: cfg.Warn}, writerID)
	if err != nil {
		return nil, err
	}
	return &Log{Log: l, a: a}, nil
}

// NewArbiter returns what decides a Log's commits, on its own.
func NewArbiter(cfg Config) (managedlog.Arbiter, error) { return newArbiter(cfg) }

var (
	validProject  = regexp.MustCompile(`^[a-z0-9.:-]{1,127}$`)
	validLocation = regexp.MustCompile(`^[A-Za-z0-9-]{2,64}$`)
)

// arbiter decides commits with BigQuery jobs.
type arbiter struct {
	cfg     Config
	manager string

	now func() time.Time

	mu sync.Mutex
	// confirmed is the newest decision this pod knows.
	confirmed decision
}

// decision is which commit holds a version of a table.
type decision struct {
	storeID  string
	version  int64
	commitID string
}

var (
	_ managedlog.Arbiter    = (*arbiter)(nil)
	_ managedlog.SelfTester = (*arbiter)(nil)
)

func newArbiter(cfg Config) (*arbiter, error) {
	switch {
	case cfg.Service == nil:
		return nil, errors.New("bigquerylog: nil service")
	case !validProject.MatchString(cfg.Project):
		return nil, fmt.Errorf("bigquerylog: invalid project %q", cfg.Project)
	case !validLocation.MatchString(cfg.Location):
		return nil, fmt.Errorf("bigquerylog: invalid location %q", cfg.Location)
	}
	if cfg.Register != nil {
		if err := cfg.Register.check(); err != nil {
			return nil, err
		}
	}
	if cfg.Warn == nil {
		cfg.Warn = func(error) {}
	}
	return &arbiter{
		cfg: cfg, now: time.Now,
		// Locations are the same in upper and lower case.
		manager: "bigquery:" + cfg.Project + "/" + strings.ToLower(cfg.Location),
	}, nil
}

// Manager names the project and the location, since jobs in others are other
// decisions.
func (a *arbiter) Manager() string { return a.manager }

var plainLabel = regexp.MustCompile(`^[a-z0-9_-]{1,63}$`)

// label is a value as a label can hold it.
func label(value string) string {
	if plainLabel.MatchString(value) {
		return value
	}
	sum := sha256.Sum256([]byte(value))
	return "sha256-" + hex.EncodeToString(sum[:20])
}

// jobID names the job that decides a version of a table.
func jobID(storeID string, version int64) string {
	return fmt.Sprintf("goalert-%s-%020d", label(storeID), version)
}

// insert makes a job with an ID, if there has never been one.
func (a *arbiter) insert(ctx context.Context, id string, labels map[string]string) error {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	_, err := a.cfg.Service.Jobs.Insert(a.cfg.Project, &bigquery.Job{
		JobReference: &bigquery.JobReference{ProjectId: a.cfg.Project, Location: a.cfg.Location, JobId: id},
		Configuration: &bigquery.JobConfiguration{
			Labels: labels,
			Query: &bigquery.JobConfigurationQuery{
				Query:    "SELECT 1 -- GoAlert: the ID of this job decides a commit",
				Priority: "BATCH",
				// False, and sent although it is.
				UseLegacySql: googleapi.Bool(false),
			},
		},
	}).Context(ctx).Do()
	return insertErr(err)
}

// get returns a job's labels, or nil if there is no job with the ID.
func (a *arbiter) get(ctx context.Context, id string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	job, err := a.cfg.Service.Jobs.Get(a.cfg.Project, id).Location(a.cfg.Location).Context(ctx).Do()
	if code(err) == http.StatusNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, opErr("look for a decision", err)
	}
	if job.Configuration == nil || job.Configuration.Labels == nil {
		return map[string]string{}, nil
	}
	return job.Configuration.Labels, nil
}

// SelfTest checks that BigQuery makes one job of an ID, and no other (F4, F33).
func (a *arbiter) SelfTest(ctx context.Context) error {
	id := "goalert-selftest-" + uuid.NewString()
	labels := map[string]string{labelCommit: "selftest"}
	// The first may be busy, or its reply lost, like any other.
	var err error
	for attempt := range 5 {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(selfTestDelay << attempt):
			}
		}
		err = a.insert(ctx, id, labels)
		if err == nil || errors.Is(err, tablelog.ErrConflict) || errors.Is(err, objstore.ErrDenied) || errors.Is(err, managedlog.ErrRejected) {
			break
		}
	}
	if err != nil && !errors.Is(err, tablelog.ErrConflict) {
		return fmt.Errorf("bigquerylog: make a job: %w", err)
	}
	switch err := a.insert(ctx, id, labels); {
	case err == nil:
		return errors.New("bigquerylog: a second job with the same ID was accepted, so two pods could commit the same version")
	case !errors.Is(err, tablelog.ErrConflict):
		return fmt.Errorf("bigquerylog: make a job again: %w", err)
	}
	got, err := a.get(ctx, id)
	if err != nil {
		return err
	}
	if got[labelCommit] != "selftest" {
		return errors.New("bigquerylog: a new job is missing, or has lost its labels")
	}
	return nil
}

var selfTestDelay = 100 * time.Millisecond

// Decision looks for a version's job. The object store decides version 0.
func (a *arbiter) Decision(ctx context.Context, storeID string, version int64) (managedlog.Decision, error) {
	if storeID == "" {
		return managedlog.Decision{}, tablelog.ErrNoTable
	}
	if version < 1 {
		return managedlog.Decision{}, nil
	}
	id := jobID(storeID, version)
	labels, err := a.get(ctx, id)
	if err != nil || labels == nil {
		return managedlog.Decision{}, err
	}
	commit := labels[labelCommit]
	if commit == "" || labels[labelStore] != label(storeID) || labels[labelVersion] != strconv.FormatInt(version, 10) {
		return managedlog.Decision{}, fmt.Errorf("%w: the BigQuery job %s is not GoAlert's (F50)", tablelog.ErrForeign, id)
	}
	format, err := strconv.Atoi(labels[labelFormat])
	if err != nil || format < 1 {
		return managedlog.Decision{}, fmt.Errorf("%w: the BigQuery job %s has GoAlert format %q", tablelog.ErrCorrupt, id, labels[labelFormat])
	}
	if format > deltalog.GoAlertFormat {
		return managedlog.Decision{}, fmt.Errorf("%w: the BigQuery job %s uses GoAlert format %d (this version reads %d)", tablelog.ErrUnsupported, id, format, deltalog.GoAlertFormat)
	}
	a.confirm(decision{storeID, version, commit})
	return managedlog.Decision{CommitID: commit}, nil
}

func (a *arbiter) confirm(d decision) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if d.storeID != a.confirmed.storeID || d.version >= a.confirmed.version {
		a.confirmed = d
	}
}

// decided checks that a version was decided for a commit (F43).
func (a *arbiter) decided(ctx context.Context, storeID string, version int64, commitID string) (managedlog.Decision, error) {
	if version < 1 {
		return managedlog.Decision{CommitID: commitID}, nil // by the object store
	}
	a.mu.Lock()
	known := a.confirmed == decision{storeID, version, commitID}
	a.mu.Unlock()
	if known {
		return managedlog.Decision{CommitID: commitID}, nil
	}
	d, err := a.Decision(ctx, storeID, version)
	if err == nil && d.CommitID != "" && d.CommitID != commitID {
		err = fmt.Errorf("%w: the Delta log has commit %s as version %d, but BigQuery decided for %s (F43)", tablelog.ErrForeign, commitID, version, d.CommitID)
	}
	return d, err
}

// Check checks that the newest version was decided for its commit, unless it is
// too old for its job to be found, or so new that its job may not be found yet.
func (a *arbiter) Check(ctx context.Context, s *tablelog.Snapshot) error {
	d, err := a.decided(ctx, s.MetaData.ID, s.Version, s.CommitID)
	if err != nil || d.CommitID != "" {
		return err
	}
	return a.undecided(s.Version, s.Time)
}

// undecided judges a published version of which BigQuery has no job, by the
// time of its commit.
func (a *arbiter) undecided(version int64, at time.Time) error {
	switch age := a.now().Sub(at); {
	case age > jobHistory:
		return nil // forgotten
	case age < settleTime:
		// Try again: it may not be found yet.
		return fmt.Errorf("%w: BigQuery does not have its decision on version %d yet", tablelog.ErrConflict, version)
	}
	return fmt.Errorf("%w: the Delta log has version %d, but BigQuery decided on no commit for it (F43, F50)", tablelog.ErrForeign, version)
}

// Prepare checks what the commit is based on. The job it takes to decide needs
// nothing else.
func (a *arbiter) Prepare(ctx context.Context, p managedlog.Proposal) (managedlog.Ask, error) {
	storeID := p.Info.GoAlertStoreID
	d, err := a.decided(ctx, storeID, p.Version-1, p.Base)
	if err != nil {
		return nil, err
	}
	if d.CommitID == "" && p.Version > 1 {
		at, err := a.baseTime(ctx, p.Version-1)
		if err != nil {
			return nil, err
		}
		if err := a.undecided(p.Version-1, at); err != nil {
			return nil, err
		}
	}
	id := jobID(storeID, p.Version)
	labels := map[string]string{
		labelStore:   label(storeID),
		labelVersion: strconv.FormatInt(p.Version, 10),
		labelCommit:  label(p.Info.GoAlertCommitID),
		labelFormat:  strconv.Itoa(p.Info.GoAlertFormat),
	}
	return func(ctx context.Context) error {
		if err := a.insert(ctx, id, labels); err != nil {
			return err
		}
		a.confirm(decision{storeID, p.Version, p.Info.GoAlertCommitID})
		return nil
	}, nil
}

// baseTime is when a published commit was made.
func (a *arbiter) baseTime(ctx context.Context, version int64) (time.Time, error) {
	data, err := a.cfg.Store.Get(ctx, deltalog.CommitName(version))
	if err != nil {
		return time.Time{}, err
	}
	info, _, err := deltalog.Decode(data)
	return time.UnixMicro(info.GoAlertTime), err
}

func code(err error) int {
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		return gerr.Code
	}
	return 0
}

// throttled reports whether BigQuery refused a request because of a quota or a
// rate, which it answers like a missing permission (F50).
func throttled(err error) bool {
	var gerr *googleapi.Error
	if !errors.As(err, &gerr) {
		return false
	}
	if gerr.Code == http.StatusTooManyRequests {
		return true
	}
	if gerr.Code != http.StatusForbidden {
		return false
	}
	for _, e := range gerr.Errors {
		switch e.Reason {
		case "rateLimitExceeded", "userRateLimitExceeded", "jobRateLimitExceeded", "quotaExceeded":
			return true
		}
	}
	return false
}

func denied(err error) bool {
	c := code(err)
	return c == http.StatusUnauthorized || c == http.StatusForbidden && !throttled(err)
}

// opErr wraps an error from a request that changes nothing.
func opErr(op string, err error) error {
	err = fmt.Errorf("bigquerylog: %s: %w", op, err)
	if denied(err) {
		return objstore.Denied(err)
	}
	return err
}

// insertErr classifies an error from making a job. Any that is not marked as
// having done nothing may have made it (F32).
func insertErr(err error) error {
	if err == nil {
		return nil
	}
	err = fmt.Errorf("bigquerylog: make a job: %w", err)
	switch c := code(err); {
	case c == http.StatusConflict:
		// There is, or was, a job with the ID, perhaps from an earlier attempt.
		return fmt.Errorf("%w: %w", tablelog.ErrConflict, err)
	case throttled(err):
		return fmt.Errorf("%w: %w", managedlog.ErrBusy, err)
	case denied(err):
		return objstore.Denied(err)
	case c >= 400 && c < 500 && c != http.StatusRequestTimeout:
		return fmt.Errorf("%w: %w", managedlog.ErrRejected, err)
	}
	return err
}
