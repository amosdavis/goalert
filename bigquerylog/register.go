package bigquerylog

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/target/goalert/objstore"
	"github.com/target/goalert/tablelog"
	bigquery "google.golang.org/api/bigquery/v2"
)

// Registration is the Delta table in BigQuery: an external table that reads
// the Delta log in the bucket whenever it is queried.
type Registration struct {
	// Dataset and Table name it.
	Dataset, Table string

	// Connection is the BigLake connection whose identity reads the bucket, such
	// as "my-project.us.goalert".
	Connection string

	// URI is the table's location, such as "gs://bucket/prefix".
	URI string
}

var validName = regexp.MustCompile(`^[A-Za-z0-9_]{1,1000}$`)

func (r *Registration) check() error {
	switch {
	case !validName.MatchString(r.Dataset) || !validName.MatchString(r.Table):
		return fmt.Errorf("bigquerylog: invalid dataset %q or table %q", r.Dataset, r.Table)
	case r.Connection == "":
		return errors.New("bigquerylog: registering the table needs a connection that can read the bucket")
	case !strings.HasPrefix(r.URI, "gs://"):
		return fmt.Errorf("bigquerylog: BigQuery can't read a table at %q; it must be in a Cloud Storage bucket", r.URI)
	}
	r.URI = strings.TrimRight(r.URI, "/")
	return nil
}

// Create creates the table, and registers it in BigQuery if that is
// configured. A table that could not be registered is created nonetheless,
// since GoAlert does not need it: Register tries again.
func (l *Log) Create(ctx context.Context, meta tablelog.MetaData, at time.Time) error {
	if err := l.Log.Create(ctx, meta, at); err != nil {
		return err
	}
	if err := l.Register(ctx); err != nil {
		l.a.cfg.Warn(fmt.Errorf("bigquerylog: the table was created, but not registered in BigQuery: %w", err))
	}
	return nil
}

// Register registers the Delta table in BigQuery, if that is configured and it
// is not yet. The table must have been created.
func (l *Log) Register(ctx context.Context) error {
	r := l.a.cfg.Register
	if r == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 2*requestTimeout)
	defer cancel()
	_, err := l.a.cfg.Service.Tables.Insert(l.a.cfg.Project, r.Dataset, &bigquery.Table{
		TableReference: &bigquery.TableReference{ProjectId: l.a.cfg.Project, DatasetId: r.Dataset, TableId: r.Table},
		Description:    "GoAlert's data, a Delta Lake table. Only GoAlert may write to it.",
		ExternalDataConfiguration: &bigquery.ExternalDataConfiguration{
			SourceFormat: "DELTA_LAKE",
			SourceUris:   []string{r.URI},
			ConnectionId: r.Connection,
			Autodetect:   true,
		},
	}).Context(ctx).Do()
	if err == nil {
		return nil
	}
	if code(err) != http.StatusConflict {
		return opErr("register the table", err)
	}
	// There is a table of that name. It may be this one.
	if problems := l.checkRegistration(ctx); len(problems) > 0 {
		return fmt.Errorf("bigquerylog: %s: %s", problems[0].Setting, problems[0].Problem)
	}
	return nil
}

// CheckSettings reports what is wrong with the table's registration in
// BigQuery. None of it breaks the table, which does not depend on it.
func (l *Log) CheckSettings(ctx context.Context) ([]objstore.SettingProblem, error) {
	if l.a.cfg.Register == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return l.checkRegistration(ctx), nil
}

func (l *Log) checkRegistration(ctx context.Context) []objstore.SettingProblem {
	r := l.a.cfg.Register
	name := r.Dataset + "." + r.Table
	var out []objstore.SettingProblem
	add := func(format string, args ...any) {
		out = append(out, objstore.SettingProblem{Setting: "BigQuery table " + name, Problem: fmt.Sprintf(format, args...)})
	}
	t, err := l.a.cfg.Service.Tables.Get(l.a.cfg.Project, r.Dataset, r.Table).Context(ctx).Do()
	switch {
	case code(err) == http.StatusNotFound:
		add("it is not registered; docs/bigquery.md says how to do it by hand")
		return out
	case err != nil:
		add("it can't be read: %v", err)
		return out
	}
	e := t.ExternalDataConfiguration
	if e == nil || e.SourceFormat != "DELTA_LAKE" {
		add("it is not an external Delta Lake table, so it is something else's")
		return out
	}
	if len(e.SourceUris) != 1 || strings.TrimRight(e.SourceUris[0], "/") != r.URI {
		add("it reads %v, not %s", e.SourceUris, r.URI)
	}
	if t.ExpirationTime > 0 {
		add("it expires at %s; GoAlert's data does not", time.UnixMilli(t.ExpirationTime).UTC().Format(time.RFC3339))
	}
	return out
}
