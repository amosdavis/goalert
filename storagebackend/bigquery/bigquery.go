// Package bigquery registers the "bigquery" log: BigQuery decides commits, and
// the files and the Delta log are in the object store, which is a bucket.
//
//	bigquery://PROJECT?location=US
//	bigquery://PROJECT/DATASET?location=US&connection=PROJECT.us.CONNECTION
//
// With a dataset, the Delta table is also registered in BigQuery when it is
// created. Application default credentials are used. They need
// bigquery.jobs.create in the project, and to register the table,
// bigquery.tables.create and bigquery.connections.delegate. Parameters:
//
//	location    where the jobs are made that decide commits (required). It can't
//	            be changed later
//	connection  the BigLake connection that reads the bucket (with a dataset)
//	table       the registered table's name (default: goalert)
package bigquery

import (
	"context"
	"errors"
	"strings"

	"github.com/target/goalert/bigquerylog"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/tablelog"
	bq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

func init() { storagebackend.RegisterLog("bigquery", open) }

func open(ctx context.Context, cfg storagebackend.LogConfig) (tablelog.Log, func() error, error) {
	p := storagebackend.NewParams(cfg.URL)
	c := bigquerylog.Config{
		Project: cfg.URL.Host, Location: p.Get("location", ""),
		Store: cfg.Objects.Store, Warn: cfg.Warn,
	}
	if c.Project == "" || c.Location == "" {
		return nil, nil, errors.New("bigquery: the URL must be bigquery://PROJECT/DATASET?location=LOCATION, with or without the dataset")
	}
	table, connection := p.Get("table", ""), p.Get("connection", "")
	switch dataset := strings.Trim(cfg.URL.Path, "/"); {
	case dataset != "":
		if table == "" {
			table = "goalert"
		}
		c.Register = &bigquerylog.Registration{Dataset: dataset, Table: table, Connection: connection, URI: cfg.Objects.Location}
	case table != "" || connection != "":
		return nil, nil, errors.New("bigquery: a table and a connection need a dataset")
	}
	if err := p.Done(); err != nil {
		return nil, nil, err
	}
	// Anything wrong is found before connecting.
	check := c
	check.Service = &bq.Service{}
	if _, err := bigquerylog.New(check, cfg.WriterID); err != nil {
		return nil, nil, err
	}
	svc, err := bq.NewService(ctx, option.WithScopes(bq.BigqueryScope))
	if err != nil {
		return nil, nil, err
	}
	c.Service = svc
	log, err := bigquerylog.New(c, cfg.WriterID)
	if err != nil {
		return nil, nil, err
	}
	return log, nil, nil
}
