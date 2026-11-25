package storagebackend

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/tablelog"
)

// Config is a deployment's choice of storage.
type Config struct {
	// Objects is the URL of the object store that holds the data, such as
	// "gs://bucket/prefix". Its scheme picks the driver. It is required: there
	// is no default place, so a pod that was not told where the data is can't
	// start on other data (F27).
	Objects string

	// Log is what decides commits. Empty, or "delta", is the Delta log in the
	// object store itself. Anything else is the URL of a log's driver, such as
	// "iceberg+https://catalog/...".
	Log string

	// Warn, if set, receives problems that fail nothing.
	Warn func(error)
}

// Objects is an object store a driver opened.
type Objects struct {
	Store objstore.Store

	// Location is the URI by which other software, such as an Iceberg reader,
	// reaches the store's root. It is empty if none can, as with a document
	// database.
	Location string

	// Close, if set, closes the store's connection.
	Close func() error
}

// An ObjectDriver opens the object store at a URL. It must not create anything
// there.
type ObjectDriver func(ctx context.Context, u *url.URL) (Objects, error)

// LogConfig is what a LogDriver gets.
type LogConfig struct {
	URL      *url.URL
	Objects  Objects
	WriterID string // names this pod in its commits
	Warn     func(error)
}

// A LogDriver opens the log at a URL. The returned function, if not nil, closes
// its connection. It must not create anything.
type LogDriver func(ctx context.Context, cfg LogConfig) (tablelog.Log, func() error, error)

var (
	mu            sync.Mutex
	objectDrivers = map[string]ObjectDriver{}
	logDrivers    = map[string]LogDriver{}
)

// RegisterObjects makes a driver available for URLs with a scheme. It panics
// if the scheme is taken.
func RegisterObjects(scheme string, driver ObjectDriver) { register(objectDrivers, scheme, driver) }

// RegisterLog makes a driver available for URLs with a scheme. It panics if
// the scheme is taken.
func RegisterLog(scheme string, driver LogDriver) { register(logDrivers, scheme, driver) }

func register[D any](drivers map[string]D, scheme string, driver D) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := drivers[scheme]; ok || scheme == "" || scheme == "delta" {
		panic(fmt.Sprintf("storagebackend: the scheme %q is taken", scheme))
	}
	drivers[scheme] = driver
}

// Drivers returns the schemes of the drivers in this build.
func Drivers() (objects, logs []string) {
	mu.Lock()
	defer mu.Unlock()
	return slices.Sorted(maps.Keys(objectDrivers)), slices.Sorted(maps.Keys(logDrivers))
}

// find returns the driver for a URL. Errors have the URL's scheme only, since
// the rest may name customers or, against the rules, hold secrets (F20).
func find[D any](what, raw string, drivers map[string]D) (*url.URL, D, error) {
	var none D
	u, err := url.Parse(raw)
	if err != nil {
		return nil, none, fmt.Errorf("storagebackend: the %s URL can't be parsed", what)
	}
	mu.Lock()
	defer mu.Unlock()
	driver, ok := drivers[u.Scheme]
	if !ok {
		have := strings.Join(slices.Sorted(maps.Keys(drivers)), ", ")
		if have == "" {
			have = "none"
		}
		return nil, none, fmt.Errorf("storagebackend: no driver for %s URLs with the scheme %q (this build has: %s)", what, u.Scheme, have)
	}
	if u.User != nil {
		return nil, none, fmt.Errorf("storagebackend: the %s URL holds a user or password, and URLs end up in logs; use the environment or an identity (F20)", what)
	}
	if u.Fragment != "" || u.RawFragment != "" {
		return nil, none, fmt.Errorf("storagebackend: the %s URL has a fragment", what)
	}
	return u, driver, nil
}

// New opens the storage a deployment chose. It contacts nothing else, and
// creates nothing. writerID names this pod in its commits.
func New(ctx context.Context, cfg Config, writerID string) (*Backend, error) {
	if cfg.Objects == "" {
		objects, _ := Drivers()
		return nil, fmt.Errorf("storagebackend: no object store is configured (this build has drivers for: %s)", strings.Join(objects, ", "))
	}
	u, driver, err := find("object store", cfg.Objects, objectDrivers)
	if err != nil {
		return nil, err
	}
	// Find a log's mistakes before connecting to anything.
	var lu *url.URL
	var logDriver LogDriver
	if cfg.Log != "" && cfg.Log != "delta" {
		if lu, logDriver, err = find("log", cfg.Log, logDrivers); err != nil {
			return nil, err
		}
	}
	if cfg.Warn == nil {
		cfg.Warn = func(error) {}
	}

	objects, err := driver(ctx, u)
	if err != nil {
		return nil, fmt.Errorf("storagebackend: open the object store: %w", err)
	}
	b := &Backend{Store: objects.Store, Location: objects.Location}
	if objects.Close != nil {
		b.close = append(b.close, objects.Close)
	}
	fail := func(err error) (*Backend, error) { return nil, errors.Join(err, b.Close()) }
	if b.Store == nil {
		return fail(fmt.Errorf("storagebackend: the driver for %q opened nothing", u.Scheme))
	}
	if logDriver == nil {
		log := deltalog.New(b.Store, writerID)
		log.Warn = cfg.Warn
		b.Log = log
		return b, nil
	}
	log, closeLog, err := logDriver(ctx, LogConfig{URL: lu, Objects: objects, WriterID: writerID, Warn: cfg.Warn})
	if closeLog != nil {
		b.close = append(b.close, closeLog)
	}
	if err != nil {
		return fail(fmt.Errorf("storagebackend: open the log: %w", err))
	}
	if log == nil {
		return fail(fmt.Errorf("storagebackend: the driver for %q opened nothing", lu.Scheme))
	}
	b.Log = log
	return b, nil
}

// Params are a driver URL's query parameters. Done reports any that the driver
// did not ask for, so that a misspelled one is not silently ignored.
type Params struct {
	values url.Values
	errs   []error
}

// NewParams returns a URL's parameters.
func NewParams(u *url.URL) *Params {
	p := &Params{values: url.Values{}}
	values, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		p.errs = append(p.errs, errors.New("storagebackend: the URL's parameters can't be parsed"))
		return p
	}
	p.values = values
	return p
}

// Get returns a parameter's value, or def if it is not set.
func (p *Params) Get(name, def string) string {
	vs, ok := p.values[name]
	delete(p.values, name)
	switch {
	case !ok:
		return def
	case len(vs) != 1 || vs[0] == "":
		p.errs = append(p.errs, fmt.Errorf("storagebackend: the URL's parameter %q needs one value", name))
		return def
	}
	return vs[0]
}

// Done returns an error if a parameter is wrong, or unknown.
func (p *Params) Done() error {
	for _, name := range slices.Sorted(maps.Keys(p.values)) {
		p.errs = append(p.errs, fmt.Errorf("storagebackend: the URL has an unknown parameter %q", name))
	}
	return errors.Join(p.errs...)
}

// Prefix returns the rest of a URL's path as an object name prefix: empty, or
// ending in "/".
func Prefix(path string) (string, error) {
	path = strings.Trim(path, "/")
	if path == "" {
		return "", nil
	}
	if err := objstore.CheckName(path); err != nil {
		return "", fmt.Errorf("storagebackend: the URL's path can't be a prefix: %w", err)
	}
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." || part == ".." {
			return "", errors.New("storagebackend: the URL's path can't be a prefix")
		}
	}
	return path + "/", nil
}
