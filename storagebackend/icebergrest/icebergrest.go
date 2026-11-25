// Package icebergrest registers the "iceberg+https" and "iceberg+http" logs:
// an Iceberg REST catalog that decides commits.
//
//	iceberg+https://catalog.example.com/iceberg?namespace=ops&table=goalert
//
// The catalog's namespace must exist, and the object store must be one Iceberg
// readers can reach. Parameters:
//
//	namespace  the table's namespace, its levels separated by dots (required)
//	table      the table's name (default: goalert)
//	warehouse  the catalog's warehouse
//	prefix     the catalog's path prefix, if it does not announce one
//	snapshots  how many snapshots the catalog keeps (default: 100)
//	auth       how to authenticate (default: none):
//	             token       a bearer token, GOALERT_ICEBERG_TOKEN
//	             credential  OAuth2 client credentials, "id:secret" in
//	                         GOALERT_ICEBERG_CREDENTIAL
//	             google      Google's application default credentials
//	             sigv4       AWS signatures, for a region and service
//	oauth-uri, scope    for credential
//	project             for google: the project that is billed
//	region, service     for sigv4
package icebergrest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/target/goalert/iceberglog"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/tablelog"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// The environment variables that hold secrets.
const (
	TokenEnv      = "GOALERT_ICEBERG_TOKEN"
	CredentialEnv = "GOALERT_ICEBERG_CREDENTIAL"
)

// requestTimeout bounds one request, so that a catalog that does not answer
// can't hold up a commit for long.
const requestTimeout = 30 * time.Second

func init() {
	storagebackend.RegisterLog("iceberg+https", open)
	storagebackend.RegisterLog("iceberg+http", open)
}

func open(ctx context.Context, cfg storagebackend.LogConfig) (tablelog.Log, func() error, error) {
	if cfg.Objects.Location == "" {
		return nil, nil, errors.New("icebergrest: Iceberg readers can't reach this object store; use one with files, such as a bucket")
	}
	p := storagebackend.NewParams(cfg.URL)
	namespace := p.Get("namespace", "")
	name := p.Get("table", "goalert")
	snapshots, err := strconv.Atoi(p.Get("snapshots", strconv.Itoa(iceberglog.DefaultSnapshots)))
	if err != nil || snapshots < 1 {
		return nil, nil, errors.New("icebergrest: snapshots must be a positive number")
	}
	if namespace == "" {
		return nil, nil, errors.New("icebergrest: the URL needs a namespace parameter")
	}
	opts, transport, err := options(ctx, p)
	if err != nil {
		return nil, nil, err
	}
	if err := p.Done(); err != nil {
		return nil, nil, err
	}
	opts = append(opts, rest.WithCustomTransport(&timeoutTransport{next: transport}))

	scheme := strings.TrimPrefix(cfg.URL.Scheme, "iceberg+")
	base := url.URL{Scheme: scheme, Host: cfg.URL.Host, Path: cfg.URL.Path}
	cat, err := rest.NewCatalog(ctx, "goalert", base.String(), opts...)
	if err != nil {
		return nil, nil, Classify(fmt.Errorf("icebergrest: connect: %w", err))
	}
	closeCatalog := func() error {
		if c, ok := any(cat).(catalog.Closer); ok {
			return c.Close()
		}
		return nil
	}
	log, err := iceberglog.New(iceberglog.Config{
		Catalog: cat, Ident: append(table.Identifier(strings.Split(namespace, ".")), name),
		Store: cfg.Objects.Store, Location: cfg.Objects.Location,
		Classify: Classify, Snapshots: snapshots, Warn: cfg.Warn,
	}, cfg.WriterID)
	if err != nil {
		return nil, closeCatalog, err
	}
	return log, closeCatalog, nil
}

func options(ctx context.Context, p *storagebackend.Params) ([]rest.Option, http.RoundTripper, error) {
	var opts []rest.Option
	if w := p.Get("warehouse", ""); w != "" {
		opts = append(opts, rest.WithWarehouseLocation(w))
	}
	if prefix := p.Get("prefix", ""); prefix != "" {
		opts = append(opts, rest.WithPrefix(prefix))
	}
	var transport http.RoundTripper = http.DefaultTransport.(*http.Transport).Clone()
	secret := func(env string) (string, error) {
		if v := os.Getenv(env); v != "" {
			return v, nil
		}
		return "", fmt.Errorf("icebergrest: %s is not set", env)
	}
	switch auth := p.Get("auth", "none"); auth {
	case "none":
	case "token":
		token, err := secret(TokenEnv)
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, rest.WithOAuthToken(token))
	case "credential":
		cred, err := secret(CredentialEnv)
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, rest.WithCredential(cred))
		if raw := p.Get("oauth-uri", ""); raw != "" {
			u, err := url.Parse(raw)
			if err != nil {
				return nil, nil, errors.New("icebergrest: oauth-uri can't be parsed")
			}
			opts = append(opts, rest.WithAuthURI(u))
		}
		if scope := p.Get("scope", ""); scope != "" {
			opts = append(opts, rest.WithScope(scope))
		}
	case "google":
		// The context must outlive this call, since the source refreshes tokens.
		src, err := google.DefaultTokenSource(context.WithoutCancel(ctx), "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			return nil, nil, fmt.Errorf("icebergrest: Google credentials: %w", err)
		}
		transport = &oauth2.Transport{Source: src, Base: transport}
		if project := p.Get("project", ""); project != "" {
			opts = append(opts, rest.WithHeaders(map[string]string{"x-goog-user-project": project}))
		}
	case "sigv4":
		region := p.Get("region", "")
		if region == "" {
			return nil, nil, errors.New("icebergrest: sigv4 needs a region")
		}
		opts = append(opts, rest.WithSigV4RegionSvc(region, p.Get("service", "s3tables")))
	default:
		return nil, nil, fmt.Errorf("icebergrest: unknown auth %q", auth)
	}
	return opts, transport, nil
}

// Classify marks a REST catalog's errors for iceberglog. Only answers that mean
// a request was not done are marked so; anything else may have been (F32).
func Classify(err error) error {
	switch {
	case errors.Is(err, rest.ErrUnauthorized), errors.Is(err, rest.ErrForbidden), errors.Is(err, rest.ErrAuthorizationExpired):
		return objstore.Denied(err)
	case errors.Is(err, rest.ErrBadRequest), errors.Is(err, catalog.ErrNoSuchTable), errors.Is(err, catalog.ErrNoSuchNamespace):
		return fmt.Errorf("%w: %w", iceberglog.ErrRejected, err)
	}
	return err
}

// timeoutTransport bounds each request, and the reading of its answer.
type timeoutTransport struct{ next http.RoundTripper }

func (t *timeoutTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	resp, err := t.next.RoundTrip(r.WithContext(ctx))
	if err != nil {
		cancel()
		return nil, err
	}
	resp.Body = &body{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}
