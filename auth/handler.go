package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/target/goalert/auth/authtoken"
	"github.com/target/goalert/config"
	"github.com/target/goalert/integrationkey"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/user"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/validation"
	"github.com/target/goalert/validation/validate"
)

// CookieName is the name of the auth session cookie.
const (
	CookieName   = "goalert_session.2"
	v1CookieName = "goalert_session"
)

type registeredProvider struct {
	// ID is the unique identifier of the provider.
	ID string

	// URL is the location of the form action (POST).
	URL string

	ProviderInfo
}

// Handler will serve authentication requests for registered identity providers.
type Handler struct {
	providers map[string]IdentityProvider
	cfg       HandlerConfig

	txdb  txn.DB
	delta *deltaAuth // delta.go
}

// UserSession represents an active user session.
type UserSession struct {
	ID           string
	UserAgent    string
	CreatedAt    time.Time
	LastAccessAt time.Time
	UserID       string
}

func (h *Handler) EndUserSessionTx(ctx context.Context, tx txn.Tx, id ...string) error {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return err
	}
	ids, err := validate.ParseManyUUID("SessionID", id, -1)
	if err != nil {
		return err
	}
	if permission.Admin(ctx) {
		return h.delta.end(ctx, tx, ids)
	}
	return h.delta.endOwn(ctx, tx, permission.UserNullUUID(ctx), ids)
}

// EndAllUserSessionsTx ends all sessions other than the user's currently active session
func (h *Handler) EndAllUserSessionsTx(ctx context.Context, tx txn.Tx) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.MatchUser(permission.UserID(ctx)))
	if err != nil {
		return err
	}

	// get current session id
	src := permission.Source(ctx)

	return h.delta.endAllOther(ctx, tx, permission.UserNullUUID(ctx), src)
}

func (h *Handler) FindAllUserSessions(ctx context.Context, userID string) ([]UserSession, error) {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.MatchUser(userID))
	if err != nil {
		return nil, err
	}

	return h.delta.userSessions(ctx, userID)
}

// ServeLogout will clear the current session cookie and end the session(s) (if any).
func (h *Handler) ServeLogout(w http.ResponseWriter, req *http.Request) {
	ClearCookie(w, req, CookieName, true)
	var sessionIDs []string
	for _, c := range req.Cookies() {
		switch c.Name {
		case CookieName, v1CookieName:
		default:
			// only interested in cookies with one of the names above
			continue
		}

		tok, _, _ := authtoken.Parse(c.Value, nil)
		if tok == nil {
			continue
		}
		sessionIDs = append(sessionIDs, tok.ID.String())
	}
	ctx := req.Context()
	src := permission.Source(ctx)
	if src != nil && src.Type == permission.SourceTypeAuthProvider {
		sessionIDs = append(sessionIDs, src.ID)
	}

	if len(sessionIDs) == 0 {
		// no session to end
		return
	}

	bgCtx := log.FromContext(ctx).BackgroundContext()
	ids, err := validate.ParseManyUUID("SessionID", sessionIDs, -1)
	if err == nil {
		err = h.delta.end(bgCtx, nil, ids)
	}
	if err != nil {
		log.Log(ctx, errors.Wrap(err, "end session(s)"))
	}
}

// ServeProviders will return a list of the currently enabled identity providers.
func (h *Handler) ServeProviders(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	ctx := req.Context()
	info := make([]registeredProvider, 0, len(h.providers))

	u, err := url.Parse(req.RequestURI)
	if errutil.HTTPError(ctx, w, err) {
		return
	}
	// Detect current pathPrefix instead of using CallbackURL since it
	// will be used for browser linking.
	//
	// Also handles edge cases around first-time setup/localhost/etc...
	pathPrefix := strings.TrimSuffix(u.Path, req.URL.Path)

	for id, p := range h.providers {
		if !p.Info(ctx).Enabled {
			continue
		}

		info = append(info, registeredProvider{
			ID:           id,
			URL:          path.Join(pathPrefix, "/api/v2/identity/providers", url.PathEscape(id)),
			ProviderInfo: p.Info(ctx),
		})
	}

	sort.Slice(info, func(i, j int) bool { return info[i].ID < info[j].ID })
	data, err := json.Marshal(info)
	if errutil.HTTPError(req.Context(), w, err) {
		return
	}
	_, _ = w.Write(data)
}

// IdentityProviderHandler will return a handler for the given provider ID.
//
// It panics if the id has not been registered with AddIdentityProvider.
func (h *Handler) IdentityProviderHandler(id string) http.HandlerFunc {
	p, ok := h.providers[id]
	if !ok {
		panic("IdentityProvider " + id + " does not exist")
	}

	return func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		cfg := config.FromContext(ctx)

		var refU *url.URL
		if req.Method == "POST" {
			if cfg.ShouldUsePublicURL() {
				refU, _ = url.Parse(req.Header.Get("referer"))
				if refU == nil || !cfg.ValidReferer("", req.Header.Get("referer")) {
					// redirect with err
					q := make(url.Values)
					q.Set("login_error", "invalid referer")
					http.Redirect(w, req, cfg.CallbackURL("", q), http.StatusTemporaryRedirect)
					return
				}
			} else {
				// fallback to old method
				var ok bool
				refU, ok = h.refererURL(w, req)
				if !ok {
					errutil.HTTPError(ctx, w, validation.NewFieldError("referer", "failed to resolve referer"))
					return
				}
			}
		} else {
			c, err := req.Cookie("login_redir")
			if err != nil {
				errutil.HTTPError(ctx, w, validation.NewFieldError("login_redir", err.Error()))
				return
			}
			refU, _ = url.Parse(c.Value)
			if refU == nil || !cfg.ValidReferer(req.URL.String(), c.Value) {
				// redirect with err
				q := make(url.Values)
				q.Set("login_error", "invalid referer")
				http.Redirect(w, req, cfg.CallbackURL("", q), http.StatusTemporaryRedirect)
				return
			}
		}

		info := p.Info(ctx)
		if !info.Enabled {
			err := Error(info.Title + " auth disabled")
			q := refU.Query()
			q.Set("login_error", err.Error())
			refU.RawQuery = q.Encode()
			http.Redirect(w, req, refU.String(), http.StatusFound)
			return
		}

		if req.Method == "POST" {
			h.serveProviderPost(id, p, refU, w, req)
			return
		}

		h.handleProvider(id, p, refU, w, req)
	}
}

// A Redirector provides a target URL for redirecting a user.
type Redirector interface {
	RedirectURL() string
}

// RedirectURL is a convenience type that can be returned as an error
// resulting in redirection. It implements the error and Redirector interfaces.
type RedirectURL string

// An Error can be returned to indicate an error message that should be displayed to
// the user attempting to authenticate.
type Error string

// ClientError indicates an error meant for the client to see.
func (Error) ClientError() bool { return true }

func (a Error) Error() string { return string(a) }

func (RedirectURL) Error() string { return "must redirect to acquire identity" }

// RedirectURL implements the Redirector interface.
func (r RedirectURL) RedirectURL() string { return string(r) }

func (h *Handler) canCreateUser(ctx context.Context, providerID string) bool {
	cfg := config.FromContext(ctx)
	switch providerID {
	case "oidc":
		return cfg.OIDC.NewUsers

	}

	return false
}

func (h *Handler) handleProvider(id string, p IdentityProvider, refU *url.URL, w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	var route RouteInfo
	route.RelativePath = strings.TrimPrefix(req.URL.Path, "/api/v2/identity/providers/"+id)
	if route.RelativePath == "" {
		route.RelativePath = "/"
	}

	cfg := config.FromContext(ctx)
	if cfg.ShouldUsePublicURL() {
		route.CurrentURL = cfg.CallbackURL(req.URL.Path)
	} else {
		u := *req.URL
		u.RawQuery = "" // strip query params
		route.CurrentURL = u.String()
	}

	sub, err := p.ExtractIdentity(&route, w, req)
	var r Redirector
	if errors.As(err, &r) {
		http.Redirect(w, req, r.RedirectURL(), http.StatusFound)
		return
	}
	noRedirect := req.FormValue("noRedirect") == "1"

	errRedirect := func(err error) {
		q := refU.Query()
		old := err
		_, err = errutil.ScrubError(err)
		if err != old {
			log.Log(ctx, old)
		}
		q.Set("login_error", err.Error())
		refU.RawQuery = q.Encode()
		if noRedirect {
			if err != old {
				w.WriteHeader(500)
			} else {
				w.WriteHeader(400)
			}
			_, _ = io.WriteString(w, err.Error())
			return
		}
		http.Redirect(w, req, refU.String(), http.StatusFound)
	}

	if err != nil {
		errRedirect(err)
		return
	}

	userID, err := h.delta.subjectUser(ctx, id, sub.SubjectID)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	if err != nil {
		errRedirect(err)
		return
	}

	var newUser bool
	if userID == "" {
		newUser = true

		if !h.canCreateUser(ctx, id) {
			errRedirect(Error("New users are not allowed right now, but you can try again later."))
			log.Log(ctx, errors.New("create user: disabled for provider"))
			return
		}

		// create user, with its subject, in one transaction
		err = h.txdb.Run(ctx, nil, func(tx txn.Tx) error {
			// Built inside, so that a rerun starts from scratch.
			u := &user.User{
				Role:  permission.RoleUser,
				Name:  validate.SanitizeName(sub.Name),
				Email: validate.SanitizeEmail(sub.Email),
			}
			var err error
			permission.SudoContext(ctx, func(ctx context.Context) {
				u, err = h.cfg.UserStore.InsertTx(ctx, tx, u)
			})
			if err != nil {
				return err
			}
			err = h.delta.addSubject(ctx, tx, user.AuthSubject{ProviderID: id, SubjectID: sub.SubjectID, UserID: u.ID})
			if err != nil {
				return err
			}
			userID = u.ID
			return nil
		})
		if err != nil {
			errRedirect(err)
			return
		}
	} else {
		name, email := validate.SanitizeName(sub.Name), validate.SanitizeEmail(sub.Email)
		err = h.delta.updateProfile(ctx, userID, name, email)
		if err != nil {
			log.Log(ctx, errors.Wrap(err, "update user info"))
		}
	}

	tok, err := h.CreateSession(ctx, req.UserAgent(), userID)
	if err != nil {
		errRedirect(err)
		return
	}
	tokStr, err := tok.Encode(h.cfg.SessionKeyring.Sign)
	if err != nil {
		errRedirect(err)
		return
	}

	if noRedirect {
		_, _ = io.WriteString(w, tokStr)
		return
	}

	h.setSessionCookie(w, req, tokStr)

	if newUser {
		q := refU.Query()
		q.Set("isFirstLogin", "1")
		refU.RawQuery = q.Encode()
	}

	http.Redirect(w, req, refU.String(), http.StatusFound)
}

// CreateSession will start a new session for the given UserID, returning a newly signed token.
func (h *Handler) CreateSession(ctx context.Context, userAgent, userID string) (*authtoken.Token, error) {
	tok := &authtoken.Token{
		Version: 1,
		Type:    authtoken.TypeSession,
		ID:      uuid.New(),
	}
	err := h.delta.start(ctx, tok.ID, userAgent, userID)
	if err != nil {
		return nil, err
	}

	return tok, nil
}

func (h *Handler) setSessionCookie(w http.ResponseWriter, req *http.Request, val string) {
	SetCookieAge(w, req, CookieName, val, 30*24*time.Hour, true)
}

func (h *Handler) authWithToken(w http.ResponseWriter, req *http.Request, next http.Handler) bool {
	err := req.ParseMultipartForm(32 << 20) // 32<<20 (32MiB) value is the `defaultMaxMemory` used in the net/http package when `req.FormValue` is called
	if err != nil && !errors.Is(err, http.ErrNotMultipart) {
		http.Error(w, err.Error(), 400)
		return true
	}

	tokStr := GetToken(req)
	if tokStr == "" {
		return false
	}

	ctx := req.Context()
	if req.URL.Path == "/api/graphql" && strings.HasPrefix(tokStr, "ey") {
		ctx, err = h.cfg.APIKeyStore.AuthorizeGraphQL(ctx, tokStr, req.UserAgent(), req.RemoteAddr)
		if errutil.HTTPError(req.Context(), w, err) {
			return true
		}

		next.ServeHTTP(w, req.WithContext(ctx))
		return true
	}

	tok, _, err := authtoken.Parse(tokStr, func(t authtoken.Type, p, sig []byte) (bool, bool) {
		if t == authtoken.TypeSession {
			return h.cfg.SessionKeyring.Verify(p, sig)
		}

		return h.cfg.APIKeyring.Verify(p, sig)
	})
	if errutil.HTTPError(req.Context(), w, err) {
		return true
	}

	switch req.URL.Path {
	case "/api/v2/generic/incoming":
		ctx, err = h.cfg.IntKeyStore.Authorize(ctx, *tok, integrationkey.TypeGeneric)
	default:
		return false
	}

	if errutil.HTTPError(req.Context(), w, err) {
		return true
	}

	next.ServeHTTP(w, req.WithContext(ctx))
	return true
}

func (h *Handler) tryAuthUser(ctx context.Context, w http.ResponseWriter, req *http.Request, tokenStr string, isCookie bool) (context.Context, error) {
	tok, isOld, err := authtoken.Parse(tokenStr, func(t authtoken.Type, p, sig []byte) (bool, bool) {
		// only session tokens are supported for cookies
		return h.cfg.SessionKeyring.Verify(p, sig)
	})
	if err != nil {
		return nil, err
	}

	userID, userRole, err := h.delta.session(ctx, tok.ID)
	if err != nil {
		return nil, err
	}

	if isCookie && isOld {
		// send new signature back if it was signed with an old key
		newSignedToken, err := tok.Encode(h.cfg.SessionKeyring.Sign)
		if err != nil {
			log.Log(ctx, errors.Wrap(err, "failed to sign/issue new session token"))
		} else {
			h.setSessionCookie(w, req, newSignedToken)
			err = h.delta.setUserAgent(ctx, tok.ID, req.UserAgent())
			if err != nil {
				log.Log(ctx, errors.Wrap(err, "update user agent (session key refresh)"))
			}
		}
	}

	return permission.UserSourceContext(
		ctx,
		userID.String(),
		userRole,
		&permission.SourceInfo{
			Type: permission.SourceTypeAuthProvider,
			ID:   tok.ID.String(),
		},
	), nil
}

// WrapHandler will wrap an existing http.Handler so the Context of the request
// includes authentication information (if the request is authorized).
//
// Updating and clearing the session cookie is automatically handled.
func (h *Handler) WrapHandler(wrapped http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if h.authWithToken(w, req, wrapped) {
			return
		}

		// User session flow

		tokStr := GetToken(req)
		// explicit token always takes precedence
		if tokStr != "" {
			ctx, err := h.tryAuthUser(req.Context(), w, req, tokStr, false)
			if err != nil {
				wrapped.ServeHTTP(w, req)
				return
			}

			wrapped.ServeHTTP(w, req.WithContext(ctx))
			return
		}

		for _, c := range req.Cookies() {
			switch c.Name {
			case CookieName, v1CookieName:
			default:
				// only interested in cookies with one of the names above
				continue
			}

			ctx, err := h.tryAuthUser(req.Context(), w, req, c.Value, true)
			if err != nil {
				continue
			}

			wrapped.ServeHTTP(w, req.WithContext(ctx))
			return
		}

		wrapped.ServeHTTP(w, req)
	})
}

func (h *Handler) refererURL(w http.ResponseWriter, req *http.Request) (*url.URL, bool) {
	ref := req.Header.Get("referer")
	ctx := req.Context()
	cfg := config.FromContext(ctx)
	refU, err := url.Parse(ref)
	if err != nil {
		errutil.HTTPError(ctx, w, validation.NewFieldError("referer", err.Error()))
		return nil, false
	}

	if !cfg.ValidReferer(req.URL.String(), ref) {
		err := validation.NewFieldError("referer", "wrong host/path")
		ctx = log.WithFields(ctx, log.Fields{
			"AuthRefererURLs": cfg.Auth.RefererURLs,
			"PublicURL":       cfg.PublicURL(),
		})
		log.Log(ctx, err)
		errutil.HTTPError(ctx, w, err)
		return nil, false
	}

	q := refU.Query() // reset existing login params
	q.Del("isFirstLogin")
	q.Del("login_error")
	refU.RawQuery = q.Encode()
	return refU, true
}

func (h *Handler) serveProviderPost(id string, p IdentityProvider, refU *url.URL, w http.ResponseWriter, req *http.Request) {
	SetCookie(w, req, "login_redir", refU.String(), false)

	h.handleProvider(id, p, refU, w, req)
}

// AddIdentityProvider registers a new IdentityProvider with the given ID.
func (h *Handler) AddIdentityProvider(id string, idp IdentityProvider) error {
	if h.providers[id] != nil {
		return errors.Errorf("provider already exists with id '%s'", id)
	}

	h.providers[id] = idp
	return nil
}
