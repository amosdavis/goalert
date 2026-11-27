package apikey

import (
	"context"
	"net"

	"github.com/google/uuid"
	"github.com/target/goalert/validation/validate"
)

// _updateLastUsed will record usage for the given API key ID, user agent, and IP address.
func (s *Store) _updateLastUsed(ctx context.Context, id uuid.UUID, ua, ip string) error {
	ua = validate.SanitizeText(ua, 1024)
	ip, _, _ = net.SplitHostPort(ip)
	ip = validate.SanitizeText(ip, 255)
	return s.delta.recordUsage(ctx, id, ua, net.ParseIP(ip))
}
