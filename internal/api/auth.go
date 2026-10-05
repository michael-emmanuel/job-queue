package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/michael-emmanuel/jobqueue/internal/broker"
)

// Role grants a set of endpoints.
type Role string

// Roles. Admin implies every other role.
const (
	RoleProducer Role = "producer" // submit, inspect and cancel the tenant's own jobs
	RoleWorker   Role = "worker"   // pull, ack, fail and renew; reads any job it needs
	RoleAdmin    Role = "admin"    // queue management, DLQ operations, everything else
)

// Principal is an authenticated caller. The tenant comes from the API key,
// never from the request body: a client cannot claim to be another tenant.
type Principal struct {
	Tenant string
	KeyID  string // short, non-secret identifier for audit logs
	roles  map[Role]bool
}

// Has reports whether the principal holds any of roles (admin holds all).
func (p *Principal) Has(roles ...Role) bool {
	if p.roles[RoleAdmin] {
		return true
	}
	for _, r := range roles {
		if p.roles[r] {
			return true
		}
	}
	return false
}

type keyEntry struct {
	hash [sha256.Size]byte
	p    Principal
}

// Authenticator maps bearer tokens to principals. Tokens are stored only as
// SHA-256 hashes and compared in constant time across every entry, so
// neither memory contents nor response timing reveal a key.
type Authenticator struct {
	entries  []keyEntry
	insecure bool
}

// ErrUnauthenticated is returned for a missing or unknown token.
var ErrUnauthenticated = errors.New("api: unauthenticated")

// ParseKeys parses "key:tenant:role+role,key2:tenant2:role" (entries may also
// be separated by newlines). Error messages never include key material.
func ParseKeys(spec string) (*Authenticator, error) {
	a := &Authenticator{}
	seen := map[[sha256.Size]byte]bool{}
	fields := strings.FieldsFunc(spec, func(r rune) bool { return r == ',' || r == '\n' || r == ';' })
	for i, raw := range fields {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		parts := strings.Split(raw, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("api key entry %d: want key:tenant:roles", i+1)
		}
		key, tenant := parts[0], parts[1]
		if len(key) < 16 {
			return nil, fmt.Errorf("api key entry %d: key must be at least 16 characters", i+1)
		}
		if !broker.ValidName(tenant) {
			return nil, fmt.Errorf("api key entry %d: invalid tenant name", i+1)
		}
		roles := map[Role]bool{}
		for _, r := range strings.Split(parts[2], "+") {
			switch role := Role(r); role {
			case RoleProducer, RoleWorker, RoleAdmin:
				roles[role] = true
			default:
				return nil, fmt.Errorf("api key entry %d: unknown role %q", i+1, r)
			}
		}
		h := sha256.Sum256([]byte(key))
		if seen[h] {
			return nil, fmt.Errorf("api key entry %d: duplicate key", i+1)
		}
		seen[h] = true
		a.entries = append(a.entries, keyEntry{hash: h, p: Principal{
			Tenant: tenant, KeyID: hex.EncodeToString(h[:4]), roles: roles,
		}})
	}
	if len(a.entries) == 0 {
		return nil, errors.New("no API keys configured")
	}
	return a, nil
}

// InsecureAuthenticator disables authentication and grants every role to the
// "default" tenant. For local development only; the binary refuses to use it
// unless explicitly asked.
func InsecureAuthenticator() *Authenticator { return &Authenticator{insecure: true} }

// Authenticate resolves the request's bearer token.
func (a *Authenticator) Authenticate(r *http.Request) (*Principal, error) {
	if a.insecure {
		return &Principal{Tenant: "default", KeyID: "insecure",
			roles: map[Role]bool{RoleAdmin: true}}, nil
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return nil, ErrUnauthenticated
	}
	h := sha256.Sum256([]byte(token))
	var found *Principal
	for i := range a.entries { // no early exit: constant work per request
		if subtle.ConstantTimeCompare(h[:], a.entries[i].hash[:]) == 1 {
			found = &a.entries[i].p
		}
	}
	if found == nil {
		return nil, ErrUnauthenticated
	}
	p := *found
	return &p, nil
}
