package clientauth

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/scrypt"

	"openpanel/ingest/internal/kv"
)

const (
	verifyCacheTTL       = 5 * time.Minute
	verifyLocalCacheTTL  = 60 * time.Second
	verifyLocalCacheSize = 1000
	// Node's crypto.scrypt defaults, which hashClientSecret uses.
	scryptN      = 16384
	scryptR      = 8
	scryptP      = 1
	scryptKeyLen = 32
)

// Verifier checks a supplied secret against the stored hash. Successes are
// cached here and under the API's Redis key, which holds base64 of the
// plaintext secret, so a server SDK's every event costs no hash.
type Verifier struct {
	Redis kv.Store
	local *expirable.LRU[string, bool]
}

// NewVerifier builds a Verifier.
func NewVerifier(rdb kv.Store) *Verifier {
	return &Verifier{Redis: rdb, local: expirable.NewLRU[string, bool](verifyLocalCacheSize, nil, verifyLocalCacheTTL)}
}

func verifyCacheKey(clientID, secret string) string {
	return "client:auth:" + clientID + ":" + base64.StdEncoding.EncodeToString([]byte(secret))
}

// Verify reports whether secret matches stored. Only successes are cached:
// the key embeds the caller's string, so caching failures would let anyone
// fill the cache.
func (v *Verifier) Verify(ctx context.Context, clientID, secret string, stored *string) bool {
	if stored == nil || *stored == "" || secret == "" {
		return false
	}
	key := verifyCacheKey(clientID, secret)
	if v.local.Contains(key) {
		return true
	}
	if value, _, err := v.Redis.Get(ctx, key); err == nil && value == "true" {
		v.local.Add(key, true)
		return true
	}
	if !VerifyHash(secret, *stored) {
		return false
	}
	v.local.Add(key, true)
	// A failed write only costs the next request a hash compare.
	go func() { _ = v.Redis.SetEx(context.WithoutCancel(ctx), key, "true", verifyCacheTTL) }()
	return true
}

// VerifyHash is verifyClientSecret: scrypt "salt.hash", or an argon2 PHC
// string. A malformed hash is a failed verification.
func VerifyHash(secret, stored string) bool {
	if strings.HasPrefix(stored, "$argon2") {
		return verifyArgon2(secret, stored)
	}
	parts := strings.Split(stored, ".")
	if len(parts) < 2 {
		return false
	}
	salt, want := parts[0], decodeHexPrefix(parts[1])
	got, err := scrypt.Key([]byte(secret), []byte(salt), scryptN, scryptR, scryptP, scryptKeyLen)
	if err != nil || len(want) != len(got) {
		return false
	}
	return subtle.ConstantTimeCompare(got, want) == 1
}

// decodeHexPrefix is Buffer.from(s, 'hex'): it stops at the first pair that
// is not hex.
func decodeHexPrefix(s string) []byte {
	out := make([]byte, 0, len(s)/2)
	for i := 0; i+1 < len(s); i += 2 {
		b, err := hex.DecodeString(s[i : i+2])
		if err != nil {
			break
		}
		out = append(out, b[0])
	}
	return out
}

// verifyArgon2 checks $argon2id$v=19$m=..,t=..,p=..$salt$hash (and argon2i).
func verifyArgon2(secret, encoded string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 {
		return false
	}
	variant := parts[1]
	var memory, iterations uint64
	var parallelism uint64
	for _, param := range strings.Split(parts[3], ",") {
		name, value, _ := strings.Cut(param, "=")
		n, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return false
		}
		switch name {
		case "m":
			memory = n
		case "t":
			iterations = n
		case "p":
			parallelism = n
		}
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil || memory == 0 || iterations == 0 || parallelism == 0 || parallelism > 255 {
		return false
	}
	var got []byte
	switch variant {
	case "argon2id":
		got = argon2.IDKey([]byte(secret), salt, uint32(iterations), uint32(memory), uint8(parallelism), uint32(len(want)))
	case "argon2i":
		got = argon2.Key([]byte(secret), salt, uint32(iterations), uint32(memory), uint8(parallelism), uint32(len(want)))
	default:
		// argon2d has no Go implementation; such a row cannot verify here.
		return false
	}
	return subtle.ConstantTimeCompare(got, want) == 1
}
