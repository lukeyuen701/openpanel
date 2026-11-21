package jsport

import (
	"errors"
	"strings"

	"openpanel/ingest/internal/jsjson"

	whatwg "github.com/nationallibraryofnorway/whatwg-url/url"
	"golang.org/x/net/idna"
)

const (
	namedRoutePrefixOrigin = "http"
	relativeBase           = "http://localhost"
	nullOrigin             = "null"
	// jsPrototypeKey is the one query key a plain JavaScript object cannot hold:
	// assigning a string to result["__proto__"] reaches the prototype setter and is ignored.
	jsPrototypeKey = "__proto__"
	aceLabelPrefix = "xn--"
)

// ErrTypeError is the TypeError a JavaScript call raises on a value of the
// wrong type (for example `path.startsWith` on a number); in the worker it is
// a handler failure.
var ErrTypeError = errors.New("jsport: TypeError")

// ParsedPath is what parsePath returns. Query is nil when the URL has no
// parameters (undefined), Hash is "" when there is no fragment (undefined,
// also for a lone "#").
type ParsedPath struct {
	Path   string
	Origin string
	Hash   string
	Query  *jsjson.Obj
}

// ParsePath is parsePath(path) of packages/shared/src/url.ts. A string that
// neither starts with "/" nor with "http" is a named route and is returned as
// is; so is a string the WHATWG URL parser rejects.
func ParsePath(path string) ParsedPath {
	if path == "" {
		return ParsedPath{}
	}
	hasOrigin := strings.HasPrefix(path, namedRoutePrefixOrigin)
	if !hasOrigin && !strings.HasPrefix(path, "/") {
		return ParsedPath{Path: path}
	}
	var (
		parsed *whatwg.Url
		err    error
	)
	if hasOrigin {
		parsed, err = parseURL(path)
	} else {
		parsed, err = parseURLWithBase(path)
	}
	if err != nil {
		return ParsedPath{Path: path}
	}
	result := ParsedPath{
		Query: parseSearchParams(parsed),
		Path:  pathname(parsed, path),
		Hash:  parsed.Hash(),
	}
	if hasOrigin {
		result.Origin = origin(parsed)
	}
	return result
}

// ParsePathValue is ParsePath for an untyped argument: a falsy value gives
// the empty result, any other non-string throws.
func ParsePathValue(v *jsjson.Value) (ParsedPath, error) {
	if !truthy(v) {
		return ParsedPath{}, nil
	}
	if v.Kind != jsjson.String {
		return ParsedPath{}, ErrTypeError
	}
	return ParsePath(v.Str), nil
}

// pathname is url.pathname. For a non-special URL without an authority,
// JavaScriptCore drops the first two characters of a path that starts with
// "/." (it takes them for the marker the standard's serializer adds before a
// path that begins with an empty segment), so "x:/.a" has the pathname "a".
// It also reports "/" for a non-special URL
// whose authority has a userinfo marker ("@", even an empty one) and a query or
// fragment (even an empty one) when the path is empty; the standard says "".
func pathname(parsed *whatwg.Url, raw string) string {
	path := parsed.Pathname()
	if parsed.IsSpecialScheme() || parsed.OpaquePath() {
		return path
	}
	if strings.HasPrefix(path, "/.") && len(path) > len("/.") && !strings.HasPrefix(parsed.Href(false), parsed.Scheme()+"://") {
		return path[len("/."):]
	}
	if path != "" {
		return path
	}
	hasUserinfo, tail := splitAuthority(raw)
	if hasUserinfo && (strings.HasPrefix(tail, "?") || strings.HasPrefix(tail, "#")) {
		return "/"
	}
	return path
}

// splitAuthority reports whether the authority of "scheme://authority..." has
// a userinfo marker ("@") and returns what follows the authority.
func splitAuthority(raw string) (hasUserinfo bool, tail string) {
	cleaned := strings.NewReplacer("\t", "", "\n", "", "\r", "").Replace(strings.TrimFunc(raw, func(r rune) bool { return r <= ' ' }))
	_, rest, found := strings.Cut(cleaned, "://")
	if !found {
		return false, ""
	}
	end := strings.IndexAny(rest, "/?#")
	if end < 0 {
		end = len(rest)
	}
	return strings.Contains(rest[:end], "@"), rest[end:]
}

// parseSearchParams is parseSearchParams(url.searchParams): a repeated key
// keeps its first position and last value.
func parseSearchParams(parsed *whatwg.Url) *jsjson.Obj {
	result := jsjson.NewObj()
	parsed.SearchParams().Iterate(func(pair *whatwg.NameValuePair) {
		if pair.Name == jsPrototypeKey {
			return
		}
		result.Set(pair.Name, jsjson.NewString(pair.Value))
	})
	if result.Len() == 0 {
		return nil
	}
	return result
}

// origin is url.origin: scheme://host[:port] for the special network
// schemes, "null" for everything else.
func origin(parsed *whatwg.Url) string {
	switch parsed.Scheme() {
	case "http", "https", "ws", "wss", "ftp":
		return parsed.Scheme() + "://" + parsed.Host()
	}
	return nullOrigin
}

// hostname is new URL(raw).hostname; ok is false when raw does not parse.
func hostname(raw string) (host string, ok bool) {
	parsed, err := parseURL(raw)
	if err != nil {
		return "", false
	}
	return parsed.Hostname(), true
}

var aceValidator = idna.New(idna.MapForLookup(), idna.BidiRule(), idna.Transitional(false))

var errInvalidACELabel = errors.New("jsport: invalid punycode label")

func parseURL(raw string) (*whatwg.Url, error) {
	parsed, err := whatwg.Parse(toScalarValues(raw))
	if err != nil {
		return nil, err
	}
	return parsed, checkACELabels(parsed)
}

func parseURLWithBase(raw string) (*whatwg.Url, error) {
	parsed, err := whatwg.ParseRef(relativeBase, toScalarValues(raw))
	if err != nil {
		return nil, err
	}
	return parsed, checkACELabels(parsed)
}

// checkACELabels refuses a special-scheme host with a punycode label that
// does not decode to a non-ASCII name. The WHATWG library accepts such a
// label (the current URL standard only warns), JavaScriptCore rejects it.
func checkACELabels(parsed *whatwg.Url) error {
	if !parsed.IsSpecialScheme() {
		return nil
	}
	for _, label := range strings.Split(parsed.Hostname(), ".") {
		if !strings.HasPrefix(label, aceLabelPrefix) {
			continue
		}
		decoded, err := aceValidator.ToUnicode(label)
		if err != nil || decoded == "" || isASCII(decoded) {
			return errInvalidACELabel
		}
	}
	return nil
}

// toScalarValues is the USVString conversion the URL constructor applies:
// an unpaired surrogate becomes U+FFFD.
func toScalarValues(s string) string {
	if IsWellFormed(s) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		r, size := decodeRune(s[i:])
		if isSurrogate(r) {
			r = replacementRune
		}
		b.WriteRune(r)
		i += size
	}
	return b.String()
}

func truthy(v *jsjson.Value) bool {
	if v == nil {
		return false
	}
	switch v.Kind {
	case jsjson.Null:
		return false
	case jsjson.Bool:
		return v.Bool
	case jsjson.Number:
		return v.Num != 0 && v.Num == v.Num
	case jsjson.String:
		return v.Str != ""
	}
	return true
}
