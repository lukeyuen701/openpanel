package clientauth

import (
	"net"
	"net/url"
	"strings"

	"golang.org/x/net/idna"
)

const wildcard = "*"

// cleanDomain is origin-allowed.ts's cleanDomain: each replacement hits the
// first occurrence only, wherever it is.
func cleanDomain(domain string) string {
	domain = strings.Replace(domain, "www.", "", 1)
	// /https?:\/\// without g: the leftmost of the two, once.
	httpAt, httpsAt := strings.Index(domain, "http://"), strings.Index(domain, "https://")
	switch {
	case httpsAt >= 0 && (httpAt < 0 || httpsAt < httpAt):
		domain = domain[:httpsAt] + domain[httpsAt+len("https://"):]
	case httpAt >= 0:
		domain = domain[:httpAt] + domain[httpAt+len("http://"):]
	}
	return strings.TrimSuffix(domain, "/")
}

func matchesWildcard(pattern, host string) bool {
	parts := strings.Split(pattern, wildcard)
	head, tail := parts[0], parts[len(parts)-1]
	if !strings.HasPrefix(host, head) {
		return false
	}
	position := len(head)
	for _, part := range parts[1 : len(parts)-1] {
		index := strings.Index(host[position:], part)
		if index < 0 {
			return false
		}
		position += index + len(part)
	}
	return len(host)-len(tail) >= position && strings.HasSuffix(host, tail)
}

var defaultPorts = map[string]string{"http": "80", "https": "443", "ws": "80", "wss": "443", "ftp": "21"}

// OriginHost is URL.parse(origin)?.host. modelled is false for an origin this
// port does not parse the WHATWG way (a non-special scheme, IPv6, userinfo);
// the caller hands those requests to the Bun API. A value with no ":" is no
// URL at all, so it has no host.
func OriginHost(origin string) (host string, modelled bool) {
	if !strings.Contains(origin, ":") {
		return "", true
	}
	return originHost(origin)
}

func originHost(origin string) (host string, ok bool) {
	u, err := url.Parse(origin)
	if err != nil || u.Opaque != "" || u.User != nil {
		return "", false
	}
	scheme := strings.ToLower(u.Scheme)
	defaultPort, special := defaultPorts[scheme]
	if !special || u.Host == "" || !strings.HasPrefix(strings.ToLower(origin), scheme+"://") {
		return "", false
	}
	hostname, port := u.Hostname(), u.Port()
	if strings.Contains(hostname, ":") || strings.ContainsAny(hostname, "%[]") {
		return "", false
	}
	ascii, err := idna.Lookup.ToASCII(hostname)
	if err != nil || ascii == "" {
		return "", false
	}
	ascii = strings.ToLower(ascii)
	if port == "" || port == defaultPort {
		return ascii, true
	}
	return net.JoinHostPort(ascii, port), true
}

// IsOriginAllowed is isOriginAllowed. host is OriginHost's answer, "" when the
// origin has none.
func IsOriginAllowed(cors []string, origin string, hasOrigin bool, host string) bool {
	cleanedOrigin := cleanDomain(origin)
	cleanedHost := ""
	if host != "" {
		cleanedHost = cleanDomain(host)
	}
	for _, domain := range cors {
		cleaned := cleanDomain(domain)
		if !strings.Contains(cleaned, wildcard) {
			if cleaned == cleanedOrigin {
				return true
			}
			continue
		}
		if cleanedHost == "" {
			continue
		}
		compared := cleanedHost
		if !strings.Contains(cleaned, ":") {
			compared = stripPort(compared)
		}
		if matchesWildcard(cleaned, compared) {
			return true
		}
	}
	if !hasOrigin || origin == "" {
		return false
	}
	for _, domain := range cors {
		if domain == wildcard {
			return true
		}
	}
	return false
}

// stripPort removes a trailing :digits.
func stripPort(host string) string {
	index := strings.LastIndexByte(host, ':')
	if index < 0 || index == len(host)-1 {
		return host
	}
	for _, c := range host[index+1:] {
		if c < '0' || c > '9' {
			return host
		}
	}
	return host[:index]
}
