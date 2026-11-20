package reqheaders

import (
	"regexp"
	"strconv"
	"strings"
)

// DefaultIPHeaderOrder is get-client-ip.ts's DEFAULT_IP_HEADER_ORDER:
// client-forwarded headers before infrastructure ones.
var DefaultIPHeaderOrder = []string{
	"openpanel-client-ip", "true-client-ip", "x-client-ip", "cf-connecting-ip",
	"x-forwarded-for", "x-real-ip", "fastly-client-ip", "x-cluster-client-ip",
	"x-appengine-user-ip", "do-connecting-ip", "x-nf-client-connection-ip",
	"x-forwarded", "forwarded",
}

var (
	ipv4Pattern      = regexp.MustCompile(`^(\d{1,3}\.){3}\d{1,3}$`)
	ipv6Pattern      = regexp.MustCompile(`^([0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}$`)
	forwardedPattern = regexp.MustCompile(`(?i)for=(?:"?\[?([^\]"]+)\]?"?)`)
)

func isPublicIP(ip string) bool {
	if strings.HasPrefix(ip, "::ffff:") {
		return isPublicIP(ip[len("::ffff:"):])
	}
	switch {
	case ip == "::1",
		strings.HasPrefix(ip, "fc00:"), strings.HasPrefix(ip, "fd00:"), strings.HasPrefix(ip, "fe80:"),
		strings.HasPrefix(ip, "127."), strings.HasPrefix(ip, "10."),
		strings.HasPrefix(ip, "192.168."), strings.HasPrefix(ip, "169.254."):
		return false
	}
	if strings.HasPrefix(ip, "172.") {
		parts := strings.Split(ip, ".")
		if len(parts) >= 2 {
			second := parseIntPrefix(parts[1])
			if second >= 16 && second <= 31 {
				return false
			}
		}
	}
	return true
}

// parseIntPrefix is Number.parseInt(s, 10) for the digits it starts with.
func parseIntPrefix(s string) int {
	end := 0
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	n, err := strconv.Atoi(s[:end])
	if err != nil {
		return 0
	}
	return n
}

func isValidIP(ip string) bool {
	return isPublicIP(ip) && (ipv4Pattern.MatchString(ip) || ipv6Pattern.MatchString(ip))
}

// ClientIP is getClientIpFromHeaders: the attribution address and the header
// it came from, or empty strings.
func ClientIP(h Headers, order []string) (ip, header string) {
	if len(order) == 0 {
		order = DefaultIPHeaderOrder
	}
	for _, name := range order {
		value := h.Value(name)
		if value == "" {
			continue
		}
		switch name {
		case "x-forwarded-for":
			first := strings.TrimSpace(strings.Split(value, ",")[0])
			if first != "" && isValidIP(first) {
				return first, name
			}
		case "forwarded":
			if m := forwardedPattern.FindStringSubmatch(value); m != nil && m[1] != "" && isValidIP(m[1]) {
				return m[1], name
			}
		default:
			if isValidIP(value) {
				return value, name
			}
		}
	}
	return "", ""
}
