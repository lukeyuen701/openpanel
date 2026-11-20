package track

import (
	"math"
	"regexp"
	"strings"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/jsregex"
)

const (
	maxEventNameLength = 80
	maxGroupsPerEvent  = 100
	maxGroupIDLength   = 256
)

var reservedEventNames = []string{"session_start", "session_end"}

// blockedSubstrings mark attack and scanner payloads posted as event names.
var blockedSubstrings = []string{
	"oastify.com", "burpcollaborator", "interact.sh", "oast.me",
	"pg_sleep", "waitfor delay", "xp_dirtree", "load_file(", "extractvalue(", "dbms_pipe.receive_message", "union select",
	"nslookup ", "/bin/sleep", "/bin/bash", "cmd.exe", "wget+http", "wget http", "chmod+777", "chmod 777",
	"processbuilder", "runtime.getruntime", "java.lang.processbuilder", "eval-stdin.php",
	"../", "..\\", "%2e%2e", "%u002e%u002e", "/etc/passwd", "/etc/shadow", "win.ini", "system.ini",
	"${", "%{",
	"<!doctype", "<!entity", "<xi:include", "xsi:schemalocation",
	"\r\n", "bcc:",
	"phpinfo.php", "wp-config.php", ".git/config", ".env.backup", ".env.bak", "/vendor/phpunit/",
	"mozi.m", "/setup.cgi?", "/cgi-bin/",
	"#<article:0x", "#<video:0x", "#<brand:0x",
	"exec master.dbo", "declare @", "' and '", "' or '", "') or ", "')and ",
}

// pathScanPatterns mark an "event" that is really a scanned URL path.
var pathScanPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^/[a-z_-]+\.(php|env|yml|yaml|json|xml|config|ini|bak|sql|log)`),
	regexp.MustCompile(`(?i)^/\.[a-z]`),
	regexp.MustCompile(`(?i)^/(wp-|wordpress)`),
	regexp.MustCompile(`(?i)^/phpmyadmin`),
	regexp.MustCompile(`(?i)^/.+\.php$`),
}

// IsBlockedEventName is isBlockedEventName.
func IsBlockedEventName(name string) bool {
	if jsregex.LenUTF16(name) > maxEventNameLength {
		return true
	}
	if strings.ContainsAny(name, "\n\r") {
		return true
	}
	lower := strings.ToLower(name)
	for _, blocked := range blockedSubstrings {
		if strings.Contains(lower, blocked) {
			return true
		}
	}
	for _, pattern := range pathScanPatterns {
		if pattern.MatchString(name) {
			return true
		}
	}
	return false
}

// ParseTrackBody is zTrackHandlerPayload.safeParse for a body of type
// "track", returning zod's output: the payload with unknown keys stripped and
// the remaining keys in schema order. ok is false for anything zod would
// refuse and for the few inputs this port does not model; both go to the Bun
// API, which answers with zod's own message.
func ParseTrackBody(body *jsjson.Value) (*jsjson.Value, bool) {
	if !body.IsObject() {
		return nil, false
	}
	kind := body.Get("type")
	if kind == nil || kind.Kind != jsjson.String || kind.Str != "track" {
		return nil, false
	}
	payload := body.Get("payload")
	if !payload.IsObject() {
		return nil, false
	}
	out := jsjson.NewObj()

	name := payload.Get("name")
	if name == nil || name.Kind != jsjson.String || name.Str == "" {
		return nil, false
	}
	out.Set("name", name)

	if properties := payload.Get("properties"); properties != nil {
		if !properties.IsObject() || properties.Obj.Has("__proto__") {
			return nil, false
		}
		out.Set("properties", properties)
	}

	if profileID := payload.Get("profileId"); profileID != nil {
		switch profileID.Kind {
		case jsjson.String:
			if profileID.Str == "" {
				return nil, false
			}
		case jsjson.Number:
		default:
			return nil, false
		}
		out.Set("profileId", profileID)
	}

	if groups := payload.Get("groups"); groups != nil {
		if groups.Kind != jsjson.Null && !validGroups(groups) {
			return nil, false
		}
		out.Set("groups", groups)
	}

	if !refinementsPass(name.Str, out.Get("properties")) {
		return nil, false
	}

	parsed := jsjson.NewObj()
	parsed.Set("type", kind)
	parsed.Set("payload", jsjson.NewObject(out))
	return jsjson.NewObject(parsed), true
}

func validGroups(groups *jsjson.Value) bool {
	if groups.Kind != jsjson.Array || len(groups.Arr) > maxGroupsPerEvent {
		return false
	}
	for _, group := range groups.Arr {
		if group.Kind != jsjson.String || group.Str == "" || jsregex.LenUTF16(group.Str) > maxGroupIDLength {
			return false
		}
	}
	return true
}

func refinementsPass(name string, properties *jsjson.Value) bool {
	for _, reserved := range reservedEventNames {
		if name == reserved {
			return false
		}
	}
	if IsBlockedEventName(name) {
		return false
	}
	if name != "revenue" || properties == nil {
		return true
	}
	revenue := properties.Get("__revenue")
	if revenue == nil || revenue.Kind == jsjson.Null {
		return true
	}
	// Number.isInteger and not negative.
	return revenue.Kind == jsjson.Number && !math.IsInf(revenue.Num, 0) && revenue.Num == math.Trunc(revenue.Num) && revenue.Num >= 0
}
