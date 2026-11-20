package track

import (
	"regexp"
	"strconv"
	"strings"

	"openpanel/ingest/internal/geo"
	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/reqheaders"
)

// botCategoryThreshold is how many distinct signal categories flag an event.
const botCategoryThreshold = 2

var (
	chromiumUA    = regexp.MustCompile(`(?:Chrome|Chromium|Edg|OPR)/\d`)
	appleWebKitUA = regexp.MustCompile(`(CriOS|EdgiOS|OPiOS|FxiOS|iPhone|iPad|iPod)`)
)

// headerAnomalies is detectHeaderAnomalies: a browser-shaped user agent that
// is missing what that browser always sends.
func headerAnomalies(h reqheaders.Headers) []string {
	ua := h.Value("user-agent")
	if ua == "" || !strings.Contains(ua, "Mozilla/") {
		return nil
	}
	present := func(name string) bool { return trimJS(h.Value(name)) != "" }
	var reasons []string
	if chromiumUA.MatchString(ua) && !appleWebKitUA.MatchString(ua) && !present("sec-ch-ua") {
		reasons = append(reasons, "header:missing_sec_ch_ua")
	}
	if !present("sec-fetch-mode") && !present("sec-fetch-site") {
		reasons = append(reasons, "header:missing_sec_fetch")
	}
	if !present("accept-language") {
		reasons = append(reasons, "header:missing_accept_language")
	}
	return reasons
}

// StripBotProperties deletes client-supplied verdicts; the server owns them.
func StripBotProperties(properties *jsjson.Obj) {
	if properties == nil {
		return
	}
	properties.Delete("__bot")
	properties.Delete("__bot_reasons")
}

// ApplyBotSuspicion is applyBotSuspicion: it marks, never blocks. It returns
// the properties to store, a new object when there were none and a signal
// fired, or nil when there were none and nothing fired.
func ApplyBotSuspicion(properties *jsjson.Obj, asn geo.ASN, h reqheaders.Headers, clientSecretAuth, isServer bool) *jsjson.Obj {
	StripBotProperties(properties)
	if clientSecretAuth || isServer {
		return properties
	}
	var reasons []string
	if asn.IsHosting {
		number := "unknown"
		if asn.Number != nil {
			number = strconv.FormatUint(uint64(*asn.Number), 10)
		}
		reasons = append(reasons, "datacenter_ip:AS"+number)
	}
	reasons = append(reasons, headerAnomalies(h)...)
	if len(reasons) == 0 {
		return properties
	}
	categories := map[string]bool{}
	for _, reason := range reasons {
		category, _, _ := strings.Cut(reason, ":")
		categories[category] = true
	}
	next := properties
	if next == nil {
		next = jsjson.NewObj()
	}
	next.Set("__bot_reasons", jsjson.NewString(strings.Join(reasons, ",")))
	if len(categories) >= botCategoryThreshold {
		next.Set("__bot", jsjson.NewString("1"))
	}
	return next
}
