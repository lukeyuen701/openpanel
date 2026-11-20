// Package useragent is @openpanel/shared's parseUserAgent: ua-parser-js 2.x
// (ported in mapper.go, its tables generated from the installed package) plus
// OpenPanel's own corrections for iOS, app-style UAs, brands and device type.
package useragent

import (
	"regexp"
	"strconv"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"

	"openpanel/ingest/internal/jsjson"
	"openpanel/ingest/internal/jsregex"
)

// uaMaxLength is ua-parser-js's UA_MAX_LENGTH, in UTF-16 code units.
const uaMaxLength = 500

const parseCacheSize = 10_000

// Info is what parseUserAgent returns. A nil field is undefined in the
// JavaScript object and so absent from the Kafka payload.
type Info struct {
	IsServer       bool
	OS             *string
	OSVersion      *string
	Browser        *string
	BrowserVersion *string
	Device         *string
	Brand          *string
	Model          *string
}

// JSON writes Info with the key order the JavaScript object literal has.
func (i Info) JSON() *jsjson.Value {
	o := jsjson.NewObj()
	if i.IsServer {
		o.Set("isServer", jsjson.NewBool(true))
		o.Set("device", jsjson.NewString("server"))
		for _, key := range []string{"os", "osVersion", "browser", "browserVersion", "brand", "model"} {
			o.Set(key, jsjson.NewString(""))
		}
		return jsjson.NewObject(o)
	}
	set := func(key string, value *string) {
		if value != nil {
			o.Set(key, jsjson.NewString(*value))
		}
	}
	set("os", i.OS)
	set("osVersion", i.OSVersion)
	set("browser", i.Browser)
	set("browserVersion", i.BrowserVersion)
	set("device", i.Device)
	set("brand", i.Brand)
	set("model", i.Model)
	o.Set("isServer", jsjson.NewBool(false))
	return jsjson.NewObject(o)
}

var serverInfo = Info{IsServer: true}

// result is the slice of UAParser#getResult() the OpenPanel layer reads.
type result struct {
	ua      string
	os      fields
	device  fields
	browser fields
}

// Parser holds the compiled tables and a parse cache.
type Parser struct {
	tables *tables
	cache  *lru.Cache[string, *result]
	// devices memoizes getDevice, which runs some twenty regexes and is a
	// pure function of the user agent and the model.
	devices *lru.Cache[string, string]
}

var (
	defaultParser     *Parser
	defaultParserOnce sync.Once
	defaultParserErr  error
)

// Default returns the process-wide parser, compiling the tables once.
func Default() (*Parser, error) {
	defaultParserOnce.Do(func() {
		defaultParser, defaultParserErr = New()
	})
	return defaultParser, defaultParserErr
}

// New compiles the tables.
func New() (*Parser, error) {
	t, err := loadTables()
	if err != nil {
		return nil, err
	}
	cache, err := lru.New[string, *result](parseCacheSize)
	if err != nil {
		return nil, err
	}
	devices, err := lru.New[string, string](parseCacheSize)
	if err != nil {
		return nil, err
	}
	return &Parser{tables: t, cache: cache, devices: devices}, nil
}

// TablesVersion is the ua-parser-js version the tables came from.
func (p *Parser) TablesVersion() string { return p.tables.version }

var (
	iphoneModelRegex     = jsregex.MustCompile(`(iPhone|iPad)\s*([0-9,]+)`, "i")
	iosModelRegex        = jsregex.MustCompile(`(iOS)\s*([0-9.]+)`, "i")
	ipadOSVersionRegex   = jsregex.MustCompile(`iPadOS\s*([0-9_]+)`, "i")
	singleNameVersion    = jsregex.MustCompile(`^[^/]+\/[\d.]+$`, "")
	appModelRegex        = jsregex.MustCompile(`Model=([^;)]+)`, "i")
	appManufacturerRegex = jsregex.MustCompile(`Manufacturer=([^;)]+)`, "i")
	iosFrozenVersion     = jsregex.MustCompile(`^1[89][^\d]`, "")
	safariRealVersion    = jsregex.MustCompile(`\) Version\/((\d+)[\d\.]*)`, "")
	underscore           = regexp.MustCompile("_")
)

// parse is the cached half of parseUserAgent: ua-parser-js plus the fixes
// that do not depend on the event's properties.
func (p *Parser) parse(ua string) *result {
	if cached, ok := p.cache.Get(ua); ok {
		return cached
	}
	res := p.uaParser(ua)

	hasModelOrOS := truthy(res.device.get("model")) || truthy(res.os.get("name"))
	if !hasModelOrOS {
		model := iphoneModelRegex.Exec(ua)
		os := iosModelRegex.Exec(ua)
		if model != nil && os != nil {
			res.device["model"] = model[1]
			res.device["vendor"] = str("Apple")
			res.os["name"] = os[1]
			res.os["version"] = os[2]
			p.cache.Add(ua, res)
			return res
		}
	}

	if model := res.device.get("model"); model != nil && *model == "iPad" && !truthy(res.os.get("version")) {
		if version := ipadOSVersionRegex.Exec(ua); version != nil {
			res.os["version"] = str(underscore.ReplaceAllString(*version[1], "."))
		}
	}

	appModel, appManufacturer := extractAppStyleInfo(ua)
	if truthy(appModel) || truthy(appManufacturer) {
		model := orString(res.device.get("model"), appModel)
		vendor := orString(res.device.get("vendor"), appManufacturer)
		if !truthy(vendor) {
			vendor = detectBrand(ua, model)
		}
		res.device["model"] = model
		res.device["vendor"] = vendor
	}

	if !truthy(res.device.get("vendor")) && (truthy(res.device.get("model")) || truthy(res.os.get("name"))) {
		if brand := detectBrand(ua, res.device.get("model")); brand != nil {
			res.device["vendor"] = brand
		}
	}

	p.cache.Add(ua, res)
	return res
}

// orString is JavaScript's a || b over possibly-undefined strings.
func orString(a, b *string) *string {
	if truthy(a) {
		return a
	}
	return b
}

// uaParser is new UAParser(ua).getResult(), reduced to the three items read.
func (p *Parser) uaParser(raw string) *result {
	ua := jsregex.SliceUTF16(trimLeft(raw), uaMaxLength)
	res := &result{ua: ua, os: fields{}, device: fields{}, browser: fields{}}
	mapRegexes(ua, p.tables.browser, res.browser)
	mapRegexes(ua, p.tables.device, res.device)
	mapRegexes(ua, p.tables.os, res.os)

	// Since iOS 26 Safari freezes the reported OS version at 18; the Safari
	// version is the real one.
	name := res.os.get("name")
	version := res.os.get("version")
	if name != nil && *name == "iOS" && truthy(version) && iosFrozenVersion.Test(*version) {
		if real := safariRealVersion.Exec(ua); real != nil {
			if major, err := strconv.Atoi(*real[2]); err == nil && major >= 26 {
				res.os["version"] = real[1]
			}
		}
	}
	return res
}

func extractAppStyleInfo(ua string) (model, manufacturer *string) {
	if m := appModelRegex.Exec(ua); m != nil && m[1] != nil {
		model = str(trimJS(*m[1]))
	}
	if m := appManufacturerRegex.Exec(ua); m != nil && m[1] != nil {
		manufacturer = str(trimJS(*m[1]))
	}
	return model, manufacturer
}

// Overrides are the event properties parseUserAgent reads (__os, __model…).
type Overrides interface {
	// OverrideString returns the property when it is a non-empty string.
	OverrideString(key string) (string, bool)
}

// PropertyOverrides reads overrides from an event's properties object.
type PropertyOverrides struct{ Properties *jsjson.Obj }

// OverrideString implements Overrides.
func (p PropertyOverrides) OverrideString(key string) (string, bool) {
	if p.Properties == nil {
		return "", false
	}
	value := p.Properties.Get(key)
	if value == nil || value.Kind != jsjson.String || value.Str == "" {
		return "", false
	}
	return value.Str, true
}

var deviceOverrideKeys = []string{"__os", "__device", "__brand", "__model"}

func hasDeviceOverrides(overrides Overrides) bool {
	if overrides == nil {
		return false
	}
	for _, key := range deviceOverrideKeys {
		if _, ok := overrides.OverrideString(key); ok {
			return true
		}
	}
	return false
}

func isServer(res *result) bool {
	if singleNameVersion.Test(res.ua) {
		return true
	}
	return res.os.get("name") == nil && res.browser.get("name") == nil &&
		res.device.get("vendor") == nil && res.device.get("model") == nil
}

// Parse is parseUserAgent(ua, overrides). An empty ua is a server.
func (p *Parser) Parse(ua string, overrides Overrides) Info {
	if ua == "" {
		return serverInfo
	}
	res := p.parse(ua)
	if isServer(res) && !hasDeviceOverrides(overrides) {
		return serverInfo
	}
	pick := func(key string, fallback *string) *string {
		if overrides != nil {
			if value, ok := overrides.OverrideString(key); ok {
				return str(value)
			}
		}
		return fallback
	}
	model := pick("__model", res.device.get("model"))
	device := res.device.get("type")
	if !truthy(device) {
		device = str(p.deviceType(ua, model))
	}
	return Info{
		OS:             pick("__os", res.os.get("name")),
		OSVersion:      pick("__osVersion", res.os.get("version")),
		Browser:        pick("__browser", res.browser.get("name")),
		BrowserVersion: pick("__browserVersion", res.browser.get("version")),
		Device:         pick("__device", device),
		Brand:          pick("__brand", res.device.get("vendor")),
		Model:          model,
	}
}

func (p *Parser) deviceType(ua string, model *string) string {
	key := ua + "\x00"
	if model != nil {
		key += *model
	}
	if cached, ok := p.devices.Get(key); ok {
		return cached
	}
	device := getDevice(ua, model)
	p.devices.Add(key, device)
	return device
}
