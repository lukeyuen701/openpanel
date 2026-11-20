// Package geo is the MaxMind lookup the API runs: GeoLite2-City for the
// event's location and GeoLite2-ASN for the datacenter signal.
package geo

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/netip"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/oschwald/maxminddb-golang/v2"

	"openpanel/ingest/internal/jsjson"
)

//go:embed data/datacenter-asns.json
var datacenterASNsJSON []byte

const (
	cacheSize = 10_000
	cacheTTL  = 5 * time.Minute
)

// ignoredIPs never resolve, as in the API.
var ignoredIPs = map[string]bool{"127.0.0.1": true, "::1": true}

// Location is GeoLocation; a nil field is undefined.
type Location struct {
	City      *string
	Country   *string
	Region    *string
	Longitude *float64
	Latitude  *float64
}

// JSON writes the location in the API's key order.
func (l Location) JSON() *jsjson.Value {
	o := jsjson.NewObj()
	if l.City != nil {
		o.Set("city", jsjson.NewString(*l.City))
	}
	if l.Country != nil {
		o.Set("country", jsjson.NewString(*l.Country))
	}
	if l.Region != nil {
		o.Set("region", jsjson.NewString(*l.Region))
	}
	if l.Longitude != nil {
		o.Set("longitude", jsjson.NewNumber(*l.Longitude))
	}
	if l.Latitude != nil {
		o.Set("latitude", jsjson.NewNumber(*l.Latitude))
	}
	return jsjson.NewObject(o)
}

// ASN is AsnInfo.
type ASN struct {
	Number       *uint32
	Organization *string
	IsHosting    bool
}

type cityRecord struct {
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
	Country struct {
		ISOCode *string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
	Subdivisions []struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"subdivisions"`
	Location struct {
		Longitude *float64 `maxminddb:"longitude"`
		Latitude  *float64 `maxminddb:"latitude"`
	} `maxminddb:"location"`
}

type asnRecord struct {
	Number       *uint32 `maxminddb:"autonomous_system_number"`
	Organization *string `maxminddb:"autonomous_system_organization"`
}

// Resolver answers both lookups. A database that failed to open answers the
// default value for every address, as the API does.
type Resolver struct {
	city        *maxminddb.Reader
	asn         *maxminddb.Reader
	datacenters map[uint32]bool
	cityCache   *expirable.LRU[string, Location]
	asnCache    *expirable.LRU[string, ASN]
}

// Open loads both databases; a missing file is reported and that lookup
// degrades to defaults.
func Open(cityPath, asnPath string) (*Resolver, []error) {
	var problems []error
	r := &Resolver{
		cityCache: expirable.NewLRU[string, Location](cacheSize, nil, cacheTTL),
		asnCache:  expirable.NewLRU[string, ASN](cacheSize, nil, cacheTTL),
	}
	var asns []uint32
	if err := json.Unmarshal(datacenterASNsJSON, &asns); err != nil {
		problems = append(problems, fmt.Errorf("geo: decode datacenter ASNs: %w", err))
	}
	r.datacenters = make(map[uint32]bool, len(asns))
	for _, asn := range asns {
		r.datacenters[asn] = true
	}
	if reader, err := maxminddb.Open(cityPath); err == nil {
		r.city = reader
	} else {
		problems = append(problems, fmt.Errorf("geo: open %s: %w", cityPath, err))
	}
	if reader, err := maxminddb.Open(asnPath); err == nil {
		r.asn = reader
	} else {
		problems = append(problems, fmt.Errorf("geo: open %s: %w", asnPath, err))
	}
	return r, problems
}

// Close releases the databases.
func (r *Resolver) Close() {
	if r.city != nil {
		r.city.Close()
	}
	if r.asn != nil {
		r.asn.Close()
	}
}

func parseIP(ip string) (netip.Addr, bool) {
	if ip == "" || ignoredIPs[ip] {
		return netip.Addr{}, false
	}
	addr, err := netip.ParseAddr(ip)
	if err != nil || addr.Zone() != "" {
		return netip.Addr{}, false
	}
	return addr, true
}

// Location is getGeoLocation.
func (r *Resolver) Location(ip string) Location {
	addr, ok := parseIP(ip)
	if !ok || r.city == nil {
		return Location{}
	}
	if cached, ok := r.cityCache.Get(ip); ok {
		return cached
	}
	var record cityRecord
	result := r.city.Lookup(addr)
	if !result.Found() || result.Decode(&record) != nil {
		r.cityCache.Add(ip, Location{})
		return Location{}
	}
	loc := Location{
		Country:   record.Country.ISOCode,
		Longitude: record.Location.Longitude,
		Latitude:  record.Location.Latitude,
	}
	if name, ok := record.City.Names["en"]; ok {
		loc.City = &name
	}
	if len(record.Subdivisions) > 0 {
		if name, ok := record.Subdivisions[0].Names["en"]; ok {
			loc.Region = &name
		}
	}
	r.cityCache.Add(ip, loc)
	return loc
}

// ASN is getAsnInfo.
func (r *Resolver) ASN(ip string) ASN {
	addr, ok := parseIP(ip)
	if !ok || r.asn == nil {
		return ASN{}
	}
	if cached, ok := r.asnCache.Get(ip); ok {
		return cached
	}
	var record asnRecord
	result := r.asn.Lookup(addr)
	if !result.Found() || result.Decode(&record) != nil {
		r.asnCache.Add(ip, ASN{})
		return ASN{}
	}
	info := ASN{Number: record.Number, Organization: record.Organization}
	info.IsHosting = record.Number != nil && r.datacenters[*record.Number]
	r.asnCache.Add(ip, info)
	return info
}
