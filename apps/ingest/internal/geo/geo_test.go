package geo

import (
	"encoding/json"
	"os"
	"testing"

	"openpanel/ingest/internal/jsjson"
)

// TestMatchesTypeScript needs the databases; INGEST_GOLDEN_GEO names the
// golden from parity/golden-geo.ts and INGEST_GEO_DIR the .mmdb directory.
func TestMatchesTypeScript(t *testing.T) {
	golden, dir := os.Getenv("INGEST_GOLDEN_GEO"), os.Getenv("INGEST_GEO_DIR")
	if golden == "" || dir == "" {
		t.Skip("INGEST_GOLDEN_GEO / INGEST_GEO_DIR not set")
	}
	data, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		IP  string
		Geo string
		ASN struct {
			Asn       *uint32
			Org       *string
			IsHosting bool
		}
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	r, problems := Open(dir+"/GeoLite2-City.mmdb", dir+"/GeoLite2-ASN.mmdb")
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	defer r.Close()
	failures := 0
	for _, c := range cases {
		got := string(jsjson.Stringify(r.Location(c.IP).JSON()))
		asn := r.ASN(c.IP)
		sameASN := asn.IsHosting == c.ASN.IsHosting && eqU32(asn.Number, c.ASN.Asn) && eqStr(asn.Organization, c.ASN.Org)
		if got != c.Geo || !sameASN {
			failures++
			if failures < 20 {
				t.Errorf("%q: geo %s want %s; asn %+v want %+v", c.IP, got, c.Geo, asn, c.ASN)
			}
		}
	}
	if failures > 0 {
		t.Errorf("%d of %d differ", failures, len(cases))
	}
}

func eqU32(a, b *uint32) bool { return (a == nil && b == nil) || (a != nil && b != nil && *a == *b) }
func eqStr(a, b *string) bool { return (a == nil && b == nil) || (a != nil && b != nil && *a == *b) }
