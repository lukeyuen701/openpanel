package reqheaders

import (
	"encoding/json"
	"os"
	"testing"
)

func TestClientIPMatchesTypeScript(t *testing.T) {
	data, err := os.ReadFile("testdata/clientip.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Order   []string
		Headers map[string]string
		Out     struct{ IP, Header string }
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		ip, header := ClientIP(FromMap(c.Headers), c.Order)
		if ip != c.Out.IP || header != c.Out.Header {
			t.Errorf("%v %v: got %q %q want %+v", c.Order, c.Headers, ip, header, c.Out)
		}
	}
}
