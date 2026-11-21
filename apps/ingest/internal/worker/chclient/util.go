package chclient

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
)

func basicAuth(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}

// newQueryID is a random UUID (version 4), as crypto.randomUUID().
func newQueryID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:])
}

var stringParamEscaper = strings.NewReplacer("\\", "\\\\", "\t", "\\t", "\n", "\\n", "\r", "\\r", "'", "\\'")

// FormatString is formatQueryParams for a String parameter.
func FormatString(value string) string { return stringParamEscaper.Replace(value) }

// FormatStringArray is formatQueryParams for an Array(String) parameter:
// ['a','b'] with each element quoted and escaped.
func FormatStringArray(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = "'" + FormatString(value) + "'"
	}
	return "[" + strings.Join(quoted, ",") + "]"
}
