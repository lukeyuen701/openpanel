package chclient

import (
	"strings"

	"openpanel/ingest/internal/worker/config"
)

// Setting is one ClickHouse setting, value already in its wire form.
type Setting = config.Setting

// Settings keep the order they were added in: the client puts them on the URL
// in that order, as the Node client does. Setting a key that is already
// present replaces its value in place.
type Settings []Setting

// With returns s with overlay applied: new keys are appended, present keys
// take the overlay's value where they already are.
func (s Settings) With(overlay ...Setting) Settings {
	out := append(Settings(nil), s...)
	for _, setting := range overlay {
		replaced := false
		for i := range out {
			if out[i].Key == setting.Key {
				out[i].Value = setting.Value
				replaced = true
				break
			}
		}
		if !replaced {
			out = append(out, setting)
		}
	}
	return out
}

// AsyncInsertSettings is the per-buffer layer of BaseBuffer.getClickhouseSettings
// when BUFFER_ASYNC_INSERTS is set (any non-blank value, "false" included):
// the insert returns once the server has queued the rows, not when they are in
// a part. Without the flag a buffer sends no per-call settings.
func AsyncInsertSettings(asyncInserts bool) Settings {
	if !asyncInserts {
		return nil
	}
	return Settings{
		{Key: "async_insert", Value: "1"},
		{Key: "wait_for_async_insert", Value: "0"},
		{Key: "parallel_view_processing", Value: "1"},
	}
}

// insertDefaults is INSERT_DEFAULT_SETTINGS of packages/db/src/clickhouse/client.ts;
// they apply to inserts only, not to Command or Query.
var insertDefaults = Settings{
	{Key: "max_execution_time", Value: "300"},
	{Key: "max_insert_block_size", Value: "500000"},
	{Key: "max_http_get_redirects", Value: "0"},
	{Key: "input_format_parallel_parsing", Value: "1"},
	{Key: "send_progress_in_http_headers", Value: "1"},
	{Key: "http_headers_progress_interval_ms", Value: "50000"},
	{Key: "wait_end_of_query", Value: "1"},
}

// clientDefaults is getClickhouseSettings() of client.ts: ClickHouse 26.3
// defaults async_insert to 1, so the client says 0 to keep inserts synchronous.
func clientDefaults(cfg config.ClickHouseConfig) Settings {
	settings := Settings{
		{Key: "distributed_product_mode", Value: "allow"},
		{Key: "date_time_input_format", Value: "best_effort"},
		{Key: "async_insert", Value: "0"},
	}
	if !cfg.RemoveConvertAnyJoin {
		settings = append(settings, Setting{Key: "query_plan_convert_any_join_to_semi_or_anti_join", Value: "0"})
	}
	return settings.With(cfg.Settings...)
}

// encodeQuery is URLSearchParams.toString(): form encoding, spaces as "+",
// and only A-Za-z0-9 and *-._ left bare.
func encodeQuery(pairs [][2]string) string {
	var out strings.Builder
	for i, pair := range pairs {
		if i > 0 {
			out.WriteByte('&')
		}
		writeFormEscaped(&out, pair[0])
		out.WriteByte('=')
		writeFormEscaped(&out, pair[1])
	}
	return out.String()
}

const upperHex = "0123456789ABCDEF"

func writeFormEscaped(out *strings.Builder, text string) {
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '*', c == '-', c == '.', c == '_':
			out.WriteByte(c)
		case c == ' ':
			out.WriteByte('+')
		default:
			out.WriteByte('%')
			out.WriteByte(upperHex[c>>4])
			out.WriteByte(upperHex[c&0x0f])
		}
	}
}
