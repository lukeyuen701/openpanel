package core

import (
	"math"
	"slices"

	"openpanel/ingest/internal/jsjson"
)

const serverDevice = "server"

// serverOnlyOmitted are the properties a server-side profile update must not
// bring along when the stored profile came from a browser.
var serverOnlyOmitted = []string{
	"city", "country", "region", "longitude", "latitude", "os", "osVersion",
	"browser", "device", "isServer", "os_version", "browser_version",
}

func isNil(value *jsjson.Value) bool { return value == nil || value.Kind == jsjson.Null }

func isPlainObject(value *jsjson.Value) bool { return value != nil && value.Kind == jsjson.Object }

// deepMergeObjects is deepMergeObjects of packages/shared/src/object.ts: keys
// are the target's, then the source's new ones. A nil or null source keeps the
// target, an empty string keeps a non-empty target string, two objects merge
// key by key, and anything else (arrays included) takes the source's value.
func deepMergeObjects(target, source *jsjson.Obj) *jsjson.Obj {
	merged := jsjson.NewObj()
	keys := target.Keys()
	for _, key := range source.Keys() {
		if !target.Has(key) {
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		targetValue, sourceValue := target.Get(key), source.Get(key)
		keepTarget := (isNil(sourceValue) && !isNil(targetValue)) ||
			(sourceValue != nil && sourceValue.Kind == jsjson.String && sourceValue.Str == "" &&
				targetValue != nil && targetValue.Kind == jsjson.String && targetValue.Str != "")
		switch {
		case keepTarget:
			merged.Set(key, targetValue)
		case sourceValue != nil && isPlainObject(targetValue) && isPlainObject(sourceValue):
			merged.Set(key, jsjson.NewObject(deepMergeObjects(targetValue.Obj, sourceValue.Obj)))
		case sourceValue != nil:
			merged.Set(key, sourceValue)
		case targetValue != nil:
			merged.Set(key, targetValue)
		}
	}
	return merged
}

// jsEquals is R.equals, which uniq uses: structural, with NaN equal to itself
// and +0 different from -0.
func jsEquals(a, b *jsjson.Value) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case jsjson.Null:
		return true
	case jsjson.Bool:
		return a.Bool == b.Bool
	case jsjson.Number:
		if math.IsNaN(a.Num) && math.IsNaN(b.Num) {
			return true
		}
		return a.Num == b.Num && math.Signbit(a.Num) == math.Signbit(b.Num)
	case jsjson.String:
		return a.Str == b.Str
	case jsjson.Array:
		return slices.EqualFunc(a.Arr, b.Arr, jsEquals)
	}
	if a.Obj.Len() != b.Obj.Len() {
		return false
	}
	for _, key := range a.Obj.Keys() {
		if !b.Obj.Has(key) || !jsEquals(a.Obj.Get(key), b.Obj.Get(key)) {
			return false
		}
	}
	return true
}

// unionGroups is uniq([...existing.groups ?? [], ...incoming.groups ?? []]).
func unionGroups(existing, incoming *jsjson.Value) *jsjson.Value {
	var unique []*jsjson.Value
	for _, groups := range []*jsjson.Value{existing, incoming} {
		if groups == nil || groups.Kind != jsjson.Array {
			continue
		}
		for _, group := range groups.Arr {
			if !slices.ContainsFunc(unique, func(seen *jsjson.Value) bool { return jsEquals(seen, group) }) {
				unique = append(unique, group)
			}
		}
	}
	return &jsjson.Value{Kind: jsjson.Array, Arr: unique}
}

func hasServerDevice(profile *jsjson.Obj) bool {
	device := jsjson.NewObject(profile).Path("properties", "device")
	return device != nil && device.Kind == jsjson.String && device.Str == serverDevice
}

// shallowCopy is {...source}: the values are shared, which is safe because the
// merge never mutates a value it did not create.
func shallowCopy(source *jsjson.Obj) *jsjson.Obj {
	out := jsjson.NewObj()
	for _, key := range source.Keys() {
		out.Set(key, source.Get(key))
	}
	return out
}

// mergeProfiles is ProfileBuffer.mergeProfiles. With nothing stored the
// incoming profile is returned as is. Otherwise created_at stays the stored
// one, last_seen_at comes from the incoming (so the ReplacingMergeTree version
// advances), groups are the union, and every other field is deepMergeObjects.
// A server-side update does not bring browser-only properties into a stored
// browser profile.
//
// A profile without a properties object is read as having no device; the
// TypeScript throws there and so blocks the whole list.
func mergeProfiles(existing, incoming *jsjson.Obj) *jsjson.Obj {
	if existing == nil {
		return incoming
	}
	profile := incoming
	if !hasServerDevice(existing) && hasServerDevice(incoming) {
		profile = shallowCopy(incoming)
		properties := shallowCopy(incoming.Get("properties").Obj)
		for _, key := range serverOnlyOmitted {
			properties.Delete(key)
		}
		profile.Set("properties", jsjson.NewObject(properties))
	}
	source := shallowCopy(profile)
	source.Delete("created_at")
	source.Delete("groups")

	merged := deepMergeObjects(existing, source)
	merged.Set("groups", unionGroups(existing.Get("groups"), incoming.Get("groups")))
	return merged
}
