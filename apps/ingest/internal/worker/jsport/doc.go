// Package jsport holds the JavaScript behaviours the Bun worker's event path
// depends on, so the Go worker produces the same bytes: property flattening
// (toDots), URL and referrer parsing, string and number conversions, Date and
// Luxon formatting, and JSON that keeps lone surrogates.
//
// Strings are WTF-8: UTF-8 in which an unpaired UTF-16 surrogate is written
// as its own three-byte sequence, so any JavaScript string round-trips. A
// high and a low surrogate are never left adjacent in that form (they are one
// supplementary character); every function here that builds a string keeps
// that invariant, and Join does it for callers that concatenate. Lengths and
// positions are UTF-16 code units, as in JavaScript.
//
// Goldens in testdata/ come from the real TypeScript (bun parity/golden-jsport.ts).
package jsport
