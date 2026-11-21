// Package events is what the Go worker does for one record of the events
// topic: incomingEvent of the TypeScript consumer (derivations, session
// lifecycle, the event row, the profile and AI rows, notification rules, first
// event) and the Redis buffers it writes to (docs/GO_WORKER_CONTRACTS.md P1.5,
// P1.6).
//
// The event payload is a jsjson.Obj for as long as it lives: the key order is
// what BullMQ job data and the stored notification serialise, an undefined
// member keeps its position, and createEvent and the notification check mutate
// the one shared object, as they do in TypeScript.
//
// A Handler is safe for concurrent use. The consumer's key serialisation is what
// keeps two messages of one device from racing through the session buffer.
package events
