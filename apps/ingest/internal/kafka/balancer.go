package kafka

import "github.com/twmb/franz-go/pkg/kgo"

// EventsGroupProtocol is the protocol name the Bun consumers advertise
// (EVENTS_GROUP_PROTOCOLS in packages/core/src/modules/ingest/src/kafka.ts).
// Members of one group must share it, so a Go member that advertised
// franz-go's own "roundrobin" could not join a group holding a Bun member.
const EventsGroupProtocol = "RoundRobinAssigner"

type eventsGroupBalancer struct{ kgo.GroupBalancer }

func (eventsGroupBalancer) ProtocolName() string { return EventsGroupProtocol }

// EventsGroupBalancer is round robin under the Bun consumers' protocol name.
func EventsGroupBalancer() kgo.GroupBalancer {
	return eventsGroupBalancer{kgo.RoundRobinBalancer()}
}
