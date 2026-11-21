package bullmq

import "strings"

const (
	keyPrefix = "bull"
	// Written to the queue's meta hash by BullMQ's Queue constructor.
	metaMaxLenEventsField = "opts.maxLenEvents"
	metaMaxLenEvents      = "10000"
	metaVersionField      = "version"
	metaVersion           = "bullmq:6.7.8"
)

// QueueKey is the name a queue lives under: core/jobs/naming.ts queueKey.
// namespace isolates deployments sharing a Redis (QUEUE_NAMESPACE); cluster
// wraps the name in braces so Redis Cluster hashes all of a queue's keys to
// one slot (QUEUE_CLUSTER).
func QueueKey(name, namespace string, cluster bool) string {
	scoped := name
	if namespace != "" {
		scoped = name + "-" + namespace
	}
	if cluster {
		return "{" + scoped + "}"
	}
	return scoped
}

// queueKeys is the nine KEYS addStandardJob takes, in script order.
type queueKeys struct{ base string }

func newQueueKeys(queueKey string) queueKeys {
	return queueKeys{base: keyPrefix + ":" + queueKey + ":"}
}

func (k queueKeys) meta() string { return k.base + "meta" }

func (k queueKeys) addStandardJob() []string {
	return []string{
		k.base + "wait",
		k.base + "paused",
		k.base + "meta",
		k.base + "id",
		k.base + "completed",
		k.base + "delayed",
		k.base + "active",
		k.base + "events",
		k.base + "marker",
	}
}

// refusesCustomID mirrors Job.validateOptions: a custom id may not be what
// parseInt would print back, and may only contain ":" as exactly three parts.
func refusesCustomID(id string) error {
	if looksLikeInteger(id) {
		return errCustomIDInteger
	}
	if strings.Contains(id, ":") && len(strings.Split(id, ":")) != 3 {
		return errCustomIDColon
	}
	return nil
}
