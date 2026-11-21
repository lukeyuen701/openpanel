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

func (k queueKeys) key(suffix string) string { return k.base + suffix }

// moveToActive is the eleven KEYS moveToActive takes; moveToFinished starts
// with the first ten of them.
func (k queueKeys) moveToActive() []string {
	return []string{
		k.key("wait"), k.key("active"), k.key("prioritized"), k.key("events"),
		k.key("stalled"), k.key("limiter"), k.key("delayed"), k.key("paused"),
		k.key("meta"), k.key("pc"), k.key("marker"),
	}
}

func (k queueKeys) moveToFinished(target, jobID string) []string {
	keys := k.moveToActive()[:10]
	return append(keys, k.key(target), k.key(jobID), k.key("metrics:"+target), k.key("marker"))
}

func (k queueKeys) moveToDelayed(jobID string) []string {
	return []string{
		k.key("marker"), k.key("active"), k.key("prioritized"), k.key("delayed"),
		k.key(jobID), k.key("events"), k.key("meta"), k.key("stalled"),
		k.key("wait"), k.key("limiter"), k.key("pc"),
	}
}

func (k queueKeys) retryJob(jobID string) []string {
	return []string{
		k.key("active"), k.key("wait"), k.key("paused"), k.key(jobID),
		k.key("meta"), k.key("events"), k.key("delayed"), k.key("prioritized"),
		k.key("pc"), k.key("marker"), k.key("stalled"),
	}
}

func (k queueKeys) moveStalledJobsToWait() []string {
	return []string{
		k.key("stalled"), k.key("wait"), k.key("active"), k.key("stalled-check"),
		k.key("meta"), k.key("paused"), k.key("marker"), k.key("events"), k.key("repeat"),
	}
}

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
