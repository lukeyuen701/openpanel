package simple

import (
	"openpanel/ingest/internal/worker/config"
	"openpanel/ingest/internal/worker/flush"
)

// Buffer is one of the simple buffers with the cron job that flushes it.
type Buffer struct {
	Job     string
	Flusher flush.Flusher
}

// Buffers are the four simple buffers, in the TypeScript registration order.
func Buffers(cfg config.BuffersConfig, deps Deps) []Buffer {
	return []Buffer{
		{flush.JobFlushBots, NewBot(cfg, deps, Overrides{})},
		{flush.JobFlushReplay, NewReplay(cfg, deps, Overrides{})},
		{flush.JobFlushGroups, NewGroup(cfg, deps, Overrides{})},
		{flush.JobFlushAiEvents, NewAiEvent(cfg, deps, Overrides{})},
	}
}

// Jobs builds a runner and a scheduler job for each buffer. runner supplies
// the shared Redis, clock, pause check and observer; ai-event gets no observer
// unless instrumentAiEvent is set, because the TypeScript worker records no
// flush metrics for it (it is missing from buffer.metrics.ts).
func Jobs(buffers []Buffer, runner flush.Deps, instrumentAiEvent bool) ([]flush.Job, error) {
	var jobs []flush.Job
	for _, buffer := range buffers {
		deps := runner
		if buffer.Job == flush.JobFlushAiEvents && !instrumentAiEvent {
			deps.Observer = nil
		}
		r, err := flush.NewRunner(buffer.Flusher, deps)
		if err != nil {
			return nil, err
		}
		job, err := flush.NewJob(buffer.Job, r)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

// ListRefs are the buffers' lists for flush.RegisterLengthGauges, ai-event
// included only when instrumentAiEvent is set.
func ListRefs(buffers []Buffer, instrumentAiEvent bool) []flush.ListRef {
	var refs []flush.ListRef
	for _, buffer := range buffers {
		if buffer.Job == flush.JobFlushAiEvents && !instrumentAiEvent {
			continue
		}
		refs = append(refs, flush.ListRef{Name: buffer.Flusher.Name(), ListKey: buffer.Flusher.ListKey()})
	}
	return refs
}
