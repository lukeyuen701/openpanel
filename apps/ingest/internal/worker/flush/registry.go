package flush

import (
	"fmt"
	"sync"
)

// JobSource builds the jobs of one package from the shared runner deps.
type JobSource func(deps Deps) ([]Job, error)

var (
	registryMutex sync.Mutex
	registry      []namedSource
)

type namedSource struct {
	name   string
	source JobSource
}

// Register adds a package's jobs to the set RegisteredJobs returns. The worker
// registers each source at boot, before it builds the scheduler; a package
// that gains jobs (the core buffers) is one more Register call there.
func Register(name string, source JobSource) {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	registry = append(registry, namedSource{name: name, source: source})
}

// ResetRegistry forgets every registered source, for tests.
func ResetRegistry() {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	registry = nil
}

// RegisteredJobs builds every registered source's jobs. A job name two sources
// both define is an error, because only one of them could run.
func RegisteredJobs(deps Deps) ([]Job, error) {
	registryMutex.Lock()
	sources := append([]namedSource(nil), registry...)
	registryMutex.Unlock()

	var jobs []Job
	owner := map[string]string{}
	for _, entry := range sources {
		built, err := entry.source(deps)
		if err != nil {
			return nil, fmt.Errorf("flush: jobs of %s: %w", entry.name, err)
		}
		for _, job := range built {
			if previous, taken := owner[job.Name]; taken {
				return nil, fmt.Errorf("flush: job %s is defined by both %s and %s", job.Name, previous, entry.name)
			}
			owner[job.Name] = entry.name
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}
