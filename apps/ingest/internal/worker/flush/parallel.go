package flush

import "sync"

// ParallelLimit runs fn over items with at most concurrency in flight
// (BaseBuffer.parallelLimit). After the first failure no new item starts, and
// the first error is returned only once the running items settled, so nothing
// is still writing when the caller releases its flush lock. A concurrency of
// 1 or less, or a single item, runs sequentially.
func ParallelLimit[T any](concurrency int, items []T, fn func(index int, item T) error) error {
	if len(items) == 0 {
		return nil
	}
	if concurrency <= 1 || len(items) == 1 {
		for index, item := range items {
			if err := fn(index, item); err != nil {
				return err
			}
		}
		return nil
	}
	var (
		mutex     sync.Mutex
		nextIndex int
		firstErr  error
		group     sync.WaitGroup
	)
	claim := func() (int, bool) {
		mutex.Lock()
		defer mutex.Unlock()
		if firstErr != nil || nextIndex >= len(items) {
			return 0, false
		}
		index := nextIndex
		nextIndex++
		return index, true
	}
	for range min(concurrency, len(items)) {
		group.Add(1)
		go func() {
			defer group.Done()
			for {
				index, ok := claim()
				if !ok {
					return
				}
				if err := fn(index, items[index]); err != nil {
					mutex.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mutex.Unlock()
				}
			}
		}()
	}
	group.Wait()
	return firstErr
}

// Chunks splits items into runs of at most size, in order. A size below 1 is
// one chunk.
func Chunks[T any](items []T, size int) [][]T {
	if size < 1 {
		size = max(len(items), 1)
	}
	var chunks [][]T
	for start := 0; start < len(items); start += size {
		chunks = append(chunks, items[start:min(start+size, len(items))])
	}
	return chunks
}
