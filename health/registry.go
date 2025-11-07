package health

import "sync"

type Checker func() error

var (
	mu       sync.RWMutex
	checkers = make(map[string]Checker)
)

func Register(name string, fn Checker) {
	mu.Lock()
	defer mu.Unlock()
	checkers[name] = fn
}

func CheckAll() map[string]string {
	mu.RLock()
	defer mu.RUnlock()

	results := make(map[string]string)
	for name, fn := range checkers {
		if err := fn(); err != nil {
			results[name] = "unhealthy: " + err.Error()
		} else {
			results[name] = "healthy"
		}
	}
	return results
}
