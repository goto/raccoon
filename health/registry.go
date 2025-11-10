package health

type Checker func() error

var (
	checkers = make(map[string]Checker)
)

func Register(name string, fn Checker) {
	checkers[name] = fn
}

func CheckAll() map[string]string {
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
