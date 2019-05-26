package runtime

import (
	"runtime"
	"../clog"
)

var (
	RealCrash = true
)

var PanicHandlers = []func(interface{}){}

func HandleCrash(customHandlers ...func(interface{})) {
	if r := recover(); r != nil {
		for _, handler := range customHandlers {
			handler(r)
		}

		for _, handler := range PanicHandlers {
			handler(r)
		}

		if RealCrash {
			panic(r)
		}
	}
}

func logPanic(r interface{}) {
	const size = 64 << 10
	stacktrace := make([]byte, size)
	stacktrace = stacktrace[:runtime.Stack(stacktrace, false)]
	if _, ok := r.(string); ok {
		clog.Errorf("Observed a panic: %s\n%s", r, stacktrace)
	} else {
		clog.Errorf("Observed a panic: %#v (%v)\n%s", r, r, stacktrace)
	}
}