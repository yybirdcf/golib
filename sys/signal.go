package sys

import (
	"os"
	"os/signal"
)

var onlyOneSignal = make(chan struct{})

func SetupQuitSignal() <-chan struct{} {
	//调用超过一次造成panic
	close(onlyOneSignal)

	quit := make(chan struct{})
	shutdownSignal := make(chan os.Signal, 2)
	signal.Notify(shutdownSignal, os.Interrupt, os.Kill)

	go func() {
		<-shutdownSignal
		close(quit)

		<-shutdownSignal
		os.Exit(1)
	}()

	return quit
}
