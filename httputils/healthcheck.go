package httputils

import (
	"fmt"
	"net/http"

	"github.com/yybirdcf/golib/clog"
)

type HealthCheck interface {
	Name() string
	Check(r *http.Request) error
}

type ping struct{}

func (p *ping) Name() string {
	return "ping"
}

func (p *ping) Check(r *http.Request) error {
	return nil
}

func DefaultHealthCheck(mux *http.ServeMux) {
	p := &ping{}
	InstallHealthChecks(mux, p)
}

func InstallHealthChecks(mux *http.ServeMux, checks ...HealthCheck) {
	for _, check := range checks {
		mux.HandleFunc(fmt.Sprintf("health/%s", check.Name()), AdapterHttpHandleFunc(check))
	}
}

func AdapterHttpHandleFunc(c HealthCheck) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		err := c.Check(req)
		if err != nil {
			clog.Errorf("healthcheck a error: %s\n%#v\n%s", c.Name(), err, err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}
