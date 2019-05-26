package wait

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"../runtime"
)

var NeverStop <-chan struct{} = make(chan struct{})

type Group struct {
	wg sync.WaitGroup
}

func (g *Group) Wait() {
	g.wg.Wait()
}

func (g *Group) StartWithChannel(stopCh <-chan struct{}, f func(<-chan struct{})) {
	g.Start(func() {
		f(stopCh)
	})
}

func (g *Group) StartWithContext(ctx context.Context, f func(context.Context)) {
	g.Start(func() {
		f(ctx)
	})
}

func (g *Group) Start(f func()) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		f()
	}()
}

func Forever(f func(), period time.Duration) {
	Until(f, period, NeverStop)
}

func Until(f func(), period time.Duration, stopCh <-chan struct{}) {
	jitterUntil(f, period, 0.0, true, stopCh)
}

func NonSlidingUntil(f func(), period time.Duration, stopCh <-chan struct{}) {
	jitterUntil(f, period, 0.0, false, stopCh)
}

func UntilWithContext(ctx context.Context, f func(ctx context.Context), period time.Duration, stopCh <-chan struct{}) {
	jitterUntil(func() { f(ctx) }, period, 0.0, true, stopCh)
}

func NonSlidingUntilWithContext(ctx context.Context, f func(ctx context.Context), period time.Duration, stopCh <-chan struct{}) {
	jitterUntil(func() { f(ctx) }, period, 0.0, false, stopCh)
}

//不稳定可变因子定时周期执行
func jitterUntil(f func(), period time.Duration, factor float64, sliding bool, stopCh <-chan struct{}) {
	var t *time.Timer
	var timeout bool

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		jitteredPeriod := period
		if factor > 0 {
			jitteredPeriod = jitter(period, factor)
		}

		if !sliding {
			t = resetOrReuseTimer(t, jitteredPeriod, timeout)
		}

		go func() {
			defer runtime.HandleCrash()
			f()
		}()

		if sliding {
			t = resetOrReuseTimer(t, jitteredPeriod, timeout)
		}

		select {
		case <-stopCh:
			return
		case <-t.C:
			timeout = true
		}
	}
}

func resetOrReuseTimer(t *time.Timer, duration time.Duration, timeout bool) *time.Timer {
	if t == nil {
		t = new(time.Timer)
	}

	if !t.Stop() && !timeout {
		<-t.C
	}

	t.Reset(duration)
	return t
}

func jitter(duration time.Duration, factor float64) time.Duration {
	if factor < 0 {
		factor = 1.0
	}

	return duration + time.Duration(rand.Float64()*factor*float64(duration))
}
