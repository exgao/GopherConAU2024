package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	throttle = "throttle"
	recovery = "recovery"
)

type adjustableDelay struct {
	val time.Duration
	mu  sync.RWMutex
}

// adjust spins off a goroutine to update the throttler's current delay value based on the configured multiplier and Interval
// it runs until the throttler's context is cancelled or maxDelay is reached, whichever happens first
func (a *adjustableDelay) adjust(interval time.Duration, multiplier float64, maxDelay time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		for {
			select {
			case <-t.C:
				a.set(time.Duration(float64(a.get().Milliseconds())*multiplier) * time.Millisecond)
				if multiplier > 1 && a.get().Milliseconds() >= maxDelay.Milliseconds() {
					a.set(maxDelay)
					return
				} else if multiplier < 1 && a.get().Seconds() <= 1 {
					a.set(0)
					return
				}
			}
		}
	}()
}

func (a *adjustableDelay) get() time.Duration {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.val
}

func (a *adjustableDelay) set(val time.Duration) {
	a.mu.Lock()
	a.val = val
	a.mu.Unlock()
}

// *** Block throttle ***

type Block struct {
	// user defined
	recoveryDelay      time.Duration
	recoveryIncrement  time.Duration
	recoveryMultiplier float64

	// internal
	mu            sync.RWMutex
	delay         adjustableDelay
	throttleStart int64
	recoveryStart int64
	lastTimestamp time.Time
}

func (b *Block) CheckAndReset(alertVal bool) {
	if alertVal && b.Status() != throttle {
		b.initThrottle()
	} else if !alertVal && b.Status() != recovery {
		b.initRecovery()
	}
}

func (b *Block) Validate() error {
	if b == nil {
		return nil
	}
	// recovery values
	if b.recoveryDelay.Milliseconds() < 50 || b.recoveryDelay.Hours() > 8 {
		return fmt.Errorf("recoveryDelay %s must be at least 50 ms and less than 8 hours", b.recoveryDelay.String())
	}
	if b.recoveryIncrement.Milliseconds() < 50 {
		return fmt.Errorf("recoveryIncrement %s must be at least 50 ms", b.recoveryIncrement.String())
	}
	if b.recoveryMultiplier <= 0 || b.recoveryMultiplier >= 1 {
		return fmt.Errorf("recoveryMultiplier %f must be greater than 0 and less than 1", b.recoveryMultiplier)
	}
	return nil
}

func (b *Block) initThrottle() {
	b.mu.Lock()
	b.recoveryStart = 0
	b.throttleStart = time.Now().Unix()
	b.mu.Unlock()
}

func (b *Block) initRecovery() {
	b.mu.Lock()
	b.throttleStart = 0
	b.recoveryStart = time.Now().Unix()
	b.delay.set(b.recoveryDelay)
	b.mu.Unlock()

	b.delay.adjust(b.recoveryIncrement, b.recoveryMultiplier, 0)
}

func (b *Block) Status() string {
	switch {
	case b.throttleStart > 0:
		return throttle
	case b.recoveryStart > 0 && b.delay.get() > 0:
		return recovery
	default:
		return ""
	}
}

func (b *Block) Apply(ctx context.Context) {
	switch b.Status() {
	case throttle:
		<-ctx.Done()
	case recovery:
		t := time.NewTimer(b.delay.get())
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			return
		}
	}
}

func NewBlockThrottler(recoveryDelay time.Duration, recoveryIncrement time.Duration, recoveryMultiplier float64) *Block {
	return &Block{
		recoveryDelay:      recoveryDelay,
		recoveryIncrement:  recoveryIncrement,
		recoveryMultiplier: recoveryMultiplier,
	}
}

// *** Interval throttle ***

type Interval struct {
	// user input
	throttleDelay      time.Duration
	recoveryDelay      time.Duration
	recoveryIncrement  time.Duration
	recoveryMultiplier float64

	// internal
	mu            sync.Mutex
	delay         adjustableDelay
	throttleStart int64
	recoveryStart int64
	lastTimestamp time.Time
}

func (i *Interval) Validate() error {
	if i == nil {
		return nil
	}
	// throttle values
	if i.throttleDelay.Milliseconds() < 50 {
		return fmt.Errorf("delay %s must be at least 50 ms", i.throttleDelay.String())
	}

	// recovery values
	if i.recoveryIncrement.Milliseconds() < 50 {
		return fmt.Errorf("recoveryIncrement %s must be at least 50 ms", i.recoveryIncrement.String())
	}
	if i.recoveryMultiplier <= 0 || i.recoveryMultiplier >= 1 {
		return fmt.Errorf("recoveryMultiplier %f must be between 0 and 1", i.recoveryMultiplier)
	}
	return nil
}

func (i *Interval) CheckAndReset(alertVal bool) {
	if alertVal && i.Status() != throttle {
		i.initThrottle()
	} else if !alertVal && i.Status() != recovery {
		i.initRecovery()
	}
}

func (i *Interval) initThrottle() {
	i.mu.Lock()
	i.recoveryStart = 0
	i.throttleStart = time.Now().Unix()
	i.delay.set(i.throttleDelay)
	i.mu.Unlock()
}

func (i *Interval) initRecovery() {
	i.mu.Lock()
	i.throttleStart = 0
	i.delay.set(i.throttleDelay)
	i.recoveryStart = time.Now().Unix()
	i.mu.Unlock()

	i.delay.adjust(i.recoveryIncrement, i.recoveryMultiplier, 0)
}

func (i *Interval) Status() string {
	switch {
	case i.throttleStart > 0:
		return throttle
	case i.recoveryStart > 0 && i.delay.get() > 0:
		return recovery
	default:
		return ""
	}
}

func (i *Interval) Apply(ctx context.Context) {
	d := i.delay.get()
	if d == 0 {
		return
	}
	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		return
	case <-t.C:
		return
	}
}

func NewIntervalThrottler(throttleDelay time.Duration, recoveryIncrement time.Duration, recoveryMultiplier float64) *Interval {
	return &Interval{
		throttleDelay:      throttleDelay,
		recoveryIncrement:  recoveryIncrement,
		recoveryMultiplier: recoveryMultiplier,
	}
}

// *** Backoff throttle ***

type Backoff struct {
	// user input
	initialDelay       time.Duration
	backoffInterval    time.Duration
	multiplier         float64
	maxDelay           time.Duration
	recoveryDelay      time.Duration
	recoveryIncrement  time.Duration
	recoveryMultiplier float64

	// internal
	mu            sync.Mutex
	throttleStart int64
	delay         adjustableDelay
	recoveryStart int64
	lastTimestamp time.Time
}

func (bo *Backoff) Validate() error {
	if bo == nil {
		return nil
	}
	if bo.initialDelay.Milliseconds() < 50 {
		return fmt.Errorf("initialDelay %s must be at least 50 ms", bo.initialDelay.String())
	}
	if bo.backoffInterval.Milliseconds() < 50 {
		return fmt.Errorf("backoffInterval %s must be at least 50 ms", bo.backoffInterval.String())
	}
	if bo.multiplier <= 1 {
		return fmt.Errorf("multiplier %f must be greater than 1", bo.multiplier)
	}

	if bo.maxDelay.Milliseconds() < 50 || bo.maxDelay.Hours() > 8 {
		return fmt.Errorf("maxDelay %s must be greater than 50 ms and less than 8 hours", bo.maxDelay.String())
	}
	if bo.initialDelay >= bo.maxDelay {
		return fmt.Errorf("maxDelay %s must be greater than initialDelay %s", bo.maxDelay.String(), bo.initialDelay.String())
	}
	return nil
}

func (bo *Backoff) CheckAndReset(alertVal bool) {
	if alertVal && bo.Status() != throttle {
		bo.initThrottle()
	} else if !alertVal && bo.Status() != recovery {
		bo.initRecovery()
	}
}

func (bo *Backoff) initThrottle() {
	bo.mu.Lock()
	bo.recoveryStart = 0
	bo.throttleStart = time.Now().Unix()
	bo.delay.set(bo.initialDelay)
	bo.mu.Unlock()

	bo.delay.adjust(bo.backoffInterval, bo.multiplier, bo.maxDelay)
}

func (bo *Backoff) initRecovery() {
	bo.mu.Lock()
	bo.throttleStart = 0
	bo.recoveryStart = time.Now().Unix()
	bo.delay.set(min(bo.delay.get(), bo.maxDelay))
	bo.mu.Unlock()

	bo.delay.adjust(bo.backoffInterval, 1/bo.multiplier, 0)
}

func (bo *Backoff) Status() string {
	switch {
	case bo.throttleStart > 0:
		return throttle
	case bo.recoveryStart > 0 && bo.delay.get() > 0:
		return recovery
	default:
		return ""
	}
}

func (bo *Backoff) Apply(ctx context.Context) {
	d := bo.delay.get()
	if d == 0 {
		time.Sleep(250 * time.Millisecond)
		return
	}
	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		return
	case <-t.C:
		return
	}
}

func NewBackoffThrottler(initialDelay time.Duration, backoffInterval time.Duration, multiplier float64, maxDelay time.Duration) *Backoff {
	var recoveryMultiplier float64
	if multiplier > 0 {
		recoveryMultiplier = 1 / multiplier
	}
	return &Backoff{
		initialDelay:       initialDelay,
		backoffInterval:    backoffInterval,
		multiplier:         multiplier,
		maxDelay:           maxDelay,
		recoveryDelay:      maxDelay,
		recoveryIncrement:  backoffInterval,
		recoveryMultiplier: recoveryMultiplier,
	}
}
