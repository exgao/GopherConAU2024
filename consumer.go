package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

type HandlerFunc func(message Message)

type Consumer struct {
	wg *sync.WaitGroup

	alertActive     *atomic.Bool
	currentRequests *atomic.Int32

	Name       string
	Deliveries <-chan Message
	Handler    HandlerFunc
}

type Throttler interface {
	Validate() error
	Apply(ctx context.Context)
	CheckAndReset(alertVal bool)
}

// [change 1] Interfaces are your best friend
// Accept interfaces, return concrete types

func StartupConsumers(wg *sync.WaitGroup, queues []Queue, m *Messenger, rm *ResourceManager) error {
	for _, queue := range queues {
		m.DeclareQueue(queue.Name)

		throttledBindings := []string{}
		throttledMu := &sync.Mutex{}
		for index, binding := range queue.Binding {
			err := m.BindQueue(queue.Name, binding.Topic, binding.RoutingKey)
			if err != nil {
				return err
			}

			if binding.throttler == nil {
				continue
			}

			err = binding.throttler.Validate()
			if err != nil {
				fmt.Printf("invalid throttler configuration: %s", err)
				return err
			}

			// Create secondary queue and consumer
			secondaryQueueName := fmt.Sprintf("%s-throttle", queue.Name)
			throttledRoutingKey := fmt.Sprintf("%s-throttle", binding.RoutingKey)

			m.DeclareQueue(secondaryQueueName)
			err = m.BindQueue(secondaryQueueName, binding.Topic, throttledRoutingKey)
			if err != nil {
				return err
			}

			throttledBindings = append(throttledBindings, binding.RoutingKey)
			secondaryConsumer := &Consumer{
				Name: fmt.Sprintf("%s-%d", secondaryQueueName, index),
				// [change 3] Keep it to yourself
				Deliveries:      m.queues[secondaryQueueName],
				wg:              wg,
				alertActive:     newAtomicFalse(),
				currentRequests: newAtomicZero(),
				Handler:         queue.Consumer.Handler,
			}
			wg.Add(1)

			rm.AlertChan[secondaryConsumer.Name] = make(chan bool, 1)
			go secondaryConsumer.ThrottleAndConsume(rm.AlertChan[secondaryConsumer.Name], binding.throttler, throttledMu)
		}

		c := &Consumer{
			Name:            queue.Name,
			Deliveries:      m.queues[queue.Name],
			wg:              wg,
			alertActive:     newAtomicFalse(),
			currentRequests: newAtomicZero(),
			Handler:         queue.Consumer.Handler,
		}

		wg.Add(1)

		rm.AlertChan[c.Name] = make(chan bool, 1)
		go c.Consume(rm.AlertChan[c.Name], m, throttledBindings)
	}
	return nil
}

func (c *Consumer) Consume(alertChan <-chan bool, m *Messenger, throttleKeys []string) {
	defer c.wg.Done()

	go func() {
		for {
			alert, ok := <-alertChan
			if !ok {
				return
			}
			c.alertActive.Store(alert)
		}
	}()

	for {
		d, ok := <-c.Deliveries
		// stop consumer if channel has been closed
		if !ok {
			if len(throttleKeys) > 0 {
				//close throttle delivery channel as well
				close(m.queues[fmt.Sprintf("%s-throttle", c.Name)])
			}
			return
		}

		topic := d.GetHeader(topicHeader)
		routingKey := d.GetHeader(routingKeyHeader)

		// need to distinguish routing keys to throttle
		if c.alertActive.Load() && shouldThrottleRoutingKey(routingKey, throttleKeys) {
			m.Publish(topic, fmt.Sprintf("%s-throttle", routingKey), d.Body)
			continue
		}

		c.currentRequests.Add(1)

		c.Handler(d)

		c.currentRequests.Add(-1)
	}
}

func (c *Consumer) ThrottleAndConsume(alertChan <-chan bool, throttler Throttler, workerMu *sync.Mutex) {
	defer c.wg.Done()
	processed := make(chan bool)
	for {
		workerMu.Lock()

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			select {
			case <-processed:
			case alert := <-alertChan:
				c.alertActive.Store(alert)
				<-processed
			}
			cancel()
			workerMu.Unlock()
		}()

		d, ok := <-c.Deliveries
		// stop consumer if channel has been closed
		if !ok {
			return
		}

		throttler.CheckAndReset(c.alertActive.Load())

		routingKey := d.GetHeader(routingKeyHeader)

		throttler.Apply(ctx)
		d.SetHeader(routingKeyHeader, strings.TrimSuffix(routingKey, "-throttle"))

		// invoke handlerFunc
		c.Handler(d)
		processed <- true
	}
}

func shouldThrottleRoutingKey(routingKey string, throttleKeys []string) bool {
	for _, throttleKey := range throttleKeys {
		if routingKey == throttleKey {
			return true
		}
	}
	return false
}

func newAtomicFalse() *atomic.Bool {
	a := new(atomic.Bool)
	a.Store(false)

	return a
}

func newAtomicZero() *atomic.Int32 {
	a := new(atomic.Int32)
	a.Store(0)

	return a
}

// Custom throttler

type customFunc func(ctx context.Context)

type CustomThrottler struct {
	throttleFunc customFunc
}

func NewCustomThrottler(f customFunc) *CustomThrottler {
	return &CustomThrottler{
		throttleFunc: f,
	}
}

func (c *CustomThrottler) Validate() error {
	return nil
}

func (c *CustomThrottler) Apply(ctx context.Context) {
	c.throttleFunc(ctx)
}

func (c *CustomThrottler) CheckAndReset(alertVal bool) {
}
