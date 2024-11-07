package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Producer struct {
	m      *Messenger
	queues []Queue
}

func NewProducer(messenger *Messenger, queues []Queue) *Producer {
	return &Producer{
		m:      messenger,
		queues: queues,
	}
}

func (p *Producer) Run() {
	for _, queue := range p.queues {
		wg := &sync.WaitGroup{}
		for _, binding := range queue.Binding {
			wg.Add(1)
			go func() {
				for i := range 10 {
					p.m.Publish(binding.Topic, binding.RoutingKey,
						fmt.Sprintf("[%s]/[%s] %d", binding.Topic, binding.RoutingKey, i))
					time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
				}
				wg.Done()
			}()
		}
		wg.Wait()
		// close delivery channel when all messages have been sent
		close(p.m.queues[queue.Name])
	}
}
