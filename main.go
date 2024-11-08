package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

func main() {
	fmt.Println("Hello, GopherConAU 2024!")
	consumerWG := sync.WaitGroup{}

	// [change 2] Structs can help clarify parameters
	rm := NewResourceManager(70, 40)

	db := NewDB(8)

	go rm.Watch(db)
	go db.SimulateLoad(20, 80)

	h := NewHandler(db)
	queues := []Queue{
		{
			Name: "GopherConAU Day 2",
			Binding: []Binding{
				{
					Topic:      "GopherConAU Day 2",
					RoutingKey: "scheduled-talks",
				},
				{
					Topic:      "GopherConAU Day 2",
					RoutingKey: "afterparty-fun",
				},
			},
			Consumer: Consumer{
				Handler: h.ProcessMessage,
			},
		},
		{
			Name: "GopherConAU Day 1",
			Binding: []Binding{
				{
					Topic:      "GopherConAU Day 1",
					RoutingKey: "photos-repo*",
					throttler:  NewIntervalThrottler(5*time.Second, 2*time.Second, 0.5),
				},
				{
					Topic:      "GopherConAU Day 1",
					RoutingKey: "lost-and-found*",
					throttler:  NewBackoffThrottler(2*time.Second, 2*time.Second, 2, 8*time.Second),
				},
				{
					Topic:      "GopherConAU Day 1",
					RoutingKey: "feedback-form*",
					throttler:  NewBlockThrottler(4*time.Second, 2*time.Second, 0.5),
				},
				{
					Topic:      "GopherConAU Day 1",
					RoutingKey: "discussions*",
					throttler:  NewBlockThrottler(4*time.Second, 2*time.Second, 0.5),
				},
			},
			Consumer: Consumer{
				Handler: h.ProcessMessage,
			},
		},
		{
			Name: "Workshop Day",
			Binding: []Binding{
				{
					Topic:      "Workshop Day",
					RoutingKey: "photos-uploaded*",
					throttler:  NewIntervalThrottler(5*time.Second, 2*time.Second, 0.5),
				},
				{
					Topic:      "Workshop Day",
					RoutingKey: "lost-and-found*",
					throttler:  NewBackoffThrottler(2*time.Second, 2*time.Second, 2, 8*time.Second),
				},
				{
					Topic:      "Workshop Day",
					RoutingKey: "feedback-form*",
					throttler:  NewBlockThrottler(4*time.Second, 2*time.Second, 0.5),
				},
				{
					Topic:      "Workshop Day",
					RoutingKey: "discussion-board*",
					throttler: NewCustomThrottler(func(ctx context.Context) {
						time.Sleep(2 * time.Second)
					}),
				},
			},
			Consumer: Consumer{
				Handler: h.ProcessMessage,
			},
		},
	}

	messenger := NewMessenger()

	producer := NewProducer(messenger, queues)

	err := StartupConsumers(&consumerWG, queues, messenger, rm)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	producer.Run()

	// wait until all consumers are done processing
	consumerWG.Wait()

}
