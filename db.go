package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Db struct {
	wg             *sync.WaitGroup
	mu             *sync.Mutex
	maxRequestTime int64

	currentRequests int
	connPoolSize    int
	requestChan     chan string
}

func NewDB(connPoolSize int) *Db {
	return &Db{
		mu:           &sync.Mutex{},
		connPoolSize: connPoolSize,
		requestChan:  make(chan string, connPoolSize),
	}
}

func (db *Db) SomeDatabaseQuery(request string) {
	// Block if max active requests reached
	db.requestChan <- request

	db.mu.Lock()
	db.currentRequests++
	count := db.currentRequests
	var timeSinceMax time.Duration

	if count > 0 && count == db.connPoolSize && db.maxRequestTime == 0 {
		db.maxRequestTime = time.Now().Unix()
	} else if count > 0 && count < db.connPoolSize && db.maxRequestTime > 0 {
		db.maxRequestTime = 0
	}

	if db.maxRequestTime > 0 {
		timeSinceMax = time.Since(time.Unix(db.maxRequestTime, 0))
	}
	latency := time.Duration(150*count+(10+rand.Intn(6))) * time.Millisecond
	db.mu.Unlock()

	extraLatency := time.Duration(0)
	if timeSinceMax > 0 {
		extraLatency = time.Duration(float64(timeSinceMax.Seconds())*2) * time.Second
	}
	combinedLatency := latency + extraLatency
	time.Sleep(combinedLatency)

	if request != "" {
		if combinedLatency.Milliseconds() > 1200 {
			fmt.Printf("%s\t [TIMED OUT]\n", request)
		} else {
			if len(request) > 39 {
				fmt.Printf("%s\t %d active requests\t latency: %v\n", request, count, latency)
			} else {
				fmt.Printf("%s\t\t %d active requests\t latency: %v\n", request, count, latency)
			}
		}
	}

	db.mu.Lock()
	db.currentRequests--
	db.mu.Unlock()

	// Free up space in the requestLimit
	<-db.requestChan
}

func (db *Db) GetUsage() float64 {
	db.mu.Lock()
	defer db.mu.Unlock()

	return float64(db.currentRequests) / float64(db.connPoolSize) * 100
}

func (db *Db) SimulateLoad(lower float64, upper float64) {
	generateLower := true
	for {
		func() {
			var usageThreshold float64
			if generateLower {
				usageThreshold = lower
			} else {
				usageThreshold = upper
			}

			start := time.Now()
			for {
				time.Sleep(50 * time.Millisecond)
				if time.Since(start) >= 5*time.Second {
					generateLower = !generateLower
					return
				}

				if db.GetUsage() < usageThreshold {
					go db.SomeDatabaseQuery("")
				}
			}
		}()
	}
}
