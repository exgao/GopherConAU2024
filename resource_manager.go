package main

import (
	"fmt"
	"sync"
	"time"
)

// HandlerFunc is a function type that outlines the structure
// of every recipients' message handling logic
//
// It is invoked by the consumer to deliver the message
type HandlerFunc func(message Message)

type ResourceManager struct {
	mu                  *sync.RWMutex
	AlertChan           map[string]chan bool
	LastAlertVal        bool
	alertThreshold      float64
	resolutionThreshold float64
}

func NewResourceManager(alertThreshold float64, resolutionThreshold float64) *ResourceManager {
	rm := &ResourceManager{
		mu:                  &sync.RWMutex{},
		alertThreshold:      alertThreshold,
		resolutionThreshold: resolutionThreshold,
	}

	rm.AlertChan = make(map[string]chan bool)

	return rm
}

func (rm *ResourceManager) Watch(db *Db) {
	for {
		usage := db.GetUsage()
		if rm.LastAlertVal && usage < rm.resolutionThreshold {
			fmt.Println("alert deactivated")
			rm.LastAlertVal = false
			for _, alertChan := range rm.AlertChan {
				alertChan <- false
			}
		} else if !rm.LastAlertVal && usage > rm.alertThreshold {
			fmt.Println("alert activated")
			rm.LastAlertVal = true
			for _, alertChan := range rm.AlertChan {
				alertChan <- true
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}
