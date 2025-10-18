package node

import (
	"log"
	"sync"
	"time"
)

type Timer struct {
	mu           sync.Mutex
	duration     time.Duration
	timer        *time.Timer
	timerStarted bool
}

func NewTimer(duration time.Duration) *Timer {
	t := &Timer{
		duration: duration,
	}
	return t
}

func (t *Timer) StartOrReset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.timer != nil {
		t.timer.Reset(t.duration)
		log.Printf("StartOrReset Reset the timer")
	} else {
		t.timer = time.NewTimer(t.duration)
		log.Printf("StartOrReset Started timer")
	}

	t.timerStarted = true
}

func (t *Timer) StartIfNotRunning() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.timerStarted {
		t.timer = time.NewTimer(t.duration)
		t.timerStarted = true
		log.Printf("StartIfNotRunning Started timer")
	} else {
		log.Printf("StartIfNotRunning Timer already running")
	}
}

func (t *Timer) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.timer != nil {
		t.timer.Stop()
	}
	t.timerStarted = false
	log.Printf("Timer stopped")
}

func (node *Node) startMonitoringTimer() {
	for {
		if node.Tmr.timerStarted && node.Tmr.timer != nil {
			select {
			case <-node.Tmr.timer.C:
				if !node.ViewChanging {					
					go node.sendViewChange(node.View + 1)
				} else {
					go node.sendViewChange(node.NewViewNum + 1)
				}
				log.Println("Timer expired")
			default:
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
