/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Modifications:

- 2019, @john.koepi/@sitano extract pool, removed spanner specific code
*/

package gcopool

import (
	"container/heap"
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

// healthChecker performs periodical health checks on registered sessions.
type healthChecker struct {
	// mu protects concurrent access to hcQueue.
	mu sync.Mutex
	// queue is the priority queue for session health checks. Sessions with lower nextCheck rank higher in the queue.
	queue sheap
	// interval is the average interval between two health checks on a session.
	interval time.Duration
	// workers is the number of concurrent health check workers.
	workers int
	// waitWorkers waits for all health check workers to exit
	waitWorkers sync.WaitGroup
	// pool is the underlying session pool.
	pool *Pool
	// sampleInterval is the interval of sampling by the maintainer.
	sampleInterval time.Duration
	// ready is used to signal that maintainer can start running.
	ready chan struct{}
	// done is used to signal that health checker should be closed.
	done chan struct{}
	// once is used for closing channel done only once.
	once             sync.Once
	maintainerCancel func()
}

// newHealthChecker initializes new instance of healthChecker.
func newHealthChecker(interval time.Duration, workers int, sampleInterval time.Duration, pool *Pool) *healthChecker {
	if workers <= 0 {
		workers = 1
	}
	hc := &healthChecker{
		interval:         interval,
		workers:          workers,
		pool:             pool,
		sampleInterval:   sampleInterval,
		ready:            make(chan struct{}),
		done:             make(chan struct{}),
		maintainerCancel: func() {},
	}
	hc.waitWorkers.Add(1)
	go hc.maintainer()
	for i := 1; i <= hc.workers; i++ {
		hc.waitWorkers.Add(1)
		go hc.worker(i)
	}
	return hc
}

// Close closes the healthChecker and waits for all healthcheck workers to exit.
func (hc *healthChecker) close() {
	hc.mu.Lock()
	hc.maintainerCancel()
	hc.mu.Unlock()
	hc.once.Do(func() { close(hc.done) })
	hc.waitWorkers.Wait()
}

// isClosing checks if a healthChecker is already closing.
func (hc *healthChecker) isClosing() bool {
	select {
	case <-hc.done:
		return true
	default:
		return false
	}
}

// getInterval gets the healthcheck interval.
func (hc *healthChecker) getInterval() time.Duration {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	return hc.interval
}

// scheduledHCLocked schedules next healthcheck on session s with the assumption that hc.mu is being held.
func (hc *healthChecker) scheduledHCLocked(s *session) {
	// The next healthcheck will be scheduled after [interval*0.5, interval*1.5) nanoseconds.
	nsFromNow := rand.Int63n(int64(hc.interval)) + int64(hc.interval)/2
	s.setNextCheck(time.Now().Add(time.Duration(nsFromNow)))
	if hi := s.getHCIndex(); hi != -1 {
		// Session is still being tracked by healthcheck workers.
		heap.Fix(&hc.queue, hi)
	}
}

// scheduledHC schedules next healthcheck on session s. It is safe to be called concurrently.
func (hc *healthChecker) scheduledHC(s *session) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.scheduledHCLocked(s)
}

// register registers a session with healthChecker for periodical healthcheck.
func (hc *healthChecker) register(s *session) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.scheduledHCLocked(s)
	heap.Push(&hc.queue, s)
}

// unregister unregisters a session from healthcheck queue.
func (hc *healthChecker) unregister(s *session) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	oi := s.setHCIndex(-1)
	if oi >= 0 {
		heap.Remove(&hc.queue, oi)
	}
}

// markDone marks that health check for session has been performed.
func (hc *healthChecker) markDone(s *session) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	s.checkingHealth = false
}

// healthCheck checks the health of the session and pings it if needed.
func (hc *healthChecker) healthCheck(s *session) {
	defer hc.markDone(s)
	if !s.pool.IsValid() {
		// Session pool is closed, perform a garbage collection.
		s.destroy(false)
		return
	}
	if err := s.ping(); shouldDropSession(err) {
		// Ping failed, Destroy the session.
		s.destroy(false)
	}
}

// worker performs the healthcheck on sessions in healthChecker's priority queue.
func (hc *healthChecker) worker(i int) {
	// Returns a session which we should ping to keep it alive.
	getNextForPing := func() *session {
		hc.pool.mu.Lock()
		defer hc.pool.mu.Unlock()
		hc.mu.Lock()
		defer hc.mu.Unlock()
		if hc.queue.Len() <= 0 {
			// Queue is empty.
			return nil
		}
		s := hc.queue.sessions[0]
		if s.getNextCheck().After(time.Now()) && hc.pool.valid {
			// All sessions have been checked recently.
			return nil
		}
		hc.scheduledHCLocked(s)
		if !s.checkingHealth {
			s.checkingHealth = true
			return s
		}
		return nil
	}

	// Returns a session which we should prepare for write.
	getNextForTx := func() *session {
		hc.pool.mu.Lock()
		defer hc.pool.mu.Unlock()
		if hc.pool.shouldPrepareWrite() {
			if hc.pool.idleList.Len() > 0 && hc.pool.valid {
				hc.mu.Lock()
				defer hc.mu.Unlock()
				if hc.pool.idleList.Front().Value.(*session).checkingHealth {
					return nil
				}
				session := hc.pool.idleList.Remove(hc.pool.idleList.Front()).(*session)
				session.checkingHealth = true
				hc.pool.prepareReqs++
				return session
			}
		}
		return nil
	}

	for {
		if hc.isClosing() {
			// Exit when the pool has been closed and all sessions have been destroyed
			// or when health checker has been closed.
			hc.waitWorkers.Done()
			return
		}
		ws := getNextForTx()
		if ws != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := ws.prepareForWrite(ctx)
			cancel()
			if err != nil {
				// Skip handling prepare error, session can be prepared in next cycle
				log.Printf("Failed to prepare session, error: %v", err)
			}
			hc.pool.recycle(ws)
			hc.pool.mu.Lock()
			hc.pool.prepareReqs--
			hc.pool.mu.Unlock()
			hc.markDone(ws)
		}
		rs := getNextForPing()
		if rs == nil {
			if ws == nil {
				// No work to be done so sleep to avoid burning cpu
				pause := int64(100 * time.Millisecond)
				if pause > int64(hc.interval) {
					pause = int64(hc.interval)
				}
				select {
				case <-time.After(time.Duration(rand.Int63n(pause) + pause/2)):
				case <-hc.done:
				}

			}
			continue
		}
		hc.healthCheck(rs)
	}
}

// maintainer maintains the maxSessionsInUse by a window of kWindowSize * sampleInterval.
// Based on this information, health checker will try to maintain the number of sessions by hc..
func (hc *healthChecker) maintainer() {
	// Wait so that pool is ready.
	<-hc.ready

	var (
		windowSize uint64 = 10
		iteration  uint64
	)

	for {
		if hc.isClosing() {
			hc.waitWorkers.Done()
			return
		}

		// maxSessionsInUse is the maximum number of sessions in use concurrently over a period of time.
		var maxSessionsInUse uint64

		// Updates metrics.
		hc.pool.mu.Lock()
		currSessionsInUse := hc.pool.numOpened - uint64(hc.pool.idleList.Len()) - uint64(hc.pool.idleWriteList.Len())
		currSessionsOpened := hc.pool.numOpened
		hc.pool.mu.Unlock()

		hc.mu.Lock()
		if iteration%windowSize == 0 || maxSessionsInUse < currSessionsInUse {
			maxSessionsInUse = currSessionsInUse
		}
		sessionsToKeep := maxUint64(hc.pool.MinOpened,
			minUint64(currSessionsOpened, hc.pool.MaxIdle+maxSessionsInUse))
		ctx, cancel := context.WithTimeout(context.Background(), hc.sampleInterval)
		hc.maintainerCancel = cancel
		hc.mu.Unlock()

		// Replenish or Shrink pool if needed.
		// Note: we don't need to worry about pending create session requests, we only need to sample the current sessions in use.
		// the routines will not try to create extra / delete creating sessions.
		if sessionsToKeep > currSessionsOpened {
			hc.replenishPool(ctx, sessionsToKeep)
		} else {
			hc.shrinkPool(ctx, sessionsToKeep)
		}

		select {
		case <-ctx.Done():
		case <-hc.done:
			cancel()
		}
		iteration++
	}
}

// replenishPool is run if numOpened is less than sessionsToKeep, timeouts on sampleInterval.
func (hc *healthChecker) replenishPool(ctx context.Context, sessionsToKeep uint64) {
	for {
		if ctx.Err() != nil {
			return
		}

		p := hc.pool
		p.mu.Lock()
		// Take budget before the actual session creation.
		if sessionsToKeep <= p.numOpened {
			p.mu.Unlock()
			break
		}
		p.numOpened++
		recordStat(ctx, OpenSessionCount, int64(p.numOpened))
		p.createReqs++
		shouldPrepareWrite := p.shouldPrepareWrite()
		p.mu.Unlock()
		var (
			s   *session
			err error
		)
		if s, err = p.createSession(ctx); err != nil {
			log.Printf("Failed to create session, error: %v", err)
			continue
		}
		if shouldPrepareWrite {
			if err = s.prepareForWrite(ctx); err != nil {
				p.recycle(s)
				log.Printf("Failed to prepare session, error: %v", err)
				continue
			}
		}
		p.recycle(s)
	}
}

// shrinkPool, scales down the session pool.
func (hc *healthChecker) shrinkPool(ctx context.Context, sessionsToKeep uint64) {
	for {
		if ctx.Err() != nil {
			return
		}

		p := hc.pool
		p.mu.Lock()

		if sessionsToKeep >= p.numOpened {
			p.mu.Unlock()
			break
		}

		var s *session
		if p.idleList.Len() > 0 {
			s = p.idleList.Front().Value.(*session)
		} else if p.idleWriteList.Len() > 0 {
			s = p.idleWriteList.Front().Value.(*session)
		}
		p.mu.Unlock()
		if s != nil {
			// Destroy session as expire.
			s.destroy(true)
		} else {
			break
		}
	}
}

// shouldDropSession returns true if a particular error leads to the removal of a session
func shouldDropSession(err error) bool {
	return err == ErrUnavailable
}

// maxUint64 returns the maximum of two uint64
func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// minUint64 returns the minimum of two uint64
func minUint64(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}
