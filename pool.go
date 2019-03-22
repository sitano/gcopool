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

package spanner

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// Pool creates and caches Cloud Spanner sessions.
type Pool struct {
	// mu protects Pool from concurrent access.
	mu sync.Mutex
	// valid marks the validity of the session pool.
	valid bool
	// idleList caches idle session IDs. Session IDs in this list can be allocated for use.
	idleList list.List
	// idleWriteList caches idle sessions which have been prepared for write.
	idleWriteList list.List
	// mayGetSession is for broadcasting that session retrival/creation may proceed.
	mayGetSession chan struct{}
	// numOpened is the total number of open sessions from the session pool.
	numOpened uint64
	// createReqs is the number of ongoing session creation requests.
	createReqs uint64
	// prepareReqs is the number of ongoing session preparation request.
	prepareReqs uint64
	// configuration of the session pool.
	Config
	// hc is the health checker
	hc *healthChecker
}

// New creates a new session pool.
func New(config Config) (*Pool, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	pool := &Pool{
		valid:         true,
		mayGetSession: make(chan struct{}),
		Config:        config,
	}
	if config.HealthCheckInterval == 0 {
		config.HealthCheckInterval = 5 * time.Minute
	}
	if config.HealthCheckSampleInterval == 0 {
		config.HealthCheckSampleInterval = time.Minute
	}
	// On GCE VM, within the same region an healthcheck ping takes on average 10ms to finish, given a 5 minutes interval and
	// 10 healthcheck workers, a healthChecker can effectively mantain 100 checks_per_worker/sec * 10 workers * 300 seconds = 300K sessions.
	pool.hc = newHealthChecker(config.HealthCheckInterval, config.HealthCheckWorkers, config.HealthCheckSampleInterval, pool)
	close(pool.hc.ready)
	return pool, nil
}

// isValid checks if the session pool is still valid.
func (p *Pool) isValid() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.valid
}

// close marks the session pool as closed.
func (p *Pool) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if !p.valid {
		p.mu.Unlock()
		return
	}
	p.valid = false
	p.mu.Unlock()
	p.hc.close()
	// destroy all the sessions
	p.hc.mu.Lock()
	allSessions := make([]*session, len(p.hc.queue.sessions))
	copy(allSessions, p.hc.queue.sessions)
	p.hc.mu.Unlock()
	for _, s := range allSessions {
		s.destroy(false)
	}
}

// errInvalidSessionPool returns error for using an invalid session pool.
func errInvalidSessionPool() error {
	return spannerErrorf(codes.InvalidArgument, "invalid session pool")
}

// errGetSessionTimeout returns error for context timeout during Pool.take().
func errGetSessionTimeout() error {
	return spannerErrorf(codes.Canceled, "timeout / context canceled during getting session")
}

// shouldPrepareWrite returns true if we should prepare more sessions for write.
func (p *Pool) shouldPrepareWrite() bool {
	return float64(p.numOpened)*p.WriteSessions > float64(p.idleWriteList.Len()+int(p.prepareReqs))
}

func (p *Pool) createSession(ctx context.Context) (*session, error) {
	statsPrintf(ctx, nil, "Creating a new session")
	doneCreate := func(done bool) {
		p.mu.Lock()
		if !done {
			// Session creation failed, give budget back.
			p.numOpened--
			recordStat(ctx, OpenSessionCount, int64(p.numOpened))
		}
		p.createReqs--
		// Notify other waiters blocking on session creation.
		close(p.mayGetSession)
		p.mayGetSession = make(chan struct{})
		p.mu.Unlock()
	}
	sc, err := p.createSession()
	if err != nil {
		doneCreate(false)
		return nil, err
	}
	s, err := createSession(ctx, sc, p.db, p.Labels, p.md)
	if err != nil {
		doneCreate(false)
		// Should return error directly because of the previous retries on CreateResource RPC.
		return nil, err
	}
	s.pool = p
	p.hc.register(s)
	doneCreate(true)
	return s, nil
}

func createSession(ctx context.Context, sc sppb.SpannerClient, db string, labels map[string]string, md metadata.MD) (*session, error) {
	var s *session
	err := runRetryable(ctx, func(ctx context.Context) error {
		sid, e := sc.CreateSession(ctx, &sppb.CreateSessionRequest{
			Database: db,
			Session:  &sppb.Session{Labels: labels},
		})
		if e != nil {
			return e
		}
		// If no error, construct the new session.
		s = &session{valid: true, res: sc, id: sid.Name, createTime: time.Now(), md: md}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (p *Pool) isHealthy(s *session) bool {
	if s.getNextCheck().Add(2 * p.hc.getInterval()).Before(time.Now()) {
		// TODO: figure out if we need to schedule a new healthcheck worker here.
		if err := s.ping(); shouldDropSession(err) {
			// The session is already bad, continue to fetch/create a new one.
			s.destroy(false)
			return false
		}
		p.hc.scheduledHC(s)
	}
	return true
}

// take returns a cached session if there are available ones; if there isn't any, it tries to allocate a new one.
// Session returned by take should be used for read operations.
func (p *Pool) take(ctx context.Context) (*sessionHandle, error) {
	statsPrintf(ctx, nil, "Acquiring a read-only session")
	ctx = contextWithOutgoingMetadata(ctx, p.md)
	for {
		var (
			s   *session
			err error
		)

		p.mu.Lock()
		if !p.valid {
			p.mu.Unlock()
			return nil, errInvalidSessionPool()
		}
		if p.idleList.Len() > 0 {
			// Idle sessions are available, get one from the top of the idle list.
			s = p.idleList.Remove(p.idleList.Front()).(*session)
			statsPrintf(ctx, map[string]interface{}{"sessionID": s.getID()},
				"Acquired read-only session")
		} else if p.idleWriteList.Len() > 0 {
			s = p.idleWriteList.Remove(p.idleWriteList.Front()).(*session)
			statsPrintf(ctx, map[string]interface{}{"sessionID": s.getID()},
				"Acquired read-write session")
		}
		if s != nil {
			s.setIdleList(nil)
			p.mu.Unlock()
			// From here, session is no longer in idle list, so healthcheck workers won't destroy it.
			// If healthcheck workers failed to schedule healthcheck for the session timely, do the check here.
			// Because session check is still much cheaper than session creation, they should be reused as much as possible.
			if !p.isHealthy(s) {
				continue
			}
			return &sessionHandle{session: s}, nil
		}
		// Idle list is empty, block if session pool has reached max session creation concurrency or max number of open sessions.
		if (p.MaxOpened > 0 && p.numOpened >= p.MaxOpened) || (p.MaxBurst > 0 && p.createReqs >= p.MaxBurst) {
			mayGetSession := p.mayGetSession
			p.mu.Unlock()
			statsPrintf(ctx, nil, "Waiting for read-only session to become available")
			select {
			case <-ctx.Done():
				statsPrintf(ctx, nil, "Context done waiting for session")
				return nil, errGetSessionTimeout()
			case <-mayGetSession:
			}
			continue
		}
		// Take budget before the actual session creation.
		p.numOpened++
		recordStat(ctx, OpenSessionCount, int64(p.numOpened))
		p.createReqs++
		p.mu.Unlock()
		if s, err = p.createSession(ctx); err != nil {
			statsPrintf(ctx, nil, "Error creating session: %v", err)
			return nil, toSpannerError(err)
		}
		statsPrintf(ctx, map[string]interface{}{"sessionID": s.getID()},
			"Created session")
		return &sessionHandle{session: s}, nil
	}
}

// takeWriteSession returns a write prepared cached session if there are available ones; if there isn't any, it tries to allocate a new one.
// Session returned should be used for read write transactions.
func (p *Pool) takeWriteSession(ctx context.Context) (*sessionHandle, error) {
	statsPrintf(ctx, nil, "Acquiring a read-write session")
	ctx = contextWithOutgoingMetadata(ctx, p.md)
	for {
		var (
			s   *session
			err error
		)

		p.mu.Lock()
		if !p.valid {
			p.mu.Unlock()
			return nil, errInvalidSessionPool()
		}
		if p.idleWriteList.Len() > 0 {
			// Idle sessions are available, get one from the top of the idle list.
			s = p.idleWriteList.Remove(p.idleWriteList.Front()).(*session)
			statsPrintf(ctx, map[string]interface{}{"sessionID": s.getID()}, "Acquired read-write session")
		} else if p.idleList.Len() > 0 {
			s = p.idleList.Remove(p.idleList.Front()).(*session)
			statsPrintf(ctx, map[string]interface{}{"sessionID": s.getID()}, "Acquired read-only session")
		}
		if s != nil {
			s.setIdleList(nil)
			p.mu.Unlock()
			// From here, session is no longer in idle list, so healthcheck workers won't destroy it.
			// If healthcheck workers failed to schedule healthcheck for the session timely, do the check here.
			// Because session check is still much cheaper than session creation, they should be reused as much as possible.
			if !p.isHealthy(s) {
				continue
			}
		} else {
			// Idle list is empty, block if session pool has reached max session creation concurrency or max number of open sessions.
			if (p.MaxOpened > 0 && p.numOpened >= p.MaxOpened) || (p.MaxBurst > 0 && p.createReqs >= p.MaxBurst) {
				mayGetSession := p.mayGetSession
				p.mu.Unlock()
				statsPrintf(ctx, nil, "Waiting for read-write session to become available")
				select {
				case <-ctx.Done():
					statsPrintf(ctx, nil, "Context done waiting for session")
					return nil, errGetSessionTimeout()
				case <-mayGetSession:
				}
				continue
			}

			// Take budget before the actual session creation.
			p.numOpened++
			recordStat(ctx, OpenSessionCount, int64(p.numOpened))
			p.createReqs++
			p.mu.Unlock()
			if s, err = p.createSession(ctx); err != nil {
				statsPrintf(ctx, nil, "Error creating session: %v", err)
				return nil, toSpannerError(err)
			}
			statsPrintf(ctx, map[string]interface{}{"sessionID": s.getID()},
				"Created session")
		}
		if !s.isWritePrepared() {
			if err = s.prepareForWrite(ctx); err != nil {
				s.recycle()
				statsPrintf(ctx, map[string]interface{}{"sessionID": s.getID()},
					"Error preparing session for write")
				return nil, toSpannerError(err)
			}
		}
		return &sessionHandle{session: s}, nil
	}
}

// recycle puts session s back to the session pool's idle list, it returns true if the session pool successfully recycles session s.
func (p *Pool) recycle(s *session) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !s.isValid() || !p.valid {
		// Reject the session if session is invalid or pool itself is invalid.
		return false
	}
	// Put session at the back of the list to round robin for load balancing across channels.
	if s.isWritePrepared() {
		s.setIdleList(p.idleWriteList.PushBack(s))
	} else {
		s.setIdleList(p.idleList.PushBack(s))
	}
	// Broadcast that a session has been returned to idle list.
	close(p.mayGetSession)
	p.mayGetSession = make(chan struct{})
	return true
}

// remove atomically removes session s from the session pool and invalidates s.
// If isExpire == true, the removal is triggered by session expiration and in such cases, only idle sessions can be removed.
func (p *Pool) remove(s *session, isExpire bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if isExpire && (p.numOpened <= p.MinOpened || s.getIdleList() == nil) {
		// Don't expire session if the session is not in idle list (in use), or if number of open sessions is going below p.MinOpened.
		return false
	}
	ol := s.setIdleList(nil)
	// If the session is in the idlelist, remove it.
	if ol != nil {
		// Remove from whichever list it is in.
		p.idleList.Remove(ol)
		p.idleWriteList.Remove(ol)
	}
	if s.invalidate() {
		// Decrease the number of opened sessions.
		p.numOpened--
		recordStat(context.Background(), OpenSessionCount, int64(p.numOpened))
		// Broadcast that a session has been destroyed.
		close(p.mayGetSession)
		p.mayGetSession = make(chan struct{})
		return true
	}
	return false
}
