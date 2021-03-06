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

- 2019, @john.koepi/@sitano extract pool
*/

package gcopool

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

const pingTimeout = time.Second
const destroyTimeout = 15 * time.Second

// session wraps a resource session.
type session struct {
	// resource is something that is managed by this session. It is set only once during session's creation.
	res Resource
	// id is the unique id of the session. It is set only once during session's creation.
	id string
	// pool is the session's home session pool where it was created. It is set only once during session's creation.
	pool *Pool
	// createTime is the timestamp of the session's creation. It is set only once during session's creation.
	createTime time.Time

	// mu protects the following fields from concurrent access: both healthcheck workers and transactions can modify them.
	mu sync.Mutex
	// valid marks the validity of a session.
	valid bool
	// hcIndex is the index of the session inside the global healthcheck queue. If hcIndex < 0, session has been unregistered from the queue.
	hcIndex int
	// idleList is the linkedlist node which links the session to its home session pool's idle list. If idleList == nil, the
	// session is not in idle list.
	idleList *list.Element
	// nextCheck is the timestamp of next scheduled healthcheck of the session. It is maintained by the global health checker.
	nextCheck time.Time
	// checkingHealth is true if currently this session is being processed by health checker. Must be modified under health checker lock.
	checkingHealth bool
}

// IsValid returns true if the session is still valid for use.
func (s *session) isValid() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.valid
}

// isWritePrepared returns true if the session is prepared for write.
func (s *session) isWritePrepared() bool {
	if tx, ok := s.res.(TX); ok {
		return tx.Prepared()
	}
	return false
}

// String implements fmt.Stringer for session.
func (s *session) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("<id=%v, hcIdx=%v, idleList=%p, valid=%v, create=%v, nextcheck=%v>",
		s.id, s.hcIndex, s.idleList, s.valid, s.createTime, s.nextCheck)
}

// ping verifies if the session is still alive.
func (s *session) ping() error {
	p, ok := s.res.(Heartbeat)
	if ok {
		ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
		defer cancel()
		return RunRetryable(ctx, func(ctx context.Context) error {
			return p.Ping(ctx)
		})
	}
	return nil
}

// setHCIndex atomically sets the session's index in the healthcheck queue and returns the old index.
func (s *session) setHCIndex(i int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	oi := s.hcIndex
	s.hcIndex = i
	return oi
}

// setIdleList atomically sets the session's idle list link and returns the old link.
func (s *session) setIdleList(le *list.Element) *list.Element {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.idleList
	s.idleList = le
	return old
}

// invalidate marks a session as invalid and returns the old validity.
func (s *session) invalidate() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ov := s.valid
	s.valid = false
	return ov
}

// setNextCheck sets the timestamp for next healthcheck on the session.
func (s *session) setNextCheck(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextCheck = t
}

// GetID returns the session ID which uniquely identifies the session.
func (s *session) getID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.id
}

// getHCIndex returns the session's index into the global health check priority queue.
func (s *session) getHCIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hcIndex
}

// getIdleList returns the session's link in its home session pool's idle list.
func (s *session) getIdleList() *list.Element {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.idleList
}

// getNextCheck returns the timestamp for next health check on the session.
func (s *session) getNextCheck() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextCheck
}

// recycle turns the session back to its home session pool.
// if commit is true recycle tries to commit a tx if it is active
// or abort otherwise. if commit or abort fail it destroys a session.
func (s *session) recycle(commit bool) {
	_ = s.signalEvent(context.Background(), EventRelease, nil)

	tx, ok := s.res.(TX)
	if ok {
		if err := RunRetryable(context.Background(), func(ctx context.Context) error {
			if !commit {
				return tx.Abort(ctx)
			}
			return tx.Commit(ctx)
		}); err != nil {
			s.destroy(false)
			return
		}
	}

	if !s.pool.recycle(s) {
		// s is rejected by its home session pool because it expired and the session pool currently has enough open sessions.
		s.destroy(false)
	}
}

// destroy removes the session from its home session pool, health check queue and service.
func (s *session) destroy(isExpire bool) bool {
	// Remove s from session pool.
	if !s.pool.remove(s, isExpire) {
		return false
	}
	// Unregister s from health check queue.
	s.pool.hc.unregister(s)
	// Remove s from service.
	ctx, cancel := context.WithTimeout(context.Background(), destroyTimeout)
	defer cancel()
	s.delete(ctx)
	return true
}

func (s *session) delete(ctx context.Context) {
	// Ignore the error returned by RunRetryable because even if we fail to explicitly Destroy the session,
	// it will be eventually garbage collected.
	err := s.processEvent(ctx, EventDestroy, nil)
	if err != nil {
		log.Printf("Failed to delete session %v. Error: %v", s.getID(), err)
	}
}

// prepareForWrite prepares the session for write if it is not already in that state.
func (s *session) prepareForWrite(ctx context.Context) error {
	if s.isWritePrepared() {
		return nil
	}
	tx, ok := s.res.(TX)
	if !ok {
		return ErrTXUnsupported
	}
	return tx.Prepare(ctx)
}

// beginTransaction prepares the session for write if it is not already in that state.
func (s *session) beginTransaction(ctx context.Context) error {
	tx, ok := s.res.(TX)
	if !ok {
		return ErrTXUnsupported
	}
	return tx.Begin(ctx)
}

func (s *session) signalEvent(ctx context.Context, code EventCode, obj interface{}) error {
	if h, ok := s.res.(Hook); ok {
		return h.Handle(ctx, code, obj)
	}
	return nil
}

func (s *session) processEvent(ctx context.Context, code EventCode, obj interface{}) error {
	return RunRetryable(ctx, func(ctx context.Context) error {
		if h, ok := s.res.(Hook); ok {
			return h.Handle(ctx, code, obj)
		}
		return nil
	})
}
