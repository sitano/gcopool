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

package spanner

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

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
	// checkingHelath is true if currently this session is being processed by health checker. Must be modified under health checker lock.
	checkingHealth bool
	// tx is true if the session has been prepared for write.
	tx bool
}

// isValid returns true if the session is still valid for use.
func (s *session) isValid() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.valid
}

// isWritePrepared returns true if the session is prepared for write.
func (s *session) isWritePrepared() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tx
}

// String implements fmt.Stringer for session.
func (s *session) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("<id=%v, hcIdx=%v, idleList=%p, valid=%v, create=%v, nextcheck=%v>",
		s.id, s.hcIndex, s.idleList, s.valid, s.createTime, s.nextCheck)
}

// ping verifies if the session is still alive in Cloud Spanner.
func (s *session) ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return runRetryable(ctx, func(ctx context.Context) error {
		_, err := s.res.GetSession(contextWithOutgoingMetadata(ctx, s.pool.md), &sppb.GetSessionRequest{Name: s.getID()}) // s.getID is safe even when s is invalid.
		return err
	})
}

// setHcIndex atomically sets the session's index in the healthcheck queue and returns the old index.
func (s *session) setHcIndex(i int) int {
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

// setTransaction sets the transaction status (read / write) in the session
func (s *session) setTransactionID(tx bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tx = tx
}

// getID returns the session ID which uniquely identifies the session in Cloud Spanner.
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
func (s *session) recycle() {
	s.setTransactionID(false)
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
	// Unregister s from healthcheck queue.
	s.pool.hc.unregister(s)
	// Remove s from Cloud Spanner service.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s.delete(ctx)
	return true
}

func (s *session) delete(ctx context.Context) {
	// Ignore the error returned by runRetryable because even if we fail to explicitly destroy the session,
	// it will be eventually garbage collected by Cloud Spanner.
	err := runRetryable(ctx, func(ctx context.Context) error {
		_, e := s.res.DeleteSession(ctx, &sppb.DeleteSessionRequest{Name: s.getID()})
		return e
	})
	if err != nil {
		log.Printf("Failed to delete session %v. Error: %v", s.getID(), err)
	}
}

// prepareForWrite prepares the session for write if it is not already in that state.
func (s *session) prepareForWrite(ctx context.Context) error {
	if s.isWritePrepared() {
		return nil
	}
	tx, err := beginTransaction(ctx, s.getID(), s.res)
	if err != nil {
		return err
	}
	s.setTransactionID(tx)
	return nil
}
