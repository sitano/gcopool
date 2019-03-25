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
	"sync"
)

// sessionHandle is an interface for transactions to access Cloud Spanner sessions safely. It is generated by Pool.take().
type sessionHandle struct {
	// mu guarantees that the inner session object is returned / destroyed only once.
	mu sync.Mutex
	// session is a pointer to a session object. Transactions never need to access it directly.
	session *session
}

// recycle gives the inner session object back to its home session pool. It is safe to call recycle multiple times but only the first one would take effect.
func (sh *sessionHandle) recycle() {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.session == nil {
		// sessionHandle has already been recycled.
		return
	}
	sh.session.recycle()
	sh.session = nil
}

// getID gets the Cloud Spanner session ID from the internal session object. getID returns empty string if the sessionHandle is nil or the inner session
// object has been released by recycle / destroy.
func (sh *sessionHandle) getID() string {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.session == nil {
		// sessionHandle has already been recycled/destroyed.
		return ""
	}
	return sh.session.getID()
}

// getClient gets the Cloud Spanner RPC resource associated with the session ID in sessionHandle.
func (sh *sessionHandle) getClient() Resource {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.session == nil {
		return nil
	}
	return sh.session.res
}

// getTransactionID returns the transaction id in the session if available.
func (sh *sessionHandle) getTransactionID() TXID {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.session == nil {
		var def TXID
		return def
	}
	return sh.session.tx
}

// destroy destroys the inner session object. It is safe to call destroy multiple times and only the first call would attempt to
// destroy the inner session object.
func (sh *sessionHandle) destroy() {
	sh.mu.Lock()
	s := sh.session
	sh.session = nil
	sh.mu.Unlock()
	if s == nil {
		// sessionHandle has already been destroyed.
		return
	}
	s.destroy(false)
}
