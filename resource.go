/*
Copyright 2019 Ivan Prisyazhnyy <john.koepi@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package gcopool

import (
	"context"
	"errors"
)

type Resource interface {
	// Unique immutable resource id per pool
	ID() string
}

type Heartbeat interface {
	// Ping verifies if the resource is still alive.
	Ping(context.Context) error
}

type TX interface {
	Prepared() bool
	// Prepare called only once when transitioning from read-only to read-write mode
	Prepare(context.Context) error

	Begin(context.Context) error
	Commit(context.Context) error
	Abort(context.Context) error
}

type Hook interface {
	Handle(ctx context.Context, event EventCode, object interface{}) error
}

// ErrUnavailable is an error which can be returned by the ping
// implementation to report that the resource is unavailable
// any more and it must be removed from the pool.
var ErrUnavailable = errors.New("session is unavailable")
var ErrTXUnsupported = errors.New("read-write mode is unsupported")

type EventCode int

const (
	EventAcquire = EventCode(1) // must NOT do blocking IO
	EventRelease = EventCode(2) // must NOT do blocking IO
	EventDestroy = EventCode(3) // have context with a deadline
)
