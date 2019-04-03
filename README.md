# gcopool [![Travis-CI](https://travis-ci.org/sitano/gcopool.svg)](https://travis-ci.org/sitano/gcopool) [![AppVeyor](https://ci.appveyor.com/api/projects/status/b98mptawhudj53ep/branch/master?svg=true)](https://ci.appveyor.com/project/sitano/gcopool/branch/master) [![GoDoc](https://godoc.org/github.com/sitano/gcopool?status.svg)](http://godoc.org/github.com/sitano/gcopool) [![Report card](https://goreportcard.com/badge/github.com/sitano/gcopool)](https://goreportcard.com/report/github.com/sitano/gcopool) [![Sourcegraph](https://sourcegraph.com/github.com/sitano/gcopool/-/badge.svg)](https://sourcegraph.com/github.com/sitano/gcopool?badge)

Connection pool derived from the Google's Spanner client.
It can manage resources of any kind, supports health checks
and support tracing.

## Quick start

Implement resource interface is everything you need to start:

```go
type Resource interface {
	ID() string
}
```

and go with:

```
pool, err := New(Config{
    CreateResource: createResource,
    MaxOpened: 10,
})
handle, err := pool.Take(context.Background())
doJob(handle.GetResource())
handle.Recycle()
pool.Close()
```

## Resource

Resource is an entity that is managed by the pool. The pool
supports health checking. The resource can be of 2 types:
read-only something (transaction) resource and read-write
something (transaction) resource.

```go
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

const (
	EventAcquire = EventCode(1) // must NOT do blocking IO
	EventRelease = EventCode(2) // must NOT do blocking IO
	EventDestroy = EventCode(3) // have context with a deadline
)
```