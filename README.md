# gcopool [![Travis-CI](https://travis-ci.org/sitano/gcopool.svg)](https://travis-ci.org/sitano/gcopool) [![AppVeyor](https://ci.appveyor.com/api/projects/status/b98mptawhudj53ep/branch/master?svg=true)](https://ci.appveyor.com/project/sitano/gcopool/branch/master) [![GoDoc](https://godoc.org/github.com/sitano/gcopool?status.svg)](http://godoc.org/github.com/sitano/gcopool) [![Report card](https://goreportcard.com/badge/github.com/sitano/gcopool)](https://goreportcard.com/report/github.com/sitano/gcopool) [![Sourcegraph](https://sourcegraph.com/github.com/sitano/gcopool/-/badge.svg)](https://sourcegraph.com/github.com/sitano/gcopool?badge)

Connection pool derived from the Google's Spanner client.
It can manage resources of any kind, supports health checks
and support tracing.

## Quick start

Implement resource interface is everything you need to start:

```go
type Resource interface {
	ID() string
	Ping(context.Context) error
	Prepare(context.Context) (TXID, error)
	Destroy(context.Context) error
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