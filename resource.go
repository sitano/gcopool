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

import "context"

type TXID bool

type Resource interface {
	// Unique immutable id per pool
	ID() string

	// Ping verifies if the resource(ID) is still alive.
	Ping(context.Context, string) error

	// Prepare prepare resource(ID) for write
	Prepare(context.Context, string) (TXID, error)

	// Destroy releases a resource
	Destroy(context.Context, string) error
}
