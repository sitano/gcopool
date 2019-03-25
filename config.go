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

- 2019, @john.koepi/@sitano extract pool, change config and errors
*/

package gcopool

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Config stores configurations of a session pool.
type Config struct {
	// CreateResource is the caller supplied method for getting a session, this makes session pool able to use pooling.
	CreateResource func(context.Context) (Resource, error)
	// MaxOpened is the maximum number of opened sessions allowed by the session
	// pool. Defaults to NumChannels * 100. If the resource tries to open a session and
	// there are already MaxOpened sessions, it will block until one becomes
	// available or the context passed to the resource method is canceled or times out.
	MaxOpened uint64
	// MinOpened is the minimum number of opened sessions that the session pool
	// tries to maintain. Session pool won't continue to expire sessions if number
	// of opened connections drops below MinOpened. However, if a session is found
	// to be broken, it will still be evicted from the session pool, therefore it is
	// posssible that the number of opened sessions drops below MinOpened.
	MinOpened uint64
	// MaxIdle is the maximum number of idle sessions, pool is allowed to keep. Defaults to 0.
	MaxIdle uint64
	// MaxBurst is the maximum number of concurrent session creation requests. Defaults to 10.
	MaxBurst uint64
	// WriteSessions is the fraction of sessions we try to keep prepared for write.
	WriteSessions float64
	// HealthCheckWorkers is number of workers used by health checker for this pool.
	HealthCheckWorkers int
	// HealthCheckInterval is how often the health checker pings a session. Defaults to 5 min.
	HealthCheckInterval time.Duration
	// HealthCheckSampleInterval is how often the health checker samples live session (for use in maintaining session pool size). Defaults to 1 min.
	HealthCheckSampleInterval time.Duration
	// Labels for the sessions created in the session pool.
	Labels map[string]string
}

// Validate verifies that the Config is good for use.
func (spc *Config) Validate() error {
	if spc.CreateResource == nil {
		return errNoSessionGetter()
	}
	if spc.MinOpened > spc.MaxOpened && spc.MaxOpened > 0 {
		return errMinOpenedGTMaxOpened(spc.MaxOpened, spc.MinOpened)
	}
	return nil
}

// errNoSessionGetter returns error for Config missing CreateResource method.
func errNoSessionGetter() error {
	return errors.New("require CreateResource configured")
}

// errMinOpenedGTMapOpened returns error for Config.MaxOpened < Config.MinOpened when Config.MaxOpened is set.
func errMinOpenedGTMaxOpened(maxOpened, minOpened uint64) error {
	return fmt.Errorf("require Config.MaxOpened >= Config.MinOpened, got %d and %d", maxOpened, minOpened)
}
