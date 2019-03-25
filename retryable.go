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
	"context"
	"errors"
	"time"
)

var ErrRetryable = errors.New("retry")

// RunRetryable keeps attempting to run f until one of the following happens:
//     1) f returns nil error or an unretryable error;
//     2) context is cancelled or timeout.
// TODO: consider using https://github.com/googleapis/gax-go/v2 once it
// becomes available internally.
func RunRetryable(ctx context.Context, f func(context.Context) error) error {
	retryCount := 0
	for {
		select {
		case <-ctx.Done():
			// Do context check here so that even f() failed to do
			// so (for example, gRPC implementation bug), the loop
			// can still have a chance to exit as expected.
			return errContextCanceled(ctx)
		default:
		}
		err := f(ctx)
		if err == nil {
			return nil
		}
		if isRetryable(err) {
			// Error is retryable, do exponential backoff and continue.
			b, ok := extractRetryDelay(err)
			if !ok {
				b = DefaultBackoff.Delay(retryCount)
			}
			statsPrintf(ctx, nil, "Backing off for %s, then retrying", b)
			select {
			case <-ctx.Done():
				return errContextCanceled(ctx)
			case <-time.After(b):
			}
			retryCount++
			continue
		}
		// Error isn't retryable / no error, return immediately.
		return err
	}
}

// errContextCanceled returns *spanner.Error for canceled context.
func errContextCanceled(ctx context.Context) error {
	if ctx.Err() == context.DeadlineExceeded {
		return context.DeadlineExceeded
	}
	return context.Canceled
}

// isRetryable returns true if the error being checked is a retryable error.
func isRetryable(err error) bool {
	return err == ErrRetryable
}

// extractRetryDelay extracts retry backoff if present.
func extractRetryDelay(_ error) (time.Duration, bool) {
	return 0, false
}

