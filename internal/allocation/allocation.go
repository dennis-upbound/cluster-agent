/*
Copyright 2026 The Modelplane Authors.

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

// Package allocation records when the agent first saw each ResourceClaim
// allocated.
//
// The agent publishes a pool's free devices and a replica's devices
// separately, so a reader needs to know whether a pool's free devices account
// for a replica's. Each publishes the latest time the agent first saw one of
// its claims allocated. A Record assigns those times so that a pool's time no
// earlier than a replica's means the pool's free devices account for the
// replica's, unless the replica's claims have since been deallocated.
package allocation

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"

	"github.com/modelplaneai/cluster-agent/apis/v1alpha1"
	"github.com/modelplaneai/cluster-agent/internal/dra"
)

// A Record records when the agent first saw each ResourceClaim allocated.
//
// It reads claims from the agent's cache and assigns their times while holding
// one lock, and each time it assigns is later than every time it assigned
// before. The cache only moves forward, so a read that includes a claim the
// Record assigned some time includes every claim it assigned an earlier time,
// unless that claim has since been deallocated or deleted.
//
// Only one Record may assign times. Two would each order times against their
// own reads, so one could give a claim a time no later than one the other
// published for a pool whose free devices don't account for the claim.
type Record struct {
	mu     sync.Mutex
	now    func() time.Time
	floor  func(ctx context.Context) (time.Time, error)
	seeded bool
	last   time.Time
	claims map[types.UID]firstSeen
}

// firstSeen is the devices a claim allocated, and when the Record first saw
// it allocate them.
type firstSeen struct {
	devices []dra.DeviceID
	at      time.Time
}

// An Option configures a Record.
type Option func(*Record)

// WithClock sets the Record's clock. It uses time.Now by default.
func WithClock(now func() time.Time) Option {
	return func(r *Record) { r.now = now }
}

// NewRecord returns a Record that assigns times later than the one the
// supplied function returns. The function must return the latest time any
// agent has published, so an agent that replaces another, perhaps with a clock
// that's behind, can't assign a claim an earlier time than one already
// published. The Record calls it before it first reads claims, and again on
// its next read if it fails.
func NewRecord(floor func(ctx context.Context) (time.Time, error), opts ...Option) *Record {
	r := &Record{now: time.Now, floor: floor, claims: map[types.UID]firstSeen{}}
	for _, fn := range opts {
		fn(r)
	}
	return r
}

// Read calls the supplied function, which must read ResourceClaims from the
// agent's cache, and returns when the Record first saw each allocated claim it
// read, keyed by UID. The function runs while the Record holds its lock, so no
// other read can be assigned times between this read and the times assigned
// to it. A claim that allocates different devices than when the Record last
// saw it is seen allocated afresh, because a pool's free devices might not
// account for its new ones.
func (r *Record) Read(ctx context.Context, read func() ([]resourcev1.ResourceClaim, error)) (map[types.UID]time.Time, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.seeded {
		floor, err := r.floor(ctx)
		if err != nil {
			return nil, fmt.Errorf("cannot get the latest published allocation time: %w", err)
		}
		r.last = floor
		r.seeded = true
	}

	claims, err := read()
	if err != nil {
		return nil, err
	}

	at := make(map[types.UID]time.Time, len(claims))
	for i := range claims {
		c := &claims[i]
		if c.Status.Allocation == nil {
			delete(r.claims, c.UID)
			continue
		}
		devices := dra.Allocations(c)
		seen, ok := r.claims[c.UID]
		if !ok || !slices.Equal(seen.devices, devices) {
			// Each time must be later than the last even once it's published
			// to the microsecond.
			t := r.now().UTC().Truncate(time.Microsecond)
			if !t.After(r.last) {
				t = r.last.Add(time.Microsecond)
			}
			r.last = t
			seen = firstSeen{devices: devices, at: t}
			r.claims[c.UID] = seen
		}
		at[c.UID] = seen.at
	}
	return at, nil
}

// Forget forgets the claim with the supplied UID.
func (r *Record) Forget(uid types.UID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.claims, uid)
}

// ForgetDeleted returns an event handler for a ResourceClaim informer that
// makes the supplied Record forget each claim that's deleted.
func ForgetDeleted(r *Record) toolscache.ResourceEventHandlerFuncs {
	return toolscache.ResourceEventHandlerFuncs{
		// An informer that relists after missing a claim's deletion, and finds
		// a new claim of the same name, reports an update rather than a
		// deletion.
		UpdateFunc: func(old, obj any) {
			o, ok := old.(*resourcev1.ResourceClaim)
			if !ok {
				return
			}
			if c, ok := obj.(*resourcev1.ResourceClaim); ok && c.GetUID() != o.GetUID() {
				r.Forget(o.GetUID())
			}
		},
		DeleteFunc: func(obj any) {
			// An informer that relists after missing a claim's deletion
			// reports a tombstone.
			if t, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
				obj = t.Obj
			}
			if c, ok := obj.(*resourcev1.ResourceClaim); ok {
				r.Forget(c.GetUID())
			}
		},
	}
}

// LatestPublished returns the latest allocation time the supplied
// PoolObservations and ReplicaObservations publish.
func LatestPublished(pools []v1alpha1.PoolObservation, replicas []v1alpha1.ReplicaObservation) time.Time {
	var latest time.Time
	for _, po := range pools {
		if t := po.Status.LastAllocationTime; t != nil && t.After(latest) {
			latest = t.Time
		}
	}
	for _, ro := range replicas {
		for _, p := range ro.Status.Pools {
			if p.LastAllocationTime.After(latest) {
				latest = p.LastAllocationTime.Time
			}
		}
	}
	return latest
}
