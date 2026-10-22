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

package allocation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"

	"github.com/modelplaneai/cluster-agent/apis/v1alpha1"
)

var errBoom = errors.New("boom")

// noFloor reports that no agent has published an allocation time.
func noFloor(_ context.Context) (time.Time, error) { return time.Time{}, nil }

// claim returns a ResourceClaim with the supplied UID that has allocated the
// supplied GPUs, or that isn't allocated if it supplies none.
func claim(uid types.UID, gpus ...string) resourcev1.ResourceClaim {
	c := resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{UID: uid}}
	if len(gpus) == 0 {
		return c
	}
	c.Status.Allocation = &resourcev1.AllocationResult{}
	for _, gpu := range gpus {
		c.Status.Allocation.Devices.Results = append(c.Status.Allocation.Devices.Results, resourcev1.DeviceRequestAllocationResult{
			Request: "gpu", Driver: "gpu.nvidia.com", Pool: "node-a", Device: gpu,
		})
	}
	return c
}

// ticks returns a clock that returns each of the supplied times in turn.
func ticks(times ...time.Time) func() time.Time {
	return func() time.Time {
		t := times[0]
		times = times[1:]
		return t
	}
}

func TestRead(t *testing.T) {
	start := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)

	// A step either reads the supplied claims, or forgets a claim.
	type step struct {
		forget  types.UID
		claims  []resourcev1.ResourceClaim
		readErr error
	}
	type result struct {
		at  map[types.UID]time.Time
		err error
	}

	// What the floor function returns each time the Record calls it.
	type floor struct {
		at  time.Time
		err error
	}

	cases := map[string]struct {
		reason string
		floors []floor
		clock  []time.Time
		steps  []step
		want   []result
	}{
		"FirstSeen": {
			reason: "Each allocated claim should get the time the Record first read it, and an unallocated one none.",
			floors: []floor{{}},
			clock:  []time.Time{start, start.Add(time.Second)},
			steps: []step{
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-0"), claim("b", "gpu-1"), claim("c")}},
			},
			want: []result{
				{at: map[types.UID]time.Time{"a": start, "b": start.Add(time.Second)}},
			},
		},
		"SeenBefore": {
			reason: "A claim read again with the same devices should keep the time it was first read.",
			floors: []floor{{}},
			clock:  []time.Time{start, start.Add(time.Second)},
			steps: []step{
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-0")}},
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-0"), claim("b", "gpu-1")}},
			},
			want: []result{
				{at: map[types.UID]time.Time{"a": start}},
				{at: map[types.UID]time.Time{"a": start, "b": start.Add(time.Second)}},
			},
		},
		"Reallocated": {
			reason: "A claim read with different devices than before should get a new time.",
			floors: []floor{{}},
			clock:  []time.Time{start, start.Add(time.Second)},
			steps: []step{
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-0")}},
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-1")}},
			},
			want: []result{
				{at: map[types.UID]time.Time{"a": start}},
				{at: map[types.UID]time.Time{"a": start.Add(time.Second)}},
			},
		},
		"Deallocated": {
			reason: "A claim read unallocated should get a new time when it's next read allocated.",
			floors: []floor{{}},
			clock:  []time.Time{start, start.Add(time.Second)},
			steps: []step{
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-0")}},
				{claims: []resourcev1.ResourceClaim{claim("a")}},
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-0")}},
			},
			want: []result{
				{at: map[types.UID]time.Time{"a": start}},
				{at: map[types.UID]time.Time{}},
				{at: map[types.UID]time.Time{"a": start.Add(time.Second)}},
			},
		},
		"Forgotten": {
			reason: "A forgotten claim should get a new time when it's next read.",
			floors: []floor{{}},
			clock:  []time.Time{start, start.Add(time.Second)},
			steps: []step{
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-0")}},
				{forget: "a"},
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-0")}},
			},
			want: []result{
				{at: map[types.UID]time.Time{"a": start}},
				{},
				{at: map[types.UID]time.Time{"a": start.Add(time.Second)}},
			},
		},
		"ClockBehind": {
			reason: "A Record whose clock is behind the latest published time should assign the microseconds after it, reading that time only once.",
			floors: []floor{{at: start.Add(time.Hour)}},
			clock:  []time.Time{start, start, start},
			steps: []step{
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-0"), claim("b", "gpu-1")}},
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-0"), claim("b", "gpu-1"), claim("c", "gpu-2")}},
			},
			want: []result{
				{at: map[types.UID]time.Time{"a": start.Add(time.Hour + time.Microsecond), "b": start.Add(time.Hour + 2*time.Microsecond)}},
				{at: map[types.UID]time.Time{"a": start.Add(time.Hour + time.Microsecond), "b": start.Add(time.Hour + 2*time.Microsecond), "c": start.Add(time.Hour + 3*time.Microsecond)}},
			},
		},
		"FloorError": {
			reason: "An error getting the latest published time should be returned, and the Record should try again on its next read.",
			floors: []floor{{err: errBoom}, {at: start.Add(time.Hour)}},
			clock:  []time.Time{start},
			steps: []step{
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-0")}},
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-0")}},
			},
			want: []result{
				{err: errBoom},
				{at: map[types.UID]time.Time{"a": start.Add(time.Hour + time.Microsecond)}},
			},
		},
		"SameMicrosecond": {
			reason: "Claims read within one microsecond should get times a microsecond apart, truncated to the microsecond.",
			floors: []floor{{}},
			clock:  []time.Time{start.Add(100 * time.Nanosecond), start.Add(900 * time.Nanosecond)},
			steps: []step{
				{claims: []resourcev1.ResourceClaim{claim("a", "gpu-0"), claim("b", "gpu-1")}},
			},
			want: []result{
				{at: map[types.UID]time.Time{"a": start, "b": start.Add(time.Microsecond)}},
			},
		},
		"ReadError": {
			reason: "An error reading claims should be returned.",
			floors: []floor{{}},
			steps: []step{
				{readErr: errBoom},
			},
			want: []result{
				{err: errBoom},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			floors := tc.floors
			// Indexing past the end panics, which fails a case whose Record reads
			// the latest published time more often than the case expects.
			r := NewRecord(func(_ context.Context) (time.Time, error) {
				f := floors[0]
				floors = floors[1:]
				return f.at, f.err
			}, WithClock(ticks(tc.clock...)))
			got := make([]result, 0, len(tc.steps))
			for _, s := range tc.steps {
				if s.forget != "" {
					r.Forget(s.forget)
					got = append(got, result{})
					continue
				}
				at, err := r.Read(t.Context(), func() ([]resourcev1.ResourceClaim, error) { return s.claims, s.readErr })
				got = append(got, result{at: at, err: err})
			}
			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(result{}), cmpopts.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nRead(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestForgetDeleted(t *testing.T) {
	start := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)

	// Two claims of the same name: the first, and the one that replaced it.
	first := claim("a", "gpu-0")
	replaced := claim("b", "gpu-0")
	first.Name, replaced.Name = "llama-0-gpu", "llama-0-gpu"

	cases := map[string]struct {
		reason string
		send   func(h toolscache.ResourceEventHandler)
		want   map[types.UID]time.Time
	}{
		"Deleted": {
			reason: "A deleted claim should be forgotten, so it gets a new time when it's next read.",
			send:   func(h toolscache.ResourceEventHandler) { h.OnDelete(&first) },
			want:   map[types.UID]time.Time{"a": start.Add(time.Second)},
		},
		"Tombstone": {
			reason: "A claim whose deletion the informer missed should be forgotten.",
			send: func(h toolscache.ResourceEventHandler) {
				h.OnDelete(toolscache.DeletedFinalStateUnknown{Key: "ml/llama-0-gpu", Obj: &first})
			},
			want: map[types.UID]time.Time{"a": start.Add(time.Second)},
		},
		"Replaced": {
			reason: "A claim the informer reports replaced by a new claim of the same name should be forgotten.",
			send:   func(h toolscache.ResourceEventHandler) { h.OnUpdate(&first, &replaced) },
			want:   map[types.UID]time.Time{"a": start.Add(time.Second)},
		},
		"Updated": {
			reason: "A claim that's only updated shouldn't be forgotten.",
			send:   func(h toolscache.ResourceEventHandler) { h.OnUpdate(&first, &first) },
			want:   map[types.UID]time.Time{"a": start},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r := NewRecord(noFloor, WithClock(ticks(start, start.Add(time.Second))))
			read := func() ([]resourcev1.ResourceClaim, error) { return []resourcev1.ResourceClaim{first}, nil }
			if _, err := r.Read(t.Context(), read); err != nil {
				t.Fatal(err)
			}
			tc.send(ForgetDeleted(r))
			got, err := r.Read(t.Context(), read)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nForgetDeleted(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestReadHoldsLock(t *testing.T) {
	r := NewRecord(noFloor)
	_, _ = r.Read(t.Context(), func() ([]resourcev1.ResourceClaim, error) {
		if r.mu.TryLock() {
			t.Error("Read(...): the read function ran without the Record's lock held")
		}
		return nil, nil
	})
}

func TestLatestPublished(t *testing.T) {
	start := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)

	type args struct {
		pools    []v1alpha1.PoolObservation
		replicas []v1alpha1.ReplicaObservation
	}

	cases := map[string]struct {
		reason string
		args   args
		want   time.Time
	}{
		"NothingPublished": {
			reason: "With no allocation times published, the latest is the zero time.",
			args: args{
				pools:    []v1alpha1.PoolObservation{{}},
				replicas: []v1alpha1.ReplicaObservation{{}},
			},
			want: time.Time{},
		},
		"PoolLatest": {
			reason: "A pool's time later than every other should be the latest.",
			args: args{
				pools: []v1alpha1.PoolObservation{
					{Status: v1alpha1.PoolObservationStatus{LastAllocationTime: new(metav1.NewMicroTime(start.Add(2 * time.Second)))}},
					{},
				},
				replicas: []v1alpha1.ReplicaObservation{
					{Status: v1alpha1.ReplicaObservationStatus{Pools: []v1alpha1.AllocatedPool{{Name: "gpu", LastAllocationTime: metav1.NewMicroTime(start.Add(time.Second))}}}},
				},
			},
			want: start.Add(2 * time.Second),
		},
		"ReplicaLatest": {
			reason: "A replica's time on any of its pools later than every other should be the latest.",
			args: args{
				pools: []v1alpha1.PoolObservation{
					{Status: v1alpha1.PoolObservationStatus{LastAllocationTime: new(metav1.NewMicroTime(start.Add(time.Second)))}},
				},
				replicas: []v1alpha1.ReplicaObservation{
					{Status: v1alpha1.ReplicaObservationStatus{Pools: []v1alpha1.AllocatedPool{
						{Name: "gpu", LastAllocationTime: metav1.NewMicroTime(start)},
						{Name: "gpu-spot", LastAllocationTime: metav1.NewMicroTime(start.Add(3 * time.Second))},
					}}},
				},
			},
			want: start.Add(3 * time.Second),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := LatestPublished(tc.args.pools, tc.args.replicas)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nLatestPublished(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}
