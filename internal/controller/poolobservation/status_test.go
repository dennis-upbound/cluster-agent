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

package poolobservation

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/modelplaneai/cluster-agent/apis/v1alpha1"
	"github.com/modelplaneai/cluster-agent/internal/pool"
)

// A condition that changes status is stamped with the current time, so allow
// for the time it takes to run a test.
var equateNow = cmpopts.EquateApproxTime(5 * time.Second)

var equateQuantities = cmp.Comparer(func(a, b resource.Quantity) bool { return a.Cmp(b) == 0 })

func TestStatus(t *testing.T) {
	then := metav1.NewTime(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	now := metav1.Now()
	allocated := time.Date(2026, 10, 20, 10, 0, 0, 123456000, time.UTC)

	type args struct {
		existing   v1alpha1.PoolObservationStatus
		generation int64
		observed   pool.Observation
	}

	cases := map[string]struct {
		reason string
		args   args
		want   v1alpha1.PoolObservationStatus
	}{
		"FirstObservation": {
			reason: "A pool observed for the first time should report everything observed about it, with conditions that transitioned now.",
			args: args{
				generation: 2,
				observed: pool.Observation{
					Nodes: pool.Nodes{Total: 2, Ready: 2, Schedulable: 2},
					Devices: []pool.Devices{
						{
							DeviceClassName: "gpu.nvidia.com",
							Nodes:           2,
							Schedulable: map[string]pool.NodeDevices{
								"node-a": {Allocatable: 4, Allocated: 1},
								"node-b": {Allocatable: 4},
							},
							Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
								"productName": {StringValue: new("NVIDIA L4")},
							},
						},
					},
					Free: map[string]pool.FreeDevices{
						"node-a": {"gpu.nvidia.com": 3},
						"node-b": {"gpu.nvidia.com": 4},
					},
					LastAllocation:    allocated,
					ExtendedResources: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8")},
					UnschedulablePods: []pool.UnschedulablePod{
						{
							NamespacedName: types.NamespacedName{Namespace: "ml", Name: "llama-1"},
							ScaleUp:        pool.ScaleUpTriggered,
							ScaleUpMessage: "pod triggered scale-up: [{gpu 2->3 (max: 4)}]",
						},
					},
				},
			},
			want: v1alpha1.PoolObservationStatus{
				ObservedGeneration: 2,
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy, ObservedGeneration: 2, LastTransitionTime: now},
					{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonScaleUpTriggered, Message: "pod ml/llama-1: pod triggered scale-up: [{gpu 2->3 (max: 4)}]", ObservedGeneration: 2, LastTransitionTime: now},
				},
				Nodes:             v1alpha1.NodeCounts{Total: 2, Ready: 2, Schedulable: 2},
				UnschedulablePods: 1,
				Devices: []v1alpha1.DeviceClassStatus{
					{
						DeviceClassName: "gpu.nvidia.com",
						Nodes:           2,
						Allocatable:     8,
						Allocated:       1,
						Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
							"productName": {StringValue: new("NVIDIA L4")},
						},
					},
				},
				NodesByFreeDevices: []v1alpha1.FreeDevicesBucket{
					{FreeDevices: map[string]int32{"gpu.nvidia.com": 3}, Nodes: 1},
					{FreeDevices: map[string]int32{"gpu.nvidia.com": 4}, Nodes: 1},
				},
				LastAllocationTime: new(metav1.NewMicroTime(allocated)),
				ExtendedResources:  map[corev1.ResourceName]resource.Quantity{"nvidia.com/gpu": resource.MustParse("8")},
			},
		},
		"KeepsTransitionTime": {
			reason: "A condition whose status hasn't changed should keep its lastTransitionTime, even when its message changes.",
			args: args{
				existing: v1alpha1.PoolObservationStatus{
					ObservedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNodesNotReady, Message: "1 of 2 nodes not ready", ObservedGeneration: 1, LastTransitionTime: then},
						{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, ObservedGeneration: 1, LastTransitionTime: then},
					},
					Nodes: v1alpha1.NodeCounts{Total: 2, Ready: 1, Schedulable: 1},
				},
				generation: 1,
				observed:   pool.Observation{Nodes: pool.Nodes{Total: 3, Ready: 1, Schedulable: 1}},
			},
			want: v1alpha1.PoolObservationStatus{
				ObservedGeneration: 1,
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNodesNotReady, Message: "2 of 3 nodes not ready", ObservedGeneration: 1, LastTransitionTime: then},
					{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, ObservedGeneration: 1, LastTransitionTime: then},
				},
				Nodes: v1alpha1.NodeCounts{Total: 3, Ready: 1, Schedulable: 1},
			},
		},
		"AllocationTimeNeverGoesBack": {
			reason: "A pool whose latest claim was deallocated should keep the later allocation time it published.",
			args: args{
				existing: v1alpha1.PoolObservationStatus{
					ObservedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy, ObservedGeneration: 1, LastTransitionTime: then},
						{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, ObservedGeneration: 1, LastTransitionTime: then},
					},
					Nodes:              v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1},
					LastAllocationTime: new(metav1.NewMicroTime(allocated.Add(24 * time.Hour))),
				},
				generation: 1,
				observed:   pool.Observation{Nodes: pool.Nodes{Total: 1, Ready: 1, Schedulable: 1}, LastAllocation: allocated},
			},
			want: v1alpha1.PoolObservationStatus{
				ObservedGeneration: 1,
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy, ObservedGeneration: 1, LastTransitionTime: then},
					{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, ObservedGeneration: 1, LastTransitionTime: then},
				},
				Nodes:              v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1},
				LastAllocationTime: new(metav1.NewMicroTime(allocated.Add(24 * time.Hour))),
			},
		},
		"Recovers": {
			reason: "A condition whose status changed should transition now, including when a failed observation recovers.",
			args: args{
				existing: v1alpha1.PoolObservationStatus{
					ObservedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonObservationFailed, Message: "boom", ObservedGeneration: 1, LastTransitionTime: then},
						{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, ObservedGeneration: 1, LastTransitionTime: then},
					},
				},
				generation: 1,
				observed:   pool.Observation{Nodes: pool.Nodes{Total: 2, Ready: 2, Schedulable: 2}},
			},
			want: v1alpha1.PoolObservationStatus{
				ObservedGeneration: 1,
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy, ObservedGeneration: 1, LastTransitionTime: now},
					{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, ObservedGeneration: 1, LastTransitionTime: then},
				},
				Nodes: v1alpha1.NodeCounts{Total: 2, Ready: 2, Schedulable: 2},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := Status(tc.args.existing, tc.args.generation, tc.args.observed)
			if diff := cmp.Diff(tc.want, got, equateNow, equateQuantities); diff != "" {
				t.Errorf("\n%s\nStatus(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestFailedStatus(t *testing.T) {
	then := metav1.NewTime(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	now := metav1.Now()

	type args struct {
		existing   v1alpha1.PoolObservationStatus
		generation int64
		err        error
	}

	cases := map[string]struct {
		reason string
		args   args
		want   v1alpha1.PoolObservationStatus
	}{
		"Failed": {
			reason: "A failed observation should make both conditions unknown, and clear the observed nodes and devices, but keep the allocation time, which never goes back.",
			args: args{
				existing: v1alpha1.PoolObservationStatus{
					ObservedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy, ObservedGeneration: 1, LastTransitionTime: then},
						{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, ObservedGeneration: 1, LastTransitionTime: then},
					},
					Nodes:              v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1},
					Devices:            []v1alpha1.DeviceClassStatus{{DeviceClassName: "gpu.nvidia.com", Nodes: 1, Allocatable: 4}},
					LastAllocationTime: new(metav1.NewMicroTime(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))),
				},
				generation: 2,
				err:        errors.New("boom"),
			},
			want: v1alpha1.PoolObservationStatus{
				ObservedGeneration: 2,
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonObservationFailed, Message: "boom", ObservedGeneration: 2, LastTransitionTime: now},
					{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonObservationFailed, Message: "boom", ObservedGeneration: 2, LastTransitionTime: now},
				},
				LastAllocationTime: new(metav1.NewMicroTime(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := FailedStatus(tc.args.existing, tc.args.generation, tc.args.err)
			if diff := cmp.Diff(tc.want, got, equateNow); diff != "" {
				t.Errorf("\n%s\nFailedStatus(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestDeviceClassStatus(t *testing.T) {
	cases := map[string]struct {
		reason string
		d      pool.Devices
		want   v1alpha1.DeviceClassStatus
	}{
		"Totals": {
			reason: "A class's allocatable and allocated devices should be summed across its schedulable nodes.",
			d: pool.Devices{
				DeviceClassName: "gpu.nvidia.com",
				Nodes:           4,
				Schedulable: map[string]pool.NodeDevices{
					"node-a": {Allocatable: 4, Allocated: 1},
					"node-b": {Allocatable: 4, Allocated: 4},
					"node-c": {Allocatable: 4, Allocated: 1},
				},
			},
			want: v1alpha1.DeviceClassStatus{
				DeviceClassName: "gpu.nvidia.com",
				Nodes:           4,
				Allocatable:     12,
				Allocated:       6,
			},
		},
		"NoSchedulableNodes": {
			reason: "A class with no devices on schedulable nodes has nothing allocatable.",
			d: pool.Devices{
				DeviceClassName: "gpu.nvidia.com",
				Nodes:           1,
				Schedulable:     map[string]pool.NodeDevices{},
				Attributes:      map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{},
			},
			want: v1alpha1.DeviceClassStatus{
				DeviceClassName: "gpu.nvidia.com",
				Nodes:           1,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := DeviceClassStatus(tc.d)
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("\n%s\nDeviceClassStatus(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestNodesByFreeDevices(t *testing.T) {
	cases := map[string]struct {
		reason string
		free   map[string]pool.FreeDevices
		want   []v1alpha1.FreeDevicesBucket
	}{
		"NoNodes": {
			reason: "A pool with no schedulable nodes has an empty histogram.",
			free:   nil,
			want:   nil,
		},
		"OneClass": {
			reason: "Nodes with the same number of free devices should share a bucket, and buckets should be sorted by how many are free.",
			free: map[string]pool.FreeDevices{
				"node-a": {"gpu.nvidia.com": 3},
				"node-b": {"gpu.nvidia.com": 0},
				"node-c": {"gpu.nvidia.com": 3},
			},
			want: []v1alpha1.FreeDevicesBucket{
				{FreeDevices: map[string]int32{"gpu.nvidia.com": 0}, Nodes: 1},
				{FreeDevices: map[string]int32{"gpu.nvidia.com": 3}, Nodes: 2},
			},
		},
		"SeveralClasses": {
			reason: "Nodes should share a bucket only when they have the same number of free devices of every class.",
			free: map[string]pool.FreeDevices{
				"node-a": {"gpu.nvidia.com": 4, "efa.networking.k8s.aws": 8},
				"node-b": {"gpu.nvidia.com": 4, "efa.networking.k8s.aws": 0},
				"node-c": {"gpu.nvidia.com": 4, "efa.networking.k8s.aws": 8},
				"node-d": {"gpu.nvidia.com": 0, "efa.networking.k8s.aws": 16},
			},
			want: []v1alpha1.FreeDevicesBucket{
				{FreeDevices: map[string]int32{"gpu.nvidia.com": 4, "efa.networking.k8s.aws": 0}, Nodes: 1},
				{FreeDevices: map[string]int32{"gpu.nvidia.com": 4, "efa.networking.k8s.aws": 8}, Nodes: 2},
				{FreeDevices: map[string]int32{"gpu.nvidia.com": 0, "efa.networking.k8s.aws": 16}, Nodes: 1},
			},
		},
		"NoDevices": {
			reason: "A node that publishes no devices of a class should count in a different bucket from one with none of them free.",
			free: map[string]pool.FreeDevices{
				"node-a": {},
				"node-b": {"gpu.nvidia.com": 0},
			},
			want: []v1alpha1.FreeDevicesBucket{
				{FreeDevices: map[string]int32{}, Nodes: 1},
				{FreeDevices: map[string]int32{"gpu.nvidia.com": 0}, Nodes: 1},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := NodesByFreeDevices(tc.free)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nNodesByFreeDevices(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestHealthy(t *testing.T) {
	cases := map[string]struct {
		reason string
		p      pool.Observation
		want   metav1.Condition
	}{
		"Healthy": {
			reason: "A pool whose nodes are all ready and schedulable, with no tainted devices, is healthy.",
			p:      pool.Observation{Nodes: pool.Nodes{Total: 2, Ready: 2, Schedulable: 2}},
			want:   metav1.Condition{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy},
		},
		"NoNodes": {
			reason: "A pool with no nodes has nothing unhealthy in it.",
			p:      pool.Observation{},
			want:   metav1.Condition{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy},
		},
		"Cordoned": {
			reason: "A pool with a cordoned node is unhealthy.",
			p:      pool.Observation{Nodes: pool.Nodes{Total: 2, Ready: 2, Cordoned: 1, Schedulable: 1}},
			want:   metav1.Condition{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNodesCordoned, Message: "1 of 2 nodes cordoned"},
		},
		"NotReadyAndCordoned": {
			reason: "A node that's both not ready and cordoned should be reported as both.",
			p:      pool.Observation{Nodes: pool.Nodes{Total: 1, Cordoned: 1}},
			want:   metav1.Condition{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNodesNotReady, Message: "1 of 1 nodes not ready; 1 of 1 nodes cordoned"},
		},
		"Tainted": {
			reason: "A pool with a tainted device is unhealthy.",
			p:      pool.Observation{Nodes: pool.Nodes{Total: 1, Ready: 1, Schedulable: 1}, TaintedDevices: 2},
			want:   metav1.Condition{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonDevicesTainted, Message: "2 devices tainted"},
		},
		"SeveralProblems": {
			reason: "The reason should name the first problem, and the message all of them.",
			p:      pool.Observation{Nodes: pool.Nodes{Total: 4, Ready: 3, Cordoned: 1, Schedulable: 2}, TaintedDevices: 1},
			want:   metav1.Condition{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNodesNotReady, Message: "1 of 4 nodes not ready; 1 of 4 nodes cordoned; 1 devices tainted"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := Healthy(tc.p)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nHealthy(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestScaleUpBlocked(t *testing.T) {
	cases := map[string]struct {
		reason string
		p      pool.Observation
		want   metav1.Condition
	}{
		"NoUnschedulablePods": {
			reason: "Scale up isn't blocked when no pods are waiting for it.",
			p:      pool.Observation{},
			want:   metav1.Condition{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods},
		},
		"NotTriggered": {
			reason: "Scale up is blocked when the autoscaler says it can't add a node for any unschedulable pod.",
			p: pool.Observation{UnschedulablePods: []pool.UnschedulablePod{
				{NamespacedName: types.NamespacedName{Namespace: "ml", Name: "a"}, ScaleUp: pool.ScaleUpTriggered, ScaleUpMessage: "pod triggered scale-up"},
				{NamespacedName: types.NamespacedName{Namespace: "ml", Name: "b"}, ScaleUp: pool.ScaleUpNotTriggered, ScaleUpMessage: "pod didn't trigger scale-up"},
			}},
			want: metav1.Condition{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonScaleUpNotTriggered, Message: "pod ml/b: pod didn't trigger scale-up"},
		},
		"Triggered": {
			reason: "Scale up isn't blocked when the autoscaler is adding nodes.",
			p: pool.Observation{UnschedulablePods: []pool.UnschedulablePod{
				{NamespacedName: types.NamespacedName{Namespace: "ml", Name: "a"}, ScaleUp: pool.ScaleUpUnknown},
				{NamespacedName: types.NamespacedName{Namespace: "ml", Name: "b"}, ScaleUp: pool.ScaleUpTriggered, ScaleUpMessage: "pod triggered scale-up"},
			}},
			want: metav1.Condition{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonScaleUpTriggered, Message: "pod ml/b: pod triggered scale-up"},
		},
		"NoVerdict": {
			reason: "Whether scale up is blocked is unknown when no autoscaler has reported on the unschedulable pods.",
			p:      pool.Observation{UnschedulablePods: []pool.UnschedulablePod{{NamespacedName: types.NamespacedName{Namespace: "ml", Name: "a"}, ScaleUp: pool.ScaleUpUnknown}}},
			want:   metav1.Condition{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonNoAutoscalerEvents, Message: "1 pods are unschedulable, but no cluster autoscaler has reported on them"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := ScaleUpBlocked(tc.p)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nScaleUpBlocked(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}
