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

package clusterobservation

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

	type args struct {
		existing v1alpha1.ClusterObservationStatus
		pools    []pool.Observation
	}

	cases := map[string]struct {
		reason string
		args   args
		want   v1alpha1.ClusterObservationStatus
	}{
		"NoPools": {
			reason: "A cluster with no pools should report that it was observed, and no pools.",
			args:   args{},
			want: v1alpha1.ClusterObservationStatus{
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: now},
				},
			},
		},
		"NewPool": {
			reason: "A pool that's new to the status should report everything observed about it, with conditions that transitioned now.",
			args: args{
				pools: []pool.Observation{
					{
						Name:  "gpu",
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
						ExtendedResources: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8")},
						Replicas:          []types.NamespacedName{{Namespace: "ml", Name: "llama"}},
					},
				},
			},
			want: v1alpha1.ClusterObservationStatus{
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: now},
				},
				Pools: []v1alpha1.PoolStatus{
					{
						Name:  "gpu",
						Nodes: v1alpha1.NodeCounts{Total: 2, Ready: 2, Schedulable: 2},
						Devices: []v1alpha1.DeviceClassStatus{
							{
								DeviceClassName: "gpu.nvidia.com",
								Nodes:           2,
								Allocatable:     8,
								Allocated:       1,
								NodesByFreeDevices: []v1alpha1.FreeDevicesBucket{
									{FreeDevices: 3, Nodes: 1},
									{FreeDevices: 4, Nodes: 1},
								},
								Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
									"productName": {StringValue: new("NVIDIA L4")},
								},
							},
						},
						ExtendedResources: map[corev1.ResourceName]resource.Quantity{"nvidia.com/gpu": resource.MustParse("8")},
						Replicas:          []v1alpha1.ReplicaReference{{Namespace: "ml", Name: "llama"}},
						Conditions: []metav1.Condition{
							{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy, LastTransitionTime: now},
							{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, LastTransitionTime: now},
						},
					},
				},
			},
		},
		"KeepsTransitionTime": {
			reason: "A condition whose status hasn't changed should keep its lastTransitionTime, even when its message changes.",
			args: args{
				existing: v1alpha1.ClusterObservationStatus{
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: then},
					},
					Pools: []v1alpha1.PoolStatus{
						{
							Name:  "gpu",
							Nodes: v1alpha1.NodeCounts{Total: 2, Ready: 1, Schedulable: 1},
							Conditions: []metav1.Condition{
								{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNodesNotReady, Message: "1 of 2 nodes not ready", LastTransitionTime: then},
								{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, LastTransitionTime: then},
							},
						},
					},
				},
				pools: []pool.Observation{
					{Name: "gpu", Nodes: pool.Nodes{Total: 3, Ready: 1, Schedulable: 1}},
				},
			},
			want: v1alpha1.ClusterObservationStatus{
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: then},
				},
				Pools: []v1alpha1.PoolStatus{
					{
						Name:  "gpu",
						Nodes: v1alpha1.NodeCounts{Total: 3, Ready: 1, Schedulable: 1},
						Conditions: []metav1.Condition{
							{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNodesNotReady, Message: "2 of 3 nodes not ready", LastTransitionTime: then},
							{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, LastTransitionTime: then},
						},
					},
				},
			},
		},
		"StatusChanged": {
			reason: "A condition whose status changed should transition now, and a failed observation should recover.",
			args: args{
				existing: v1alpha1.ClusterObservationStatus{
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonObservationFailed, Message: "boom", LastTransitionTime: then},
					},
					Pools: []v1alpha1.PoolStatus{
						{
							Name:  "gpu",
							Nodes: v1alpha1.NodeCounts{Total: 2, Ready: 1, Schedulable: 1},
							Conditions: []metav1.Condition{
								{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNodesNotReady, Message: "1 of 2 nodes not ready", LastTransitionTime: then},
								{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, LastTransitionTime: then},
							},
						},
					},
				},
				pools: []pool.Observation{
					{Name: "gpu", Nodes: pool.Nodes{Total: 2, Ready: 2, Schedulable: 2}},
				},
			},
			want: v1alpha1.ClusterObservationStatus{
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: now},
				},
				Pools: []v1alpha1.PoolStatus{
					{
						Name:  "gpu",
						Nodes: v1alpha1.NodeCounts{Total: 2, Ready: 2, Schedulable: 2},
						Conditions: []metav1.Condition{
							{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy, LastTransitionTime: now},
							{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, LastTransitionTime: then},
						},
					},
				},
			},
		},
		"DropsVanishedPool": {
			reason: "A pool that's no longer observed should drop out of the status.",
			args: args{
				existing: v1alpha1.ClusterObservationStatus{
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: then},
					},
					Pools: []v1alpha1.PoolStatus{
						{Name: "gone", Nodes: v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1}},
					},
				},
			},
			want: v1alpha1.ClusterObservationStatus{
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: then},
				},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := Status(tc.args.existing, tc.args.pools)
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
		existing v1alpha1.ClusterObservationStatus
		err      error
	}

	cases := map[string]struct {
		reason string
		args   args
		want   v1alpha1.ClusterObservationStatus
	}{
		"KeepsStalePools": {
			reason: "A failed observation should make Ready False, and keep the pools the last successful one reported.",
			args: args{
				existing: v1alpha1.ClusterObservationStatus{
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: then},
					},
					Pools: []v1alpha1.PoolStatus{
						{Name: "gpu", Nodes: v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1}},
					},
				},
				err: errors.New("boom"),
			},
			want: v1alpha1.ClusterObservationStatus{
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonObservationFailed, Message: "boom", LastTransitionTime: now},
				},
				Pools: []v1alpha1.PoolStatus{
					{Name: "gpu", Nodes: v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1}},
				},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := FailedStatus(tc.args.existing, tc.args.err)
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
		"Histogram": {
			reason: "Schedulable nodes should be counted by how many of their allocatable devices aren't allocated.",
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
				NodesByFreeDevices: []v1alpha1.FreeDevicesBucket{
					{FreeDevices: 0, Nodes: 1},
					{FreeDevices: 3, Nodes: 2},
				},
			},
		},
		"NoSchedulableNodes": {
			reason: "A class with no devices on schedulable nodes has nothing allocatable and an empty histogram.",
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
