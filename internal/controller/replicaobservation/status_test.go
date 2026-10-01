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

package replicaobservation

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/modelplaneai/cluster-agent/apis/v1alpha1"
	"github.com/modelplaneai/cluster-agent/internal/dra"
	"github.com/modelplaneai/cluster-agent/internal/replica"
)

// A condition that changes status is stamped with the current time, so allow
// for the time it takes to run a test.
var equateNow = cmpopts.EquateApproxTime(5 * time.Second)

func TestStatus(t *testing.T) {
	then := metav1.NewTime(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	now := metav1.Now()

	type args struct {
		existing   v1alpha1.ReplicaObservationStatus
		generation int64
		o          replica.Observation
	}

	cases := map[string]struct {
		reason string
		args   args
		want   v1alpha1.ReplicaObservationStatus
	}{
		"Serving": {
			reason: "A replica whose pods are all scheduled and ready should report its pods, its devices, and conditions that transitioned now.",
			args: args{
				generation: 2,
				o: replica.Observation{
					Pods: []replica.Pod{
						{Name: "leader", Scheduling: replica.Scheduled, Ready: true},
						{Name: "worker", Scheduling: replica.Scheduled, Ready: true},
					},
					Devices: []replica.Device{
						{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "node-a", Device: "gpu-0"}, NodeName: "node-a", UUID: "GPU-a"},
						{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "node-b", Device: "gpu-0"}, NodeName: "node-b"},
					},
				},
			},
			want: v1alpha1.ReplicaObservationStatus{
				ObservedGeneration: 2,
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPodsScheduled, ObservedGeneration: 2, LastTransitionTime: now},
					{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPodsReady, ObservedGeneration: 2, LastTransitionTime: now},
				},
				Pods: v1alpha1.PodCounts{Total: 2, Scheduled: 2, Ready: 2},
				Devices: []v1alpha1.AllocatedDevice{
					{Driver: "gpu.nvidia.com", Pool: "node-a", Device: "gpu-0", NodeName: "node-a", UUID: "GPU-a"},
					{Driver: "gpu.nvidia.com", Pool: "node-b", Device: "gpu-0", NodeName: "node-b"},
				},
			},
		},
		"KeepsTransitionTime": {
			reason: "A condition whose status hasn't changed should keep its lastTransitionTime, even when its message changes.",
			args: args{
				existing: v1alpha1.ReplicaObservationStatus{
					ObservedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPodsScheduled, ObservedGeneration: 1, LastTransitionTime: then},
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonStarting, Message: "0 of 2 pods ready", ObservedGeneration: 1, LastTransitionTime: then},
					},
					Pods: v1alpha1.PodCounts{Total: 2, Scheduled: 2},
				},
				generation: 1,
				o: replica.Observation{
					Pods: []replica.Pod{
						{Name: "leader", Scheduling: replica.Scheduled, Ready: true},
						{Name: "worker", Scheduling: replica.Scheduled},
					},
				},
			},
			want: v1alpha1.ReplicaObservationStatus{
				ObservedGeneration: 1,
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPodsScheduled, ObservedGeneration: 1, LastTransitionTime: then},
					{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonStarting, Message: "1 of 2 pods ready", ObservedGeneration: 1, LastTransitionTime: then},
				},
				Pods: v1alpha1.PodCounts{Total: 2, Scheduled: 2, Ready: 1},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := Status(tc.args.existing, tc.args.generation, tc.args.o)
			if diff := cmp.Diff(tc.want, got, equateNow); diff != "" {
				t.Errorf("\n%s\nStatus(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestFailedStatus(t *testing.T) {
	then := metav1.NewTime(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	now := metav1.Now()

	type args struct {
		existing   v1alpha1.ReplicaObservationStatus
		generation int64
		reason     string
		err        error
	}

	cases := map[string]struct {
		reason string
		args   args
		want   v1alpha1.ReplicaObservationStatus
	}{
		"Failed": {
			reason: "A failed observation should make both conditions unknown for the supplied reason, and clear the observed pods and devices.",
			args: args{
				existing: v1alpha1.ReplicaObservationStatus{
					ObservedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPodsScheduled, ObservedGeneration: 1, LastTransitionTime: then},
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPodsReady, ObservedGeneration: 1, LastTransitionTime: then},
					},
					Pods:    v1alpha1.PodCounts{Total: 1, Scheduled: 1, Ready: 1},
					Devices: []v1alpha1.AllocatedDevice{{Driver: "gpu.nvidia.com", Pool: "node-a", Device: "gpu-0"}},
				},
				generation: 2,
				reason:     v1alpha1.ReasonInvalidPodSelector,
				err:        errors.New("boom"),
			},
			want: v1alpha1.ReplicaObservationStatus{
				ObservedGeneration: 2,
				Conditions: []metav1.Condition{
					{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonInvalidPodSelector, Message: "boom", ObservedGeneration: 2, LastTransitionTime: now},
					{Type: v1alpha1.TypeReady, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonInvalidPodSelector, Message: "boom", ObservedGeneration: 2, LastTransitionTime: now},
				},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := FailedStatus(tc.args.existing, tc.args.generation, tc.args.reason, tc.args.err)
			if diff := cmp.Diff(tc.want, got, equateNow); diff != "" {
				t.Errorf("\n%s\nFailedStatus(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestScheduled(t *testing.T) {
	cases := map[string]struct {
		reason string
		o      replica.Observation
		want   metav1.Condition
	}{
		"NoPods": {
			reason: "A replica with no pods isn't scheduled.",
			o:      replica.Observation{},
			want:   metav1.Condition{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoPods},
		},
		"AllScheduled": {
			reason: "A replica whose pods are all bound to nodes is scheduled.",
			o: replica.Observation{Pods: []replica.Pod{
				{Name: "a", Scheduling: replica.Scheduled},
				{Name: "b", Scheduling: replica.Scheduled},
			}},
			want: metav1.Condition{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPodsScheduled},
		},
		"Unschedulable": {
			reason: "An unschedulable pod should be reported ahead of one the scheduler hasn't tried yet.",
			o: replica.Observation{Pods: []replica.Pod{
				{Name: "a", Scheduling: replica.Pending},
				{Name: "b", Scheduling: replica.Unschedulable, SchedulingMessage: "0/4 nodes are available: 4 cannot allocate all claims."},
			}},
			want: metav1.Condition{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonUnschedulable, Message: "pod b: 0/4 nodes are available: 4 cannot allocate all claims."},
		},
		"Gated": {
			reason: "A gated pod should be reported ahead of one the scheduler hasn't tried yet.",
			o: replica.Observation{Pods: []replica.Pod{
				{Name: "a", Scheduling: replica.Pending},
				{Name: "b", Scheduling: replica.Gated},
			}},
			want: metav1.Condition{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonSchedulingGated, Message: "pod b"},
		},
		"Pending": {
			reason: "A pod the scheduler hasn't tried yet means the replica isn't scheduled.",
			o: replica.Observation{Pods: []replica.Pod{
				{Name: "a", Scheduling: replica.Scheduled},
				{Name: "b", Scheduling: replica.Pending},
			}},
			want: metav1.Condition{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonSchedulingPending, Message: "pod b"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := Scheduled(tc.o)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nScheduled(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestReady(t *testing.T) {
	cases := map[string]struct {
		reason string
		o      replica.Observation
		want   metav1.Condition
	}{
		"NoPods": {
			reason: "A replica with no pods isn't ready.",
			o:      replica.Observation{},
			want:   metav1.Condition{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoPods},
		},
		"AllReady": {
			reason: "A replica whose pods are all ready is ready.",
			o: replica.Observation{Pods: []replica.Pod{
				{Name: "a", Scheduling: replica.Scheduled, Ready: true},
			}},
			want: metav1.Condition{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPodsReady},
		},
		"NotScheduled": {
			reason: "A replica with an unscheduled pod is waiting to be scheduled, whatever its other pods are doing.",
			o: replica.Observation{Pods: []replica.Pod{
				{Name: "a", Scheduling: replica.Scheduled, Waiting: []replica.Waiting{{Container: "engine", Reason: "ImagePullBackOff"}}},
				{Name: "b", Scheduling: replica.Unschedulable},
			}},
			want: metav1.Condition{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNotScheduled, Message: "1 of 2 pods scheduled"},
		},
		"ImagePullBackOff": {
			reason: "A container that can't pull its image should be reported ahead of one that's crashing.",
			o: replica.Observation{Pods: []replica.Pod{
				{Name: "a", Scheduling: replica.Scheduled, Waiting: []replica.Waiting{{Container: "engine", Reason: "CrashLoopBackOff", Message: "back-off restarting"}}},
				{Name: "b", Scheduling: replica.Scheduled, Waiting: []replica.Waiting{{Container: "engine", Reason: "ErrImagePull", Message: "not found"}}},
			}},
			want: metav1.Condition{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonImagePullBackOff, Message: "pod b container engine: ErrImagePull: not found"},
		},
		"CrashLoopBackOff": {
			reason: "A container that keeps crashing should be reported.",
			o: replica.Observation{Pods: []replica.Pod{
				{Name: "a", Scheduling: replica.Scheduled, Waiting: []replica.Waiting{
					{Container: "init", Reason: "PodInitializing"},
					{Container: "engine", Reason: "CrashLoopBackOff", Message: "back-off restarting"},
				}},
			}},
			want: metav1.Condition{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonCrashLoopBackOff, Message: "pod a container engine: CrashLoopBackOff: back-off restarting"},
		},
		"CreateContainerError": {
			reason: "A container that can't be created should be reported.",
			o: replica.Observation{Pods: []replica.Pod{
				{Name: "a", Scheduling: replica.Scheduled, Waiting: []replica.Waiting{{Container: "engine", Reason: "CreateContainerConfigError", Message: "secret not found"}}},
			}},
			want: metav1.Condition{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonCreateContainerError, Message: "pod a container engine: CreateContainerConfigError: secret not found"},
		},
		"Starting": {
			reason: "A scheduled replica whose containers aren't stuck is still starting.",
			o: replica.Observation{Pods: []replica.Pod{
				{Name: "a", Scheduling: replica.Scheduled, Ready: true},
				{Name: "b", Scheduling: replica.Scheduled, Waiting: []replica.Waiting{{Container: "engine", Reason: "ContainerCreating"}}},
			}},
			want: metav1.Condition{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonStarting, Message: "1 of 2 pods ready"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := Ready(tc.o)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nReady(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}
