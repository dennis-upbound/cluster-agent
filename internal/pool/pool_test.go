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

package pool

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/modelplaneai/cluster-agent/internal/dra"
)

// The testdata directories hold objects captured from an EKS cluster with
// one g6.12xlarge node (4 NVIDIA L4 GPUs) in pool gpu-l4x4-a, one system
// node in pool system, and pool gpu-l4x4, whose node group couldn't launch a
// node for want of capacity. Workloads in the ml-test namespace claim GPUs:
//
//   - full: two one-gpu pods and a two-gpu pod have allocated all 4 GPUs.
//     too-big claims 8 GPUs and stocked-out is pinned to gpu-l4x4, so neither
//     can be scheduled.
//   - partial: as full, with one one-gpu pod, so 3 GPUs are allocated.
//   - cordoned: as partial, with the GPU node cordoned.
//
// Each snapshot was captured the way the agent caches objects: nodes in a
// pool, pods of a replica and the cluster autoscaler's events, all without
// managed fields.

var equateQuantities = cmp.Comparer(func(a, b resource.Quantity) bool { return a.Cmp(b) == 0 })

// The attributes every L4 on the GPU node agrees on.
var l4Attributes = map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
	"addressingMode":        {StringValue: new("HMM")},
	"architecture":          {StringValue: new("Ada Lovelace")},
	"brand":                 {StringValue: new("Nvidia")},
	"cudaComputeCapability": {VersionValue: new("8.9.0")},
	"cudaDriverVersion":     {VersionValue: new("13.0.0")},
	"driverVersion":         {VersionValue: new("580.178.4")},
	"productName":           {StringValue: new("NVIDIA L4")},
	"type":                  {StringValue: new("gpu")},
}

func TestObservePool(t *testing.T) {
	full := inputs(t, filepath.Join("testdata", "full"))
	partial := inputs(t, filepath.Join("testdata", "partial"))
	cordoned := inputs(t, filepath.Join("testdata", "cordoned"))

	type args struct {
		pool string
		in   Inputs
	}

	cases := map[string]struct {
		reason string
		args   args
		want   Observation
	}{
		"Full": {
			reason: "Every GPU on the node is allocated.",
			args:   args{pool: "gpu-l4x4-a", in: full},
			want: Observation{
				Nodes: Nodes{Total: 1, Ready: 1, Schedulable: 1},
				Devices: []Devices{
					{
						DeviceClassName: "gpu.nvidia.com",
						Nodes:           1,
						Schedulable:     map[string]NodeDevices{"ip-10-0-54-161.ec2.internal": {Allocatable: 4, Allocated: 4}},
						Attributes:      l4Attributes,
					},
				},
				Free: map[string]FreeDevices{"ip-10-0-54-161.ec2.internal": {"gpu.nvidia.com": 0}},
				UnschedulablePods: []UnschedulablePod{
					{
						NamespacedName: types.NamespacedName{Namespace: "ml-test", Name: "too-big-5859cf75cf-jzm8n"},
						ScaleUp:        ScaleUpNotTriggered,
						ScaleUpMessage: "pod didn't trigger scale-up: 1 node(s) didn't match Pod's node affinity/selector, 1 not ready for scale-up, 1 max node group size reached",
					},
				},
			},
		},
		"Partial": {
			reason: "A node with one GPU free should report it.",
			args:   args{pool: "gpu-l4x4-a", in: partial},
			want: Observation{
				Nodes: Nodes{Total: 1, Ready: 1, Schedulable: 1},
				Devices: []Devices{
					{
						DeviceClassName: "gpu.nvidia.com",
						Nodes:           1,
						Schedulable:     map[string]NodeDevices{"ip-10-0-54-161.ec2.internal": {Allocatable: 4, Allocated: 3}},
						Attributes:      l4Attributes,
					},
				},
				Free: map[string]FreeDevices{"ip-10-0-54-161.ec2.internal": {"gpu.nvidia.com": 1}},
				UnschedulablePods: []UnschedulablePod{
					{
						NamespacedName: types.NamespacedName{Namespace: "ml-test", Name: "too-big-5859cf75cf-jzm8n"},
						ScaleUp:        ScaleUpNotTriggered,
						ScaleUpMessage: "pod didn't trigger scale-up: 1 node(s) didn't match Pod's node affinity/selector, 1 not ready for scale-up, 1 max node group size reached",
					},
				},
			},
		},
		"Cordoned": {
			reason: "A cordoned node's devices should count toward the nodes that publish them, but not toward what's allocatable.",
			args:   args{pool: "gpu-l4x4-a", in: cordoned},
			want: Observation{
				Nodes: Nodes{Total: 1, Ready: 1, Cordoned: 1, Schedulable: 0},
				Devices: []Devices{
					{
						DeviceClassName: "gpu.nvidia.com",
						Nodes:           1,
						Attributes:      l4Attributes,
					},
				},
				UnschedulablePods: []UnschedulablePod{
					{
						NamespacedName: types.NamespacedName{Namespace: "ml-test", Name: "too-big-5859cf75cf-jzm8n"},
						ScaleUp:        ScaleUpNotTriggered,
						ScaleUpMessage: "pod didn't trigger scale-up: 1 node(s) didn't match Pod's node affinity/selector, 1 not ready for scale-up, 1 max node group size reached",
					},
				},
			},
		},
		"NoNodes": {
			reason: "A pool with no nodes should report only the pods pinned to it. The autoscaler's latest event about a pod should win.",
			args:   args{pool: "gpu-l4x4", in: cordoned},
			want: Observation{
				UnschedulablePods: []UnschedulablePod{
					{
						NamespacedName: types.NamespacedName{Namespace: "ml-test", Name: "stocked-out-db588bffc-bzgt8"},
						ScaleUp:        ScaleUpNotTriggered,
						ScaleUpMessage: "pod didn't trigger scale-up: 1 max node group size reached, 1 node(s) didn't match Pod's node affinity/selector, 1 not ready for scale-up",
					},
				},
			},
		},
		"NoDevices": {
			reason: "A pool whose nodes publish no devices should report only its nodes, with no devices free.",
			args:   args{pool: "system", in: full},
			want: Observation{
				Nodes: Nodes{Total: 1, Ready: 1, Schedulable: 1},
				Free:  map[string]FreeDevices{"ip-10-0-69-153.ec2.internal": {}},
			},
		},
		"UnknownPool": {
			reason: "A pool no node or pod names should report nothing.",
			args:   args{pool: "nonexistent", in: full},
			want:   Observation{},
		},
		"OtherPoolsExtendedResources": {
			reason: "Only the pool's nodes should count toward its extended resources.",
			args: args{
				pool: "gpu",
				in: Inputs{Nodes: []corev1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{Name: "gpu-0", Labels: map[string]string{LabelPool: "gpu"}},
						Status:     corev1.NodeStatus{Allocatable: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("4")}},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Name: "gpu-spot-0", Labels: map[string]string{LabelPool: "gpu-spot"}},
						Status:     corev1.NodeStatus{Allocatable: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8")}},
					},
				}},
			},
			want: Observation{
				Nodes:             Nodes{Total: 1},
				ExtendedResources: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("4")},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			before := deepCopy(tc.args.in)
			got, err := NewObserver().ObservePool(t.Context(), tc.args.pool, tc.args.in)
			if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nObservePool(...): -want error, +got error:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty(), equateQuantities); diff != "" {
				t.Errorf("\n%s\nObservePool(...): -want, +got:\n%s", tc.reason, diff)
			}
			// The agent reads inputs straight from its cache.
			if diff := cmp.Diff(before, tc.args.in, equateQuantities); diff != "" {
				t.Errorf("\n%s\nObservePool(...): modified its inputs: -before, +after:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestMatches(t *testing.T) {
	gpu := resourcev1.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu.nvidia.com"},
		Spec: resourcev1.DeviceClassSpec{Selectors: []resourcev1.DeviceSelector{
			{CEL: &resourcev1.CELDeviceSelector{Expression: "device.driver == 'gpu.nvidia.com' && device.attributes['gpu.nvidia.com'].type == 'gpu'"}},
		}},
	}

	type args struct {
		dc     resourcev1.DeviceClass
		device string
	}
	type want struct {
		match bool
		err   error
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"Match": {
			reason: "A device that satisfies every selector belongs to the class.",
			args:   args{dc: gpu, device: "gpu-0"},
			want:   want{match: true},
		},
		"NoMatch": {
			reason: "A device that fails a selector doesn't belong to the class.",
			args: args{
				dc: resourcev1.DeviceClass{Spec: resourcev1.DeviceClassSpec{Selectors: []resourcev1.DeviceSelector{
					{CEL: &resourcev1.CELDeviceSelector{Expression: "device.attributes['gpu.nvidia.com'].type == 'mig'"}},
				}}},
				device: "gpu-0",
			},
			want: want{match: false},
		},
		"RuntimeError": {
			reason: "A selector that errors on a device, here by reading an attribute it doesn't have, means the device doesn't belong to the class.",
			args: args{
				dc: resourcev1.DeviceClass{Spec: resourcev1.DeviceClassSpec{Selectors: []resourcev1.DeviceSelector{
					{CEL: &resourcev1.CELDeviceSelector{Expression: "device.attributes['gpu.nvidia.com'].missing == 'x'"}},
				}}},
				device: "gpu-0",
			},
			want: want{match: false},
		},
		"CompileError": {
			reason: "A selector that doesn't compile should return an error.",
			args: args{
				dc: resourcev1.DeviceClass{Spec: resourcev1.DeviceClassSpec{Selectors: []resourcev1.DeviceSelector{
					{CEL: &resourcev1.CELDeviceSelector{Expression: "device.driver =="}},
				}}},
				device: "gpu-0",
			},
			want: want{match: false, err: cmpopts.AnyError},
		},
	}

	devices := dra.Devices(load[resourcev1.ResourceSliceList](t, filepath.Join("testdata", "full", "slices.json")).Items)

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			d := devices[dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "ip-10-0-54-161.ec2.internal", Device: tc.args.device}]
			got, err := NewObserver().Matches(t.Context(), tc.args.dc, d)
			if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nMatches(...): -want error, +got error:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.match, got); diff != "" {
				t.Errorf("\n%s\nMatches(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestIsExtendedResource(t *testing.T) {
	cases := map[string]struct {
		reason string
		r      corev1.ResourceName
		want   bool
	}{
		"DevicePlugin": {
			reason: "A resource a device plugin advertises is an extended resource.",
			r:      "nvidia.com/gpu",
			want:   true,
		},
		"Native": {
			reason: "A native resource isn't an extended resource.",
			r:      corev1.ResourceCPU,
			want:   false,
		},
		"KubernetesDomain": {
			reason: "A resource in the kubernetes.io domain isn't an extended resource.",
			r:      "kubernetes.io/batch-cpu",
			want:   false,
		},
		"KubernetesSubdomain": {
			reason: "A resource in a kubernetes.io subdomain isn't an extended resource.",
			r:      "node.kubernetes.io/thing",
			want:   false,
		},
		"Requests": {
			reason: "A quota's requests. prefixed name isn't an extended resource.",
			r:      "requests.nvidia.com/gpu",
			want:   false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := IsExtendedResource(tc.r)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nIsExtendedResource(%q): -want, +got:\n%s", tc.reason, tc.r, diff)
			}
		})
	}
}

func TestEventTime(t *testing.T) {
	early := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	late := early.Add(time.Minute)

	cases := map[string]struct {
		reason string
		e      corev1.Event
		want   time.Time
	}{
		"LastTimestamp": {
			reason: "An event reported through the older events API happened at its lastTimestamp.",
			e:      corev1.Event{LastTimestamp: metav1.NewTime(late)},
			want:   late,
		},
		"EventTime": {
			reason: "An event reported through the newer events API happened at its eventTime.",
			e:      corev1.Event{EventTime: metav1.NewMicroTime(late)},
			want:   late,
		},
		"Series": {
			reason: "A repeated event happened when its series was last observed.",
			e: corev1.Event{
				EventTime: metav1.NewMicroTime(early),
				Series:    &corev1.EventSeries{LastObservedTime: metav1.NewMicroTime(late)},
			},
			want: late,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := EventTime(tc.e)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nEventTime(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func inputs(t *testing.T, dir string) Inputs {
	t.Helper()
	return Inputs{
		Nodes:   load[corev1.NodeList](t, filepath.Join(dir, "nodes.json")).Items,
		Slices:  load[resourcev1.ResourceSliceList](t, filepath.Join(dir, "slices.json")).Items,
		Classes: load[resourcev1.DeviceClassList](t, filepath.Join(dir, "classes.json")).Items,
		Claims:  load[resourcev1.ResourceClaimList](t, filepath.Join(dir, "claims.json")).Items,
		Pods:    load[corev1.PodList](t, filepath.Join(dir, "pods.json")).Items,
		Events:  load[corev1.EventList](t, filepath.Join(dir, "events.json")).Items,
	}
}

func deepCopy(in Inputs) Inputs {
	out := Inputs{}
	for i := range in.Nodes {
		out.Nodes = append(out.Nodes, *in.Nodes[i].DeepCopy())
	}
	for i := range in.Slices {
		out.Slices = append(out.Slices, *in.Slices[i].DeepCopy())
	}
	for i := range in.Classes {
		out.Classes = append(out.Classes, *in.Classes[i].DeepCopy())
	}
	for i := range in.Claims {
		out.Claims = append(out.Claims, *in.Claims[i].DeepCopy())
	}
	for i := range in.Pods {
		out.Pods = append(out.Pods, *in.Pods[i].DeepCopy())
	}
	for i := range in.Events {
		out.Events = append(out.Events, *in.Events[i].DeepCopy())
	}
	return out
}

func load[T any](t *testing.T, path string) T {
	t.Helper()
	var v T
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cannot read %s: %v", path, err)
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("cannot unmarshal %s: %v", path, err)
	}
	return v
}

func TestCountNodes(t *testing.T) {
	ready := corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}
	notReady := corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}}

	cases := map[string]struct {
		reason string
		nodes  []corev1.Node
		want   Nodes
	}{
		"States": {
			reason: "Each node should count by whether it's ready and cordoned.",
			nodes: []corev1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "ready"}, Status: ready},
				{ObjectMeta: metav1.ObjectMeta{Name: "cordoned"}, Spec: corev1.NodeSpec{Unschedulable: true}, Status: ready},
				{ObjectMeta: metav1.ObjectMeta{Name: "draining"}, Spec: corev1.NodeSpec{Unschedulable: true}, Status: notReady},
				{ObjectMeta: metav1.ObjectMeta{Name: "no-condition"}},
			},
			want: Nodes{Total: 4, Ready: 2, Cordoned: 2, Schedulable: 1},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := CountNodes(tc.nodes)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nCountNodes(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestExtendedResources(t *testing.T) {
	cases := map[string]struct {
		reason string
		nodes  []corev1.Node
		want   corev1.ResourceList
	}{
		"DevicePlugin": {
			reason: "Extended resources should be summed across a pool's nodes, whatever their state, and native resources ignored.",
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "a"},
					Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("48"),
						"nvidia.com/gpu":   resource.MustParse("4"),
					}},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "b"},
					Spec:       corev1.NodeSpec{Unschedulable: true},
					Status:     corev1.NodeStatus{Allocatable: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("4")}},
				},
			},
			want: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8")},
		},
		"NoneAdvertised": {
			reason: "A pool whose nodes advertise only native resources has no extended resources.",
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "c"},
					Status:     corev1.NodeStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")}},
				},
			},
			want: nil,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := ExtendedResources(tc.nodes)
			if diff := cmp.Diff(tc.want, got, equateQuantities); diff != "" {
				t.Errorf("\n%s\nExtendedResources(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestDevicesByClass(t *testing.T) {
	gpu := resourcev1.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu.nvidia.com"},
		Spec: resourcev1.DeviceClassSpec{Selectors: []resourcev1.DeviceSelector{
			{CEL: &resourcev1.CELDeviceSelector{Expression: "device.driver == 'gpu.nvidia.com'"}},
		}},
	}
	taint := []resourcev1.DeviceTaint{{Key: "gpu.nvidia.com/xid", Effect: resourcev1.DeviceTaintEffectNoSchedule}}
	type args struct {
		devices   []dra.Device
		allocated map[dra.DeviceID]bool
		m         Membership
	}

	cases := map[string]struct {
		reason string
		args   args
		want   []Devices
	}{
		"Tainted": {
			reason: "A tainted device isn't allocatable, but a schedulable node whose devices are all tainted still counts as a node with none free.",
			args: args{
				devices: []dra.Device{
					{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "a", Device: "gpu-0"}, NodeName: "a", Spec: resourcev1.Device{Name: "gpu-0"}},
					{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "a", Device: "gpu-1"}, NodeName: "a", Spec: resourcev1.Device{Name: "gpu-1", Taints: taint}},
					{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "b", Device: "gpu-0"}, NodeName: "b", Spec: resourcev1.Device{Name: "gpu-0", Taints: taint}},
				},
				allocated: map[dra.DeviceID]bool{{Driver: "gpu.nvidia.com", Pool: "a", Device: "gpu-0"}: true},
				m: Membership{
					Nodes:       map[string]bool{"a": true, "b": true},
					Schedulable: map[string]bool{"a": true, "b": true},
				},
			},
			want: []Devices{
				{
					DeviceClassName: "gpu.nvidia.com",
					Nodes:           2,
					Schedulable: map[string]NodeDevices{
						"a": {Allocatable: 1, Allocated: 1},
						"b": {},
					},
				},
			},
		},
		"OtherPools": {
			reason: "Devices on nodes outside the pool shouldn't count.",
			args: args{
				devices: []dra.Device{{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "a", Device: "gpu-0"}, NodeName: "a", Spec: resourcev1.Device{Name: "gpu-0"}}},
				m:       Membership{Nodes: map[string]bool{"b": true}, Schedulable: map[string]bool{"b": true}},
			},
			want: nil,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			devices := map[dra.DeviceID]dra.Device{}
			for _, d := range tc.args.devices {
				devices[d.ID] = d
			}
			got, err := NewObserver().DevicesByClass(t.Context(), devices, []resourcev1.DeviceClass{gpu}, tc.args.allocated, tc.args.m)
			if diff := cmp.Diff(nil, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nDevicesByClass(...): -want error, +got error:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("\n%s\nDevicesByClass(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestTaintedDevices(t *testing.T) {
	type args struct {
		devices []dra.Device
		m       Membership
	}

	cases := map[string]struct {
		reason string
		args   args
		want   int32
	}{
		"Effects": {
			reason: "A pool should count its devices with a taint that keeps new claims from allocating them. A taint with no effect, and a device outside the pool, shouldn't count.",
			args: args{
				devices: []dra.Device{
					{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "a", Device: "gpu-0"}, NodeName: "a", Spec: resourcev1.Device{Name: "gpu-0", Taints: []resourcev1.DeviceTaint{{Key: "gpu.nvidia.com/xid", Effect: resourcev1.DeviceTaintEffectNoSchedule}}}},
					{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "a", Device: "gpu-1"}, NodeName: "a", Spec: resourcev1.Device{Name: "gpu-1", Taints: []resourcev1.DeviceTaint{{Key: "gpu.nvidia.com/xid", Effect: resourcev1.DeviceTaintEffectNoExecute}}}},
					{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "a", Device: "gpu-2"}, NodeName: "a", Spec: resourcev1.Device{Name: "gpu-2", Taints: []resourcev1.DeviceTaint{{Key: "gpu.nvidia.com/xid", Effect: resourcev1.DeviceTaintEffectNone}}}},
					{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "a", Device: "gpu-3"}, NodeName: "a", Spec: resourcev1.Device{Name: "gpu-3"}},
					{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "b", Device: "gpu-0"}, NodeName: "b", Spec: resourcev1.Device{Name: "gpu-0", Taints: []resourcev1.DeviceTaint{{Key: "gpu.nvidia.com/xid", Effect: resourcev1.DeviceTaintEffectNoSchedule}}}},
				},
				m: Membership{Nodes: map[string]bool{"a": true}},
			},
			want: 2,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			devices := map[dra.DeviceID]dra.Device{}
			for _, d := range tc.args.devices {
				devices[d.ID] = d
			}
			got := TaintedDevices(devices, tc.args.m)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nTaintedDevices(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestLatestScaleUpEvents(t *testing.T) {
	early := metav1.NewTime(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	late := metav1.NewTime(early.Add(time.Minute))

	cases := map[string]struct {
		reason string
		events []corev1.Event
		want   map[types.UID]corev1.Event
	}{
		"LatestListedFirst": {
			reason: "The latest scale-up event about a pod should win when it's listed first.",
			events: []corev1.Event{
				{ObjectMeta: metav1.ObjectMeta{Name: "late"}, InvolvedObject: corev1.ObjectReference{UID: "pod"}, Reason: eventTriggeredScaleUp, LastTimestamp: late},
				{ObjectMeta: metav1.ObjectMeta{Name: "early"}, InvolvedObject: corev1.ObjectReference{UID: "pod"}, Reason: eventNotTriggerScaleUp, LastTimestamp: early},
			},
			want: map[types.UID]corev1.Event{
				"pod": {ObjectMeta: metav1.ObjectMeta{Name: "late"}, InvolvedObject: corev1.ObjectReference{UID: "pod"}, Reason: eventTriggeredScaleUp, LastTimestamp: late},
			},
		},
		"LatestListedLast": {
			reason: "The latest scale-up event about a pod should win when it's listed last.",
			events: []corev1.Event{
				{ObjectMeta: metav1.ObjectMeta{Name: "early"}, InvolvedObject: corev1.ObjectReference{UID: "pod"}, Reason: eventNotTriggerScaleUp, LastTimestamp: early},
				{ObjectMeta: metav1.ObjectMeta{Name: "late"}, InvolvedObject: corev1.ObjectReference{UID: "pod"}, Reason: eventTriggeredScaleUp, LastTimestamp: late},
			},
			want: map[types.UID]corev1.Event{
				"pod": {ObjectMeta: metav1.ObjectMeta{Name: "late"}, InvolvedObject: corev1.ObjectReference{UID: "pod"}, Reason: eventTriggeredScaleUp, LastTimestamp: late},
			},
		},
		"TieListedFirst": {
			reason: "Of events that happened in the same second, the one with the greater name should win when it's listed first.",
			events: []corev1.Event{
				{ObjectMeta: metav1.ObjectMeta{Name: "b"}, InvolvedObject: corev1.ObjectReference{UID: "pod"}, Reason: eventTriggeredScaleUp, LastTimestamp: late},
				{ObjectMeta: metav1.ObjectMeta{Name: "a"}, InvolvedObject: corev1.ObjectReference{UID: "pod"}, Reason: eventNotTriggerScaleUp, LastTimestamp: late},
			},
			want: map[types.UID]corev1.Event{
				"pod": {ObjectMeta: metav1.ObjectMeta{Name: "b"}, InvolvedObject: corev1.ObjectReference{UID: "pod"}, Reason: eventTriggeredScaleUp, LastTimestamp: late},
			},
		},
		"TieListedLast": {
			reason: "Of events that happened in the same second, the one with the greater name should win when it's listed last.",
			events: []corev1.Event{
				{ObjectMeta: metav1.ObjectMeta{Name: "a"}, InvolvedObject: corev1.ObjectReference{UID: "pod"}, Reason: eventNotTriggerScaleUp, LastTimestamp: late},
				{ObjectMeta: metav1.ObjectMeta{Name: "b"}, InvolvedObject: corev1.ObjectReference{UID: "pod"}, Reason: eventTriggeredScaleUp, LastTimestamp: late},
			},
			want: map[types.UID]corev1.Event{
				"pod": {ObjectMeta: metav1.ObjectMeta{Name: "b"}, InvolvedObject: corev1.ObjectReference{UID: "pod"}, Reason: eventTriggeredScaleUp, LastTimestamp: late},
			},
		},
		"EachPod": {
			reason: "Each pod should get its own latest event. Events that aren't scale-up verdicts shouldn't count.",
			events: []corev1.Event{
				{ObjectMeta: metav1.ObjectMeta{Name: "a"}, InvolvedObject: corev1.ObjectReference{UID: "pod-a"}, Reason: eventNotTriggerScaleUp, LastTimestamp: early},
				{ObjectMeta: metav1.ObjectMeta{Name: "b"}, InvolvedObject: corev1.ObjectReference{UID: "pod-b"}, Reason: eventTriggeredScaleUp, LastTimestamp: early},
				{ObjectMeta: metav1.ObjectMeta{Name: "a-scale-down"}, InvolvedObject: corev1.ObjectReference{UID: "pod-a"}, Reason: "ScaleDown", LastTimestamp: late},
				{ObjectMeta: metav1.ObjectMeta{Name: "c-scale-down"}, InvolvedObject: corev1.ObjectReference{UID: "pod-c"}, Reason: "ScaleDown", LastTimestamp: late},
			},
			want: map[types.UID]corev1.Event{
				"pod-a": {ObjectMeta: metav1.ObjectMeta{Name: "a"}, InvolvedObject: corev1.ObjectReference{UID: "pod-a"}, Reason: eventNotTriggerScaleUp, LastTimestamp: early},
				"pod-b": {ObjectMeta: metav1.ObjectMeta{Name: "b"}, InvolvedObject: corev1.ObjectReference{UID: "pod-b"}, Reason: eventTriggeredScaleUp, LastTimestamp: early},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := LatestScaleUpEvents(tc.events)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nLatestScaleUpEvents(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}
