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

package dra

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	resourcev1 "k8s.io/api/resource/v1"
)

func TestDevices(t *testing.T) {
	cases := map[string]struct {
		reason string
		slices []resourcev1.ResourceSlice
		want   map[DeviceID]Device
	}{
		"NewestGeneration": {
			reason: "Only the slices of a pool's newest generation are current.",
			slices: []resourcev1.ResourceSlice{
				{Spec: resourcev1.ResourceSliceSpec{
					Driver:   "gpu.nvidia.com",
					Pool:     resourcev1.ResourcePool{Name: "node-a", Generation: 1, ResourceSliceCount: 1},
					NodeName: new("node-a"),
					Devices:  []resourcev1.Device{{Name: "gpu-0"}, {Name: "gpu-1"}},
				}},
				{Spec: resourcev1.ResourceSliceSpec{
					Driver:   "gpu.nvidia.com",
					Pool:     resourcev1.ResourcePool{Name: "node-a", Generation: 2, ResourceSliceCount: 1},
					NodeName: new("node-a"),
					Devices:  []resourcev1.Device{{Name: "gpu-0"}},
				}},
			},
			want: map[DeviceID]Device{
				{Driver: "gpu.nvidia.com", Pool: "node-a", Device: "gpu-0"}: {
					ID:       DeviceID{Driver: "gpu.nvidia.com", Pool: "node-a", Device: "gpu-0"},
					NodeName: "node-a",
					Spec:     resourcev1.Device{Name: "gpu-0"},
				},
			},
		},
		"PerDeviceNode": {
			reason: "A device that names its own node is attached to that node. One in a slice that isn't local to a node isn't attached to any.",
			slices: []resourcev1.ResourceSlice{
				{Spec: resourcev1.ResourceSliceSpec{
					Driver:                 "gpu.nvidia.com",
					Pool:                   resourcev1.ResourcePool{Name: "pool", Generation: 1, ResourceSliceCount: 1},
					PerDeviceNodeSelection: new(true),
					Devices:                []resourcev1.Device{{Name: "gpu-0", NodeName: new("node-a")}},
				}},
				{Spec: resourcev1.ResourceSliceSpec{
					Driver:   "nic.example.com",
					Pool:     resourcev1.ResourcePool{Name: "fabric", Generation: 1, ResourceSliceCount: 1},
					AllNodes: new(true),
					Devices:  []resourcev1.Device{{Name: "nic-0"}},
				}},
			},
			want: map[DeviceID]Device{
				{Driver: "gpu.nvidia.com", Pool: "pool", Device: "gpu-0"}: {
					ID:       DeviceID{Driver: "gpu.nvidia.com", Pool: "pool", Device: "gpu-0"},
					NodeName: "node-a",
					Spec:     resourcev1.Device{Name: "gpu-0", NodeName: new("node-a")},
				},
				{Driver: "nic.example.com", Pool: "fabric", Device: "nic-0"}: {
					ID:   DeviceID{Driver: "nic.example.com", Pool: "fabric", Device: "nic-0"},
					Spec: resourcev1.Device{Name: "nic-0"},
				},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := Devices(tc.slices)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nDevices(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestAllocations(t *testing.T) {
	cases := map[string]struct {
		reason string
		claim  resourcev1.ResourceClaim
		want   []DeviceID
	}{
		"Unallocated": {
			reason: "An unallocated claim has no devices.",
			claim:  resourcev1.ResourceClaim{},
			want:   nil,
		},
		"AdminAccess": {
			reason: "A device allocated for admin access isn't consumed by the claim.",
			claim: resourcev1.ResourceClaim{Status: resourcev1.ResourceClaimStatus{
				Allocation: &resourcev1.AllocationResult{Devices: resourcev1.DeviceAllocationResult{
					Results: []resourcev1.DeviceRequestAllocationResult{
						{Request: "gpu", Driver: "gpu.nvidia.com", Pool: "node-a", Device: "gpu-0"},
						{Request: "monitor", Driver: "gpu.nvidia.com", Pool: "node-a", Device: "gpu-1", AdminAccess: new(true)},
					},
				}},
			}},
			want: []DeviceID{{Driver: "gpu.nvidia.com", Pool: "node-a", Device: "gpu-0"}},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := Allocations(&tc.claim)
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("\n%s\nAllocations(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}
