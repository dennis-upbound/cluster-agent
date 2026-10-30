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

package replica

import (
	"encoding/json"
	"maps"
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

var equateQuantities = cmp.Comparer(func(a, b resource.Quantity) bool { return a.Cmp(b) == 0 })

// The testdata directory holds objects captured from an EKS cluster with one
// g6.12xlarge node (4 NVIDIA L4 GPUs). Each replica in the ml-test namespace
// is one pod labelled with modelplane.ai/replica:
//
//   - one-gpu and two-gpu are running with 1 and 2 GPUs.
//   - too-big claims 8 GPUs, so it can't be scheduled.
//   - bad-image is scheduled but can't pull its image.
func TestObserve(t *testing.T) {
	pods := load[corev1.PodList](t, filepath.Join("testdata", "pods.json")).Items
	claims := load[resourcev1.ResourceClaimList](t, filepath.Join("testdata", "claims.json")).Items
	rslices := load[resourcev1.ResourceSliceList](t, filepath.Join("testdata", "slices.json")).Items

	start := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	nodePools := map[string]string{
		"ip-10-0-54-161.ec2.internal": "gpu-l4x4-a",
		"ip-10-0-69-153.ec2.internal": "system",
	}
	// When the agent first saw each of the allocated claims.
	allocatedAt := map[types.UID]time.Time{
		"d18cf08e-3135-4b08-acf0-0db06605d2eb": start,                  // one-gpu-57bf7796f9-f6vrt
		"bd5b9231-19f7-4ebb-92c9-6a9a990e8c89": start.Add(time.Second), // two-gpu-7f7ff797d7-vs2w9
	}

	// The captured pods, grouped by the replica they belong to.
	selected := map[string][]corev1.Pod{}
	for _, p := range pods {
		r := p.Labels["modelplane.ai/replica"]
		selected[r] = append(selected[r], p)
	}

	cases := map[string]struct {
		reason string
		in     Inputs
		want   Observation
	}{
		"OneGPU": {
			reason: "A running replica should report its pod, the device allocated to it, and when the device's pool was allocated to it.",
			in:     Inputs{Pods: selected["one-gpu"], Claims: claims, Slices: rslices, NodePools: nodePools, AllocatedAt: allocatedAt},
			want: Observation{
				Pods: []Pod{
					{Name: "one-gpu-57bf7796f9-f6vrt", Scheduling: Scheduled, Ready: true},
				},
				Devices: []Device{
					{
						ID:       dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "ip-10-0-54-161.ec2.internal", Device: "gpu-2"},
						NodeName: "ip-10-0-54-161.ec2.internal",
						UUID:     "GPU-22d002d7-9f44-8df8-be6a-ed374d260c75",
					},
				},
				Pools: []Pool{{Name: "gpu-l4x4-a", LastAllocation: start}},
			},
		},
		"TwoGPU": {
			reason: "A replica with two devices should report both, sorted.",
			in:     Inputs{Pods: selected["two-gpu"], Claims: claims, Slices: rslices, NodePools: nodePools, AllocatedAt: allocatedAt},
			want: Observation{
				Pods: []Pod{
					{Name: "two-gpu-7f7ff797d7-vs2w9", Scheduling: Scheduled, Ready: true},
				},
				Devices: []Device{
					{
						ID:       dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "ip-10-0-54-161.ec2.internal", Device: "gpu-0"},
						NodeName: "ip-10-0-54-161.ec2.internal",
						UUID:     "GPU-69cc0a42-63bd-f2b2-4188-90f6a6345830",
					},
					{
						ID:       dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "ip-10-0-54-161.ec2.internal", Device: "gpu-3"},
						NodeName: "ip-10-0-54-161.ec2.internal",
						UUID:     "GPU-df9aa0e4-eeb5-f9ec-3eb8-2c20dd4df42d",
					},
				},
				Pools: []Pool{{Name: "gpu-l4x4-a", LastAllocation: start.Add(time.Second)}},
			},
		},
		"Unschedulable": {
			reason: "A pod the scheduler can't place should report why, and have no devices or allocated claims.",
			in:     Inputs{Pods: selected["too-big"], Claims: claims, Slices: rslices, NodePools: nodePools, AllocatedAt: allocatedAt},
			want: Observation{
				Pods: []Pod{
					{
						Name:              "too-big-5859cf75cf-jzm8n",
						Scheduling:        Unschedulable,
						SchedulingMessage: "0/2 nodes are available: 1 cannot allocate all claims, 1 node(s) didn't match Pod's node affinity/selector. still not schedulable, preemption: 0/2 nodes are available: 2 Preemption is not helpful for scheduling.",
						UnallocatedClaims: []string{"too-big-5859cf75cf-jzm8n-gpu-86w5w"},
					},
				},
			},
		},
		"ImagePullBackOff": {
			reason: "A scheduled pod whose container can't start should report why it's waiting.",
			in:     Inputs{Pods: selected["bad-image"], Claims: claims, Slices: rslices, NodePools: nodePools, AllocatedAt: allocatedAt},
			want: Observation{
				Pods: []Pod{
					{
						Name:       "bad-image-6cc4ddc98f-6gm5s",
						Scheduling: Scheduled,
						Waiting: []Waiting{
							{
								Container: "engine",
								Reason:    "ImagePullBackOff",
								Message:   `Back-off pulling image "registry.k8s.io/does-not-exist:v0.0.0": ErrImagePull: rpc error: code = NotFound desc = failed to pull and unpack image "registry.k8s.io/does-not-exist:v0.0.0": failed to resolve reference "registry.k8s.io/does-not-exist:v0.0.0": registry.k8s.io/does-not-exist:v0.0.0: not found`,
							},
						},
					},
				},
			},
		},
		"OOMKilled": {
			reason: "A pod whose containers restarted should count their restarts and report the most recent termination, like an engine that was OOMKilled.",
			in: Inputs{
				Pods: []corev1.Pod{{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0"},
					Spec:       corev1.PodSpec{NodeName: "node-a"},
					Status: corev1.PodStatus{
						InitContainerStatuses: []corev1.ContainerStatus{{
							Name:         "proxy",
							RestartCount: 1,
							LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
								Reason:     "Error",
								ExitCode:   1,
								FinishedAt: metav1.NewTime(start),
							}},
							State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
						}},
						ContainerStatuses: []corev1.ContainerStatus{{
							Name:         "engine",
							RestartCount: 3,
							LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
								Reason:     "OOMKilled",
								ExitCode:   137,
								FinishedAt: metav1.NewTime(start.Add(time.Minute)),
							}},
							State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
								Reason:  "CrashLoopBackOff",
								Message: "back-off 40s restarting failed container=engine pod=llama-0_ml",
							}},
						}},
					},
				}},
			},
			want: Observation{
				Pods: []Pod{{
					Name:       "llama-0",
					Scheduling: Scheduled,
					Waiting: []Waiting{{
						Container: "engine",
						Reason:    "CrashLoopBackOff",
						Message:   "back-off 40s restarting failed container=engine pod=llama-0_ml",
					}},
					Restarts: 4,
					LastTermination: &Termination{
						Container:  "engine",
						Reason:     "OOMKilled",
						ExitCode:   137,
						FinishedAt: start.Add(time.Minute),
					},
				}},
			},
		},
		"NetworkAttached": {
			reason: "A device that isn't attached to one node should report no node, though its pod is on one.",
			in: Inputs{
				Pods: []corev1.Pod{{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0"},
					Spec: corev1.PodSpec{
						NodeName:       "node-a",
						ResourceClaims: []corev1.PodResourceClaim{{Name: "nic", ResourceClaimName: new("llama-nic")}},
					},
				}},
				Claims: []resourcev1.ResourceClaim{{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-nic"},
					Status: resourcev1.ResourceClaimStatus{Allocation: &resourcev1.AllocationResult{
						Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{
							{Request: "nic", Driver: "nic.example.org", Pool: "fabric", Device: "nic-0"},
						}},
					}},
				}},
				Slices: []resourcev1.ResourceSlice{{
					Spec: resourcev1.ResourceSliceSpec{
						Driver:   "nic.example.org",
						Pool:     resourcev1.ResourcePool{Name: "fabric", ResourceSliceCount: 1},
						AllNodes: new(true),
						Devices:  []resourcev1.Device{{Name: "nic-0"}},
					},
				}},
			},
			want: Observation{
				Pods:    []Pod{{Name: "llama-0", Scheduling: Scheduled}},
				Devices: []Device{{ID: dra.DeviceID{Driver: "nic.example.org", Pool: "fabric", Device: "nic-0"}}},
			},
		},
		"ClaimNotSeen": {
			reason: "A bound pod whose claim the agent can't find yet should report the claim unallocated.",
			in: Inputs{
				Pods: []corev1.Pod{{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0"},
					Spec: corev1.PodSpec{
						NodeName:       "node-a",
						ResourceClaims: []corev1.PodResourceClaim{{Name: "gpu", ResourceClaimName: new("llama-0-gpu")}},
					},
				}},
			},
			want: Observation{
				Pods: []Pod{{Name: "llama-0", Scheduling: Scheduled, UnallocatedClaims: []string{"llama-0-gpu"}}},
			},
		},
		"Finished": {
			reason: "Pods that have run to completion, like an evicted one, shouldn't be observed, nor should a claim one still names.",
			in: Inputs{
				Pods: []corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0"},
						Spec: corev1.PodSpec{
							NodeName:       "node-a",
							ResourceClaims: []corev1.PodResourceClaim{{Name: "gpu", ResourceClaimName: new("llama-0-gpu")}},
						},
						Status: corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted"},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-1"},
						Spec:       corev1.PodSpec{NodeName: "node-a"},
						Status:     corev1.PodStatus{Phase: corev1.PodSucceeded},
					},
				},
				// The claim hasn't been deallocated yet.
				Claims: []resourcev1.ResourceClaim{{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0-gpu", UID: "llama-0-gpu"},
					Status: resourcev1.ResourceClaimStatus{Allocation: &resourcev1.AllocationResult{
						Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{
							{Request: "gpu", Driver: "gpu.nvidia.com", Pool: "node-a", Device: "gpu-0"},
						}},
					}},
				}},
				Slices: []resourcev1.ResourceSlice{{Spec: resourcev1.ResourceSliceSpec{
					Driver:   "gpu.nvidia.com",
					Pool:     resourcev1.ResourcePool{Name: "node-a", ResourceSliceCount: 1},
					NodeName: new("node-a"),
					Devices:  []resourcev1.Device{{Name: "gpu-0"}},
				}}},
				NodePools:   map[string]string{"node-a": "gpu"},
				AllocatedAt: map[types.UID]time.Time{"llama-0-gpu": start},
			},
			want: Observation{},
		},
		"SeveralPools": {
			reason: "A replica should report the latest allocation of its claims on each pool, but not on a node in no pool.",
			in: Inputs{
				Pods: []corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "prefill-0"},
						Spec: corev1.PodSpec{
							NodeName:       "node-a",
							ResourceClaims: []corev1.PodResourceClaim{{Name: "gpu", ResourceClaimName: new("prefill-0-gpu")}},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "prefill-1"},
						Spec: corev1.PodSpec{
							NodeName:       "node-a",
							ResourceClaims: []corev1.PodResourceClaim{{Name: "gpu", ResourceClaimName: new("prefill-1-gpu")}},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "decode-0"},
						Spec: corev1.PodSpec{
							NodeName:       "node-b",
							ResourceClaims: []corev1.PodResourceClaim{{Name: "gpu", ResourceClaimName: new("decode-0-gpu")}},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "decode-1"},
						Spec: corev1.PodSpec{
							NodeName:       "node-c",
							ResourceClaims: []corev1.PodResourceClaim{{Name: "gpu", ResourceClaimName: new("decode-1-gpu")}},
						},
					},
				},
				Claims: []resourcev1.ResourceClaim{
					{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "prefill-0-gpu", UID: "prefill-0-gpu"},
						Status: resourcev1.ResourceClaimStatus{Allocation: &resourcev1.AllocationResult{
							Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{
								{Request: "gpu", Driver: "gpu.nvidia.com", Pool: "node-a", Device: "gpu-0"},
							}},
						}},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "prefill-1-gpu", UID: "prefill-1-gpu"},
						Status: resourcev1.ResourceClaimStatus{Allocation: &resourcev1.AllocationResult{
							Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{
								{Request: "gpu", Driver: "gpu.nvidia.com", Pool: "node-a", Device: "gpu-1"},
							}},
						}},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "decode-0-gpu", UID: "decode-0-gpu"},
						Status: resourcev1.ResourceClaimStatus{Allocation: &resourcev1.AllocationResult{
							Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{
								{Request: "gpu", Driver: "gpu.nvidia.com", Pool: "node-b", Device: "gpu-0"},
							}},
						}},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "decode-1-gpu", UID: "decode-1-gpu"},
						Status: resourcev1.ResourceClaimStatus{Allocation: &resourcev1.AllocationResult{
							Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{
								{Request: "gpu", Driver: "gpu.nvidia.com", Pool: "node-c", Device: "gpu-0"},
							}},
						}},
					},
				},
				Slices: []resourcev1.ResourceSlice{
					{Spec: resourcev1.ResourceSliceSpec{
						Driver:   "gpu.nvidia.com",
						Pool:     resourcev1.ResourcePool{Name: "node-a", ResourceSliceCount: 1},
						NodeName: new("node-a"),
						Devices:  []resourcev1.Device{{Name: "gpu-0"}, {Name: "gpu-1"}},
					}},
					{Spec: resourcev1.ResourceSliceSpec{
						Driver:   "gpu.nvidia.com",
						Pool:     resourcev1.ResourcePool{Name: "node-b", ResourceSliceCount: 1},
						NodeName: new("node-b"),
						Devices:  []resourcev1.Device{{Name: "gpu-0"}},
					}},
					{Spec: resourcev1.ResourceSliceSpec{
						Driver:   "gpu.nvidia.com",
						Pool:     resourcev1.ResourcePool{Name: "node-c", ResourceSliceCount: 1},
						NodeName: new("node-c"),
						Devices:  []resourcev1.Device{{Name: "gpu-0"}},
					}},
				},
				NodePools: map[string]string{"node-a": "prefill", "node-b": "decode"},
				AllocatedAt: map[types.UID]time.Time{
					"prefill-0-gpu": start.Add(2 * time.Second),
					"prefill-1-gpu": start,
					"decode-0-gpu":  start.Add(time.Second),
					"decode-1-gpu":  start.Add(3 * time.Second),
				},
			},
			want: Observation{
				Pods: []Pod{
					{Name: "decode-0", Scheduling: Scheduled},
					{Name: "decode-1", Scheduling: Scheduled},
					{Name: "prefill-0", Scheduling: Scheduled},
					{Name: "prefill-1", Scheduling: Scheduled},
				},
				Devices: []Device{
					{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "node-a", Device: "gpu-0"}, NodeName: "node-a"},
					{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "node-a", Device: "gpu-1"}, NodeName: "node-a"},
					{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "node-b", Device: "gpu-0"}, NodeName: "node-b"},
					{ID: dra.DeviceID{Driver: "gpu.nvidia.com", Pool: "node-c", Device: "gpu-0"}, NodeName: "node-c"},
				},
				Pools: []Pool{
					{Name: "decode", LastAllocation: start.Add(time.Second)},
					{Name: "prefill", LastAllocation: start.Add(2 * time.Second)},
				},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			before := deepCopy(tc.in)
			got := Observe(tc.in)
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("\n%s\nObserve(...): -want, +got:\n%s", tc.reason, diff)
			}
			// The agent reads inputs straight from its cache.
			if diff := cmp.Diff(before, tc.in, equateQuantities); diff != "" {
				t.Errorf("\n%s\nObserve(...): modified its inputs: -before, +after:\n%s", tc.reason, diff)
			}
		})
	}
}

func deepCopy(in Inputs) Inputs {
	out := Inputs{NodePools: maps.Clone(in.NodePools), AllocatedAt: maps.Clone(in.AllocatedAt)}
	for i := range in.Pods {
		out.Pods = append(out.Pods, *in.Pods[i].DeepCopy())
	}
	for i := range in.Claims {
		out.Claims = append(out.Claims, *in.Claims[i].DeepCopy())
	}
	for i := range in.Slices {
		out.Slices = append(out.Slices, *in.Slices[i].DeepCopy())
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

func TestClaimNames(t *testing.T) {
	cases := map[string]struct {
		reason string
		pod    corev1.Pod
		want   []string
	}{
		"EverySource": {
			reason: "A pod uses the claims it names, those generated from its templates, and the one generated for its DRA extended resources.",
			pod: corev1.Pod{
				Spec: corev1.PodSpec{ResourceClaims: []corev1.PodResourceClaim{
					{Name: "shared", ResourceClaimName: new("shared-gpus")},
					{Name: "gpu", ResourceClaimTemplateName: new("gpu")},
				}},
				Status: corev1.PodStatus{
					ResourceClaimStatuses: []corev1.PodResourceClaimStatus{
						{Name: "gpu", ResourceClaimName: new("llama-0-gpu-abcde")},
						{Name: "unneeded"},
					},
					ExtendedResourceClaimStatus: &corev1.PodExtendedResourceClaimStatus{ResourceClaimName: "llama-0-extended-resources-fghij"},
				},
			},
			want: []string{"shared-gpus", "llama-0-gpu-abcde", "llama-0-extended-resources-fghij"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := ClaimNames(tc.pod)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nClaimNames(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}
