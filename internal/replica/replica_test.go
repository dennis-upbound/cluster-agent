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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"

	"github.com/modelplaneai/cluster-agent/internal/dra"
)

// The testdata directory holds objects captured from an EKS cluster with one
// g6.12xlarge node (4 NVIDIA L4 GPUs). Each replica in the ml-test namespace
// is one pod labelled with modelplane.ai/replica:
//
//   - one-gpu and two-gpu are running with 1 and 2 GPUs.
//   - too-big claims 8 GPUs, so it can't be scheduled.
//   - bad-image is scheduled but can't pull its image.
func TestObserve(t *testing.T) {
	cases := map[string]struct {
		reason  string
		replica string
		want    Observation
	}{
		"OneGPU": {
			reason:  "A running replica should report its pod and the device allocated to it.",
			replica: "one-gpu",
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
			},
		},
		"TwoGPU": {
			reason:  "A replica with two devices should report both, sorted.",
			replica: "two-gpu",
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
			},
		},
		"Unschedulable": {
			reason:  "A pod the scheduler can't place should report why, and have no devices.",
			replica: "too-big",
			want: Observation{
				Pods: []Pod{
					{
						Name:              "too-big-5859cf75cf-jzm8n",
						Scheduling:        Unschedulable,
						SchedulingMessage: "0/2 nodes are available: 1 cannot allocate all claims, 1 node(s) didn't match Pod's node affinity/selector. still not schedulable, preemption: 0/2 nodes are available: 2 Preemption is not helpful for scheduling.",
					},
				},
			},
		},
		"ImagePullBackOff": {
			reason:  "A scheduled pod whose container can't start should report why it's waiting.",
			replica: "bad-image",
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
	}

	pods := load[corev1.PodList](t, filepath.Join("testdata", "pods.json")).Items
	claims := load[resourcev1.ResourceClaimList](t, filepath.Join("testdata", "claims.json")).Items
	slices := load[resourcev1.ResourceSliceList](t, filepath.Join("testdata", "slices.json")).Items

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// Select the replica's pods the way a ReplicaObservation's pod
			// selector does.
			var selected []corev1.Pod
			for _, p := range pods {
				if p.Labels["modelplane.ai/replica"] == tc.replica {
					selected = append(selected, p)
				}
			}

			got := Observe(Inputs{Pods: selected, Claims: claims, Slices: slices})
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("\n%s\nObserve(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
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
