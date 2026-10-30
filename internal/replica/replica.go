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

// Package replica observes a replica: its pods, and the DRA devices allocated
// to them.
package replica

import (
	"cmp"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/modelplaneai/cluster-agent/internal/dra"
)

// LabelReplica is the pod label Modelplane sets to the name of the replica a
// pod belongs to.
const LabelReplica = "modelplane.ai/replica"

// Inputs are the objects Observe observes.
type Inputs struct {
	// Pods are the replica's pods.
	Pods []corev1.Pod

	// Claims are ResourceClaims in the replica's namespace. Only those the
	// pods reference are observed.
	Claims []resourcev1.ResourceClaim

	// Slices are ResourceSlices. Only those of the DRA pools the claims
	// allocated devices from are observed.
	Slices []resourcev1.ResourceSlice

	// NodePools is the pool each node belongs to, keyed by node name.
	NodePools map[string]string

	// AllocatedAt is when the agent first saw each allocated claim, keyed by
	// the claim's UID.
	AllocatedAt map[types.UID]time.Time
}

// An Observation is what's true of a replica.
type Observation struct {
	// Pods are the replica's pods that haven't run to completion, sorted by
	// name.
	Pods []Pod

	// Devices are the devices allocated to the replica's pods, sorted.
	Devices []Device

	// Pools are the pools whose nodes the devices are attached to, sorted by
	// name.
	Pools []Pool
}

// A Pool is a pool whose nodes some of a replica's devices are attached to.
type Pool struct {
	Name string

	// LastAllocation is the latest time the agent first saw one of the
	// replica's claims that hold devices on the pool allocated.
	LastAllocation time.Time
}

// Scheduling is how far the scheduler got placing a pod.
type Scheduling string

// Scheduling states.
const (
	// Scheduled pods are bound to a node.
	Scheduled Scheduling = "Scheduled"

	// Unschedulable pods are ones the scheduler couldn't find a node for.
	Unschedulable Scheduling = "Unschedulable"

	// Gated pods carry a scheduling gate, so the scheduler won't try to
	// place them yet.
	Gated Scheduling = "Gated"

	// Pending pods are ones the scheduler hasn't tried to place yet.
	Pending Scheduling = "Pending"
)

// A Pod is one of a replica's pods.
type Pod struct {
	Name string

	// Scheduling is how far the scheduler got placing the pod.
	Scheduling Scheduling

	// SchedulingMessage is the message of the pod's PodScheduled condition.
	SchedulingMessage string

	// Ready is true if the pod's Ready condition is True.
	Ready bool

	// Waiting are the pod's containers that are waiting to run, init
	// containers first.
	Waiting []Waiting

	// Restarts counts the times the kubelet restarted the pod's containers,
	// init containers included.
	Restarts int32

	// LastTermination is the most recent termination of one of the pod's
	// containers that was restarted, or nil if none was.
	LastTermination *Termination

	// UnallocatedClaims are the names of the ResourceClaims the pod uses that
	// the agent hasn't seen allocated, because they aren't allocated or it
	// can't find them.
	UnallocatedClaims []string
}

// Waiting is a container that's waiting to run.
type Waiting struct {
	Container string
	Reason    string
	Message   string
}

// A Termination is a container that terminated and was restarted.
type Termination struct {
	Container  string
	Reason     string
	ExitCode   int32
	FinishedAt time.Time
}

// A Device is a device allocated to one of a replica's pods.
type Device struct {
	ID dra.DeviceID

	// NodeName is the node the device is attached to. It's empty for a device
	// that isn't attached to one node, or that no ResourceSlice publishes.
	NodeName string

	// UUID is the device's uuid attribute, when its driver publishes one.
	UUID string
}

// Observe returns what's true of the replica the supplied objects describe.
func Observe(inputs Inputs) Observation {
	claims := make(map[string]*resourcev1.ResourceClaim, len(inputs.Claims))
	for i := range inputs.Claims {
		claims[inputs.Claims[i].Namespace+"/"+inputs.Claims[i].Name] = &inputs.Claims[i]
	}

	observation := Observation{}
	// Several pods can share a claim.
	allocated := map[*resourcev1.ResourceClaim]bool{}
	for _, p := range inputs.Pods {
		if Finished(p) {
			continue
		}
		pod := ObservePod(p)
		for _, name := range ClaimNames(p) {
			c, ok := claims[p.Namespace+"/"+name]
			if !ok || c.Status.Allocation == nil {
				pod.UnallocatedClaims = append(pod.UnallocatedClaims, name)
				continue
			}
			allocated[c] = true
		}
		observation.Pods = append(observation.Pods, pod)
	}

	// A replica's devices come from a few of the cluster's DRA pools, so only
	// index the devices those pools publish.
	type driverPool struct{ driver, pool string }
	draPools := map[driverPool]bool{}
	for c := range allocated {
		for _, id := range dra.Allocations(c) {
			draPools[driverPool{driver: id.Driver, pool: id.Pool}] = true
		}
	}
	var published []resourcev1.ResourceSlice
	for _, s := range inputs.Slices {
		if draPools[driverPool{driver: s.Spec.Driver, pool: s.Spec.Pool.Name}] {
			published = append(published, s)
		}
	}
	devices := dra.Devices(published)

	seen := map[dra.DeviceID]bool{}
	latest := map[string]time.Time{}
	for c := range allocated {
		for _, id := range dra.Allocations(c) {
			pd := devices[id]
			if pool, ok := inputs.NodePools[pd.NodeName]; ok && inputs.AllocatedAt[c.UID].After(latest[pool]) {
				latest[pool] = inputs.AllocatedAt[c.UID]
			}
			if seen[id] {
				continue
			}
			seen[id] = true
			d := Device{ID: id, NodeName: pd.NodeName}
			d.UUID, _ = pd.StringAttribute("uuid")
			observation.Devices = append(observation.Devices, d)
		}
	}
	for name, t := range latest {
		observation.Pools = append(observation.Pools, Pool{Name: name, LastAllocation: t})
	}

	slices.SortFunc(observation.Pods, func(a, b Pod) int { return strings.Compare(a.Name, b.Name) })
	slices.SortFunc(observation.Pools, func(a, b Pool) int { return strings.Compare(a.Name, b.Name) })
	slices.SortFunc(observation.Devices, func(a, b Device) int {
		return cmp.Or(
			strings.Compare(a.ID.Driver, b.ID.Driver),
			strings.Compare(a.ID.Pool, b.ID.Pool),
			strings.Compare(a.ID.Device, b.ID.Device),
		)
	})
	return observation
}

// Finished returns true if the pod has run to completion, like an evicted pod.
// A finished pod holds no devices, and its controller replaces it, so it's no
// longer one of its replica's pods.
func Finished(p corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
}

// ObservePod returns what's true of a pod.
func ObservePod(p corev1.Pod) Pod {
	observed := Pod{Name: p.Name, Scheduling: Pending}

	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodScheduled {
			observed.SchedulingMessage = c.Message
			if c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable {
				observed.Scheduling = Unschedulable
			}
		}
		if c.Type == corev1.PodReady {
			observed.Ready = c.Status == corev1.ConditionTrue
		}
	}
	if len(p.Spec.SchedulingGates) > 0 {
		observed.Scheduling = Gated
	}
	if p.Spec.NodeName != "" {
		observed.Scheduling = Scheduled
	}

	for _, cs := range slices.Concat(p.Status.InitContainerStatuses, p.Status.ContainerStatuses) {
		observed.Restarts += cs.RestartCount
		if t := cs.LastTerminationState.Terminated; t != nil && (observed.LastTermination == nil || t.FinishedAt.After(observed.LastTermination.FinishedAt)) {
			observed.LastTermination = &Termination{
				Container:  cs.Name,
				Reason:     t.Reason,
				ExitCode:   t.ExitCode,
				FinishedAt: t.FinishedAt.Time,
			}
		}
		if cs.State.Waiting == nil {
			continue
		}
		observed.Waiting = append(observed.Waiting, Waiting{
			Container: cs.Name,
			Reason:    cs.State.Waiting.Reason,
			Message:   cs.State.Waiting.Message,
		})
	}

	return observed
}

// ClaimNames returns the names of the ResourceClaims a pod uses: those it
// names in its spec, those generated from its ResourceClaimTemplates, and the
// one generated for any extended resources it requests that DRA provides.
func ClaimNames(p corev1.Pod) []string {
	var names []string
	for _, c := range p.Spec.ResourceClaims {
		if c.ResourceClaimName != nil {
			names = append(names, *c.ResourceClaimName)
		}
	}
	for _, cs := range p.Status.ResourceClaimStatuses {
		if cs.ResourceClaimName != nil {
			names = append(names, *cs.ResourceClaimName)
		}
	}
	if s := p.Status.ExtendedResourceClaimStatus; s != nil {
		names = append(names, s.ResourceClaimName)
	}
	return names
}
