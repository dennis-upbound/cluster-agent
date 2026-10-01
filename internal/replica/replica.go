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

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"

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
}

// An Observation is what's true of a replica.
type Observation struct {
	// Pods are the replica's pods, sorted by name.
	Pods []Pod

	// Devices are the devices allocated to the replica's pods, sorted.
	Devices []Device
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
}

// Waiting is a container that's waiting to run.
type Waiting struct {
	Container string
	Reason    string
	Message   string
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
func Observe(in Inputs) Observation {
	claims := make(map[string]*resourcev1.ResourceClaim, len(in.Claims))
	for i := range in.Claims {
		claims[in.Claims[i].Namespace+"/"+in.Claims[i].Name] = &in.Claims[i]
	}

	observation := Observation{}
	allocated := map[dra.DeviceID]bool{}
	for _, p := range in.Pods {
		observation.Pods = append(observation.Pods, ObservePod(p))
		for _, name := range ClaimNames(p) {
			if c, ok := claims[p.Namespace+"/"+name]; ok {
				for _, id := range dra.Allocations(c) {
					allocated[id] = true
				}
			}
		}
	}

	// A replica's devices come from a few of the cluster's DRA pools, so only
	// index the devices those pools publish.
	type driverPool struct{ driver, pool string }
	pools := map[driverPool]bool{}
	for id := range allocated {
		pools[driverPool{driver: id.Driver, pool: id.Pool}] = true
	}
	var published []resourcev1.ResourceSlice
	for _, s := range in.Slices {
		if pools[driverPool{driver: s.Spec.Driver, pool: s.Spec.Pool.Name}] {
			published = append(published, s)
		}
	}
	devices := dra.Devices(published)

	for id := range allocated {
		d := Device{ID: id}
		if pd, ok := devices[id]; ok {
			d.NodeName = pd.NodeName
			d.UUID, _ = pd.StringAttribute("uuid")
		}
		observation.Devices = append(observation.Devices, d)
	}

	slices.SortFunc(observation.Pods, func(a, b Pod) int { return strings.Compare(a.Name, b.Name) })
	slices.SortFunc(observation.Devices, func(a, b Device) int {
		return cmp.Or(
			strings.Compare(a.ID.Driver, b.ID.Driver),
			strings.Compare(a.ID.Pool, b.ID.Pool),
			strings.Compare(a.ID.Device, b.ID.Device),
		)
	})
	return observation
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
