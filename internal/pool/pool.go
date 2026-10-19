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

// Package pool observes a GPU pool: the nodes Modelplane labels with the
// pool's name, the DRA devices they publish, and the pods waiting for them.
package pool

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/dynamic-resource-allocation/cel"

	"github.com/modelplaneai/cluster-agent/internal/dra"
)

// LabelPool is the node label Modelplane sets to the name of a node's pool.
// Modelplane pins a pod to a pool with a node selector on it.
const LabelPool = "modelplane.ai/pool"

// Cluster autoscaler pod event reasons. See
// https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/FAQ.md#what-events-are-emitted-by-ca
const (
	eventTriggeredScaleUp  = "TriggeredScaleUp"
	eventNotTriggerScaleUp = "NotTriggerScaleUp"
)

// celCacheSize bounds how many compiled DeviceClass selectors an Observer
// keeps. A cluster has a handful of DeviceClasses.
const celCacheSize = 128

// Inputs are the objects an Observer observes. Only the observed pool's nodes,
// the devices they publish, and the pods pinned to the pool count, so callers
// may supply objects from the whole cluster.
type Inputs struct {
	Nodes   []corev1.Node
	Slices  []resourcev1.ResourceSlice
	Classes []resourcev1.DeviceClass
	Claims  []resourcev1.ResourceClaim
	Pods    []corev1.Pod
	Events  []corev1.Event
}

// An Observation is what's true of a pool.
type Observation struct {
	// Nodes counts the pool's nodes.
	Nodes Nodes

	// Devices are the pool's devices by DeviceClass, sorted by class name.
	Devices []Devices

	// Free counts each of the pool's schedulable nodes' free devices, keyed
	// by node name.
	Free map[string]FreeDevices

	// TaintedDevices counts the pool's devices with a taint that keeps new
	// claims from allocating them, whatever their class.
	TaintedDevices int32

	// ExtendedResources sums the extended resources the pool's nodes
	// advertise as allocatable.
	ExtendedResources corev1.ResourceList

	// UnschedulablePods are the pods pinned to the pool that the scheduler
	// couldn't find a node for, sorted.
	UnschedulablePods []UnschedulablePod
}

// Nodes counts a pool's nodes.
type Nodes struct {
	Total int32

	// Ready counts the nodes whose Ready condition is True.
	Ready int32

	// Cordoned counts the nodes marked unschedulable, ready or not.
	Cordoned int32

	// Schedulable counts the ready nodes that aren't cordoned.
	Schedulable int32
}

// Devices are a pool's devices of one DeviceClass.
type Devices struct {
	DeviceClassName string

	// Nodes counts the pool's nodes that publish devices of the class.
	Nodes int32

	// Schedulable describes the class's devices on each schedulable node
	// that publishes some, keyed by node name.
	Schedulable map[string]NodeDevices

	// Attributes every device of the class in the pool agrees on.
	Attributes map[resourcev1.QualifiedName]resourcev1.DeviceAttribute
}

// NodeDevices counts one node's devices of a class.
type NodeDevices struct {
	// Allocatable counts the node's untainted devices.
	Allocatable int32

	// Allocated counts the allocatable devices a claim has allocated.
	Allocated int32
}

// FreeDevices counts one node's devices that are allocatable and not
// allocated, keyed by DeviceClass. A class the node publishes no devices of is
// absent.
type FreeDevices map[string]int32

// ScaleUp is the cluster autoscaler's latest verdict on an unschedulable pod.
type ScaleUp string

// Cluster autoscaler verdicts.
const (
	// ScaleUpUnknown means the autoscaler hasn't emitted an event about the
	// pod, perhaps because the cluster doesn't run one.
	ScaleUpUnknown ScaleUp = "Unknown"

	// ScaleUpTriggered means the autoscaler is adding nodes for the pod.
	ScaleUpTriggered ScaleUp = "Triggered"

	// ScaleUpNotTriggered means the autoscaler found no node group it could
	// grow to fit the pod.
	ScaleUpNotTriggered ScaleUp = "NotTriggered"
)

// An UnschedulablePod is a pod pinned to a pool that the scheduler couldn't
// find a node for.
type UnschedulablePod struct {
	types.NamespacedName

	// ScaleUp is the autoscaler's latest verdict on the pod.
	ScaleUp ScaleUp

	// ScaleUpMessage is the message of the event that carried the verdict.
	ScaleUpMessage string
}

// Membership records which nodes are in a pool.
type Membership struct {
	// Nodes is the set of the pool's nodes.
	Nodes map[string]bool

	// Schedulable is the set of the pool's nodes that are ready and not
	// cordoned.
	Schedulable map[string]bool
}

// An Observer observes pools.
type Observer struct {
	cel *cel.Cache
}

// NewObserver returns an Observer.
func NewObserver() *Observer {
	// Enable every CEL feature so the Observer can compile any expression
	// the API server accepted.
	return &Observer{cel: cel.NewCache(celCacheSize, cel.Features{
		EnableConsumableCapacity: true,
		EnableListTypeAttributes: true,
	})}
}

// ObservePool returns what's true of the named pool. A pool's nodes are those
// whose modelplane.ai/pool label is its name.
func (o *Observer) ObservePool(ctx context.Context, name string, in Inputs) (Observation, error) {
	var nodes []corev1.Node
	for _, n := range in.Nodes {
		if n.Labels[LabelPool] == name {
			nodes = append(nodes, n)
		}
	}
	m := Members(nodes)
	devices := dra.Devices(in.Slices)

	byClass, err := o.DevicesByClass(ctx, devices, in.Classes, Allocated(in.Claims), m)
	if err != nil {
		return Observation{}, err
	}
	return Observation{
		Nodes:             CountNodes(nodes),
		Devices:           byClass,
		Free:              Free(byClass, m),
		TaintedDevices:    TaintedDevices(devices, m),
		ExtendedResources: ExtendedResources(nodes),
		UnschedulablePods: UnschedulablePods(name, in.Pods, in.Events),
	}, nil
}

// Members returns the supplied nodes as a pool's members, and which of them
// are schedulable.
func Members(nodes []corev1.Node) Membership {
	m := Membership{Nodes: map[string]bool{}, Schedulable: map[string]bool{}}
	for _, n := range nodes {
		m.Nodes[n.Name] = true
		if IsReady(n) && !n.Spec.Unschedulable {
			m.Schedulable[n.Name] = true
		}
	}
	return m
}

// CountNodes counts a pool's nodes.
func CountNodes(nodes []corev1.Node) Nodes {
	c := Nodes{}
	for _, n := range nodes {
		c.Total++
		if n.Spec.Unschedulable {
			c.Cordoned++
		}
		if IsReady(n) {
			c.Ready++
			if !n.Spec.Unschedulable {
				c.Schedulable++
			}
		}
	}
	return c
}

// ExtendedResources sums the extended resources a pool's nodes advertise as
// allocatable.
func ExtendedResources(nodes []corev1.Node) corev1.ResourceList {
	var sums corev1.ResourceList
	for _, n := range nodes {
		for r, q := range n.Status.Allocatable {
			if !IsExtendedResource(r) {
				continue
			}
			if sums == nil {
				sums = corev1.ResourceList{}
			}
			sum := sums[r]
			sum.Add(q)
			sums[r] = sum
		}
	}
	return sums
}

// Allocated returns the devices the supplied claims have allocated.
func Allocated(claims []resourcev1.ResourceClaim) map[dra.DeviceID]bool {
	allocated := map[dra.DeviceID]bool{}
	for i := range claims {
		for _, id := range dra.Allocations(&claims[i]) {
			allocated[id] = true
		}
	}
	return allocated
}

// TaintedDevices counts a pool's devices that carry a taint that keeps new
// claims from allocating them.
func TaintedDevices(devices map[dra.DeviceID]dra.Device, m Membership) int32 {
	var tainted int32
	for _, d := range devices {
		if m.Nodes[d.NodeName] && d.Tainted() {
			tainted++
		}
	}
	return tainted
}

// DevicesByClass returns a pool's devices by the DeviceClasses they belong to,
// sorted by class name.
func (o *Observer) DevicesByClass(ctx context.Context, devices map[dra.DeviceID]dra.Device, classes []resourcev1.DeviceClass, allocated map[dra.DeviceID]bool, members Membership) ([]Devices, error) {
	byClass := map[string]*Devices{}
	publishes := map[string]map[string]bool{}

	for _, device := range devices {
		if !members.Nodes[device.NodeName] {
			continue
		}
		for _, dc := range classes {
			match, err := o.Matches(ctx, dc, device)
			if err != nil {
				return nil, fmt.Errorf("cannot match device %s/%s/%s against DeviceClass %q: %w", device.ID.Driver, device.ID.Pool, device.ID.Device, dc.Name, err)
			}
			if !match {
				continue
			}

			ds, ok := byClass[dc.Name]
			if !ok {
				// The device's attributes belong to the cache, so narrow a copy.
				ds = &Devices{DeviceClassName: dc.Name, Schedulable: map[string]NodeDevices{}, Attributes: maps.Clone(device.Spec.Attributes)}
				byClass[dc.Name] = ds
				publishes[dc.Name] = map[string]bool{}
			}
			publishes[dc.Name][device.NodeName] = true
			maps.DeleteFunc(ds.Attributes, func(name resourcev1.QualifiedName, a resourcev1.DeviceAttribute) bool {
				b, ok := device.Spec.Attributes[name]
				return !ok || !equality.Semantic.DeepEqual(a, b)
			})

			if !members.Schedulable[device.NodeName] {
				continue
			}
			// Record the node even if its devices are all tainted, so it
			// counts as a node with no free devices.
			nd := ds.Schedulable[device.NodeName]
			if !device.Tainted() {
				nd.Allocatable++
				if allocated[device.ID] {
					nd.Allocated++
				}
			}
			ds.Schedulable[device.NodeName] = nd
		}
	}

	out := make([]Devices, 0, len(byClass))
	for class, ds := range byClass {
		ds.Nodes = int32(len(publishes[class])) //nolint:gosec // A pool can't approach 2^31 nodes.
		out = append(out, *ds)
	}
	slices.SortFunc(out, func(a, b Devices) int { return strings.Compare(a.DeviceClassName, b.DeviceClassName) })
	return out, nil
}

// Free returns each schedulable node's free devices, keyed by node name. A
// schedulable node that publishes no devices has none free.
func Free(byClass []Devices, m Membership) map[string]FreeDevices {
	free := make(map[string]FreeDevices, len(m.Schedulable))
	for name := range m.Schedulable {
		free[name] = FreeDevices{}
	}
	for _, ds := range byClass {
		for name, nd := range ds.Schedulable {
			free[name][ds.DeviceClassName] = nd.Allocatable - nd.Allocated
		}
	}
	return free
}

// UnschedulablePods returns the unschedulable pods pinned to the named pool by
// a modelplane.ai/pool node selector, sorted, with the cluster autoscaler's
// latest verdict on each.
func UnschedulablePods(pool string, pods []corev1.Pod, events []corev1.Event) []UnschedulablePod {
	latest := LatestScaleUpEvents(events)
	var unschedulable []UnschedulablePod
	for _, pod := range pods {
		if pod.Spec.NodeSelector[LabelPool] != pool || !IsUnschedulable(pod) {
			continue
		}
		pp := UnschedulablePod{
			NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
			ScaleUp:        ScaleUpUnknown,
		}
		if e, ok := latest[pod.UID]; ok {
			pp.ScaleUp = ScaleUpNotTriggered
			if e.Reason == eventTriggeredScaleUp {
				pp.ScaleUp = ScaleUpTriggered
			}
			pp.ScaleUpMessage = e.Message
		}
		unschedulable = append(unschedulable, pp)
	}
	slices.SortFunc(unschedulable, func(a, b UnschedulablePod) int { return compareNamespacedName(a.NamespacedName, b.NamespacedName) })
	return unschedulable
}

// Matches returns true if the device belongs to the DeviceClass: it does when
// it satisfies every one of the class's selectors. It returns an error if it
// can't compile a selector.
func (o *Observer) Matches(ctx context.Context, dc resourcev1.DeviceClass, d dra.Device) (bool, error) {
	for _, s := range dc.Spec.Selectors {
		expr := o.cel.GetOrCompile(s.CEL.Expression)
		if expr.Error != nil {
			return false, expr.Error
		}
		match, _, err := expr.DeviceMatches(ctx, cel.Device{
			Driver:                   d.ID.Driver,
			AllowMultipleAllocations: d.Spec.AllowMultipleAllocations,
			Attributes:               d.Spec.Attributes,
			Capacity:                 d.Spec.Capacity,
		})
		// A selector that errors on a device, for example by reading an
		// attribute the device doesn't have, fails any allocation that
		// considers it. The scheduler can't allocate the device through the
		// class, so it doesn't belong to it.
		if err != nil || !match {
			return false, nil //nolint:nilerr // See above.
		}
	}
	return true, nil
}

// IsReady returns true if the node's Ready condition is True.
func IsReady(n corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// IsUnschedulable returns true if the scheduler couldn't find a node for the
// pod.
func IsUnschedulable(p corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodScheduled {
			return c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable
		}
	}
	return false
}

// IsExtendedResource returns true if the resource is an extended resource: a
// resource name with a domain other than kubernetes.io, like nvidia.com/gpu.
func IsExtendedResource(r corev1.ResourceName) bool {
	domain, _, ok := strings.Cut(string(r), "/")
	if !ok || strings.HasPrefix(string(r), corev1.DefaultResourceRequestsPrefix) {
		return false
	}
	return domain != "kubernetes.io" && !strings.HasSuffix(domain, ".kubernetes.io")
}

// LatestScaleUpEvents returns the cluster autoscaler's latest scale-up event
// about each pod, keyed by the pod's UID.
func LatestScaleUpEvents(events []corev1.Event) map[types.UID]corev1.Event {
	latest := map[types.UID]corev1.Event{}
	for _, e := range events {
		if e.Reason != eventTriggeredScaleUp && e.Reason != eventNotTriggerScaleUp {
			continue
		}
		uid := e.InvolvedObject.UID
		// Event times have one second precision, so break ties by name
		// rather than by the order the cache lists events in.
		if l, ok := latest[uid]; ok {
			if t, lt := EventTime(e), EventTime(l); t.Before(lt) || t.Equal(lt) && e.Name < l.Name {
				continue
			}
		}
		latest[uid] = e
	}
	return latest
}

// EventTime returns when an event last happened. Events reported through the
// older events API set lastTimestamp, and those reported through the newer
// one set eventTime and, once repeated, series.lastObservedTime.
func EventTime(e corev1.Event) time.Time {
	t := e.LastTimestamp.Time
	if e.EventTime.After(t) {
		t = e.EventTime.Time
	}
	if e.Series != nil && e.Series.LastObservedTime.After(t) {
		t = e.Series.LastObservedTime.Time
	}
	return t
}

func compareNamespacedName(a, b types.NamespacedName) int {
	return cmp.Or(strings.Compare(a.Namespace, b.Namespace), strings.Compare(a.Name, b.Name))
}
