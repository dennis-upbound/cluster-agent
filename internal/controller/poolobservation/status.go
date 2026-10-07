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
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/modelplaneai/cluster-agent/apis/v1alpha1"
	"github.com/modelplaneai/cluster-agent/internal/pool"
)

// Status returns the PoolObservation status that reports the supplied pool. A
// condition keeps its existing lastTransitionTime unless its status changed.
func Status(existing v1alpha1.PoolObservationStatus, generation int64, observed pool.Observation) v1alpha1.PoolObservationStatus {
	// Condition is a struct of values, so a shallow clone is a deep copy.
	status := v1alpha1.PoolObservationStatus{
		ObservedGeneration: generation,
		Conditions:         slices.Clone(existing.Conditions),
		Nodes: v1alpha1.NodeCounts{
			Total:       observed.Nodes.Total,
			Ready:       observed.Nodes.Ready,
			Schedulable: observed.Nodes.Schedulable,
		},
		UnschedulablePods: int32(len(observed.UnschedulablePods)), //nolint:gosec // A pool can't approach 2^31 unschedulable pods.
		ExtendedResources: observed.ExtendedResources,
	}
	for _, d := range observed.Devices {
		status.Devices = append(status.Devices, DeviceClassStatus(d))
	}

	healthy := Healthy(observed)
	healthy.ObservedGeneration = generation
	meta.SetStatusCondition(&status.Conditions, healthy)

	blocked := ScaleUpBlocked(observed)
	blocked.ObservedGeneration = generation
	meta.SetStatusCondition(&status.Conditions, blocked)

	return status
}

// FailedStatus returns the PoolObservation status that reports the pool
// couldn't be observed. Its conditions become Unknown, and it reports no
// nodes or devices.
func FailedStatus(existing v1alpha1.PoolObservationStatus, generation int64, err error) v1alpha1.PoolObservationStatus {
	// Condition is a struct of values, so a shallow clone is a deep copy.
	status := v1alpha1.PoolObservationStatus{
		ObservedGeneration: generation,
		Conditions:         slices.Clone(existing.Conditions),
	}
	for _, t := range []string{v1alpha1.TypeHealthy, v1alpha1.TypeScaleUpBlocked} {
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:               t,
			Status:             metav1.ConditionUnknown,
			Reason:             v1alpha1.ReasonObservationFailed,
			Message:            err.Error(),
			ObservedGeneration: generation,
		})
	}
	return status
}

// DeviceClassStatus returns the status that reports a pool's devices of one
// class.
func DeviceClassStatus(d pool.Devices) v1alpha1.DeviceClassStatus {
	s := v1alpha1.DeviceClassStatus{
		DeviceClassName: d.DeviceClassName,
		Nodes:           d.Nodes,
		Attributes:      d.Attributes,
	}

	nodes := map[int32]int32{}
	for _, nd := range d.Schedulable {
		s.Allocatable += nd.Allocatable
		s.Allocated += nd.Allocated
		nodes[nd.Allocatable-nd.Allocated]++
	}
	for _, free := range slices.Sorted(maps.Keys(nodes)) {
		s.NodesByFreeDevices = append(s.NodesByFreeDevices, v1alpha1.FreeDevicesBucket{
			FreeDevices: free,
			Nodes:       nodes[free],
		})
	}

	return s
}

// Healthy returns a pool's Healthy condition. It's False when any of the
// pool's nodes is not ready or cordoned, or any of its devices is tainted.
// The reason names the first of those problems, and the message all of them.
func Healthy(p pool.Observation) metav1.Condition {
	var reason string
	var problems []string

	if n := p.Nodes.Total - p.Nodes.Ready; n > 0 {
		reason = v1alpha1.ReasonNodesNotReady
		problems = append(problems, fmt.Sprintf("%d of %d nodes not ready", n, p.Nodes.Total))
	}
	if n := p.Nodes.Cordoned; n > 0 {
		reason = cmp.Or(reason, v1alpha1.ReasonNodesCordoned)
		problems = append(problems, fmt.Sprintf("%d of %d nodes cordoned", n, p.Nodes.Total))
	}
	if p.TaintedDevices > 0 {
		reason = cmp.Or(reason, v1alpha1.ReasonDevicesTainted)
		problems = append(problems, fmt.Sprintf("%d devices tainted", p.TaintedDevices))
	}

	if reason == "" {
		return metav1.Condition{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy}
	}
	return metav1.Condition{
		Type:    v1alpha1.TypeHealthy,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: strings.Join(problems, "; "),
	}
}

// ScaleUpBlocked returns a pool's ScaleUpBlocked condition. It's True when
// the cluster autoscaler's latest verdict on any of the pool's unschedulable
// pods is that it can't add a node for it.
func ScaleUpBlocked(p pool.Observation) metav1.Condition {
	cond := metav1.Condition{Type: v1alpha1.TypeScaleUpBlocked}

	if len(p.UnschedulablePods) == 0 {
		cond.Status = metav1.ConditionFalse
		cond.Reason = v1alpha1.ReasonNoUnschedulablePods
		return cond
	}

	if i := slices.IndexFunc(p.UnschedulablePods, func(pp pool.UnschedulablePod) bool { return pp.ScaleUp == pool.ScaleUpNotTriggered }); i >= 0 {
		pp := p.UnschedulablePods[i]
		cond.Status = metav1.ConditionTrue
		cond.Reason = v1alpha1.ReasonScaleUpNotTriggered
		cond.Message = fmt.Sprintf("pod %s: %s", pp.NamespacedName, pp.ScaleUpMessage)
		return cond
	}

	if i := slices.IndexFunc(p.UnschedulablePods, func(pp pool.UnschedulablePod) bool { return pp.ScaleUp == pool.ScaleUpTriggered }); i >= 0 {
		pp := p.UnschedulablePods[i]
		cond.Status = metav1.ConditionFalse
		cond.Reason = v1alpha1.ReasonScaleUpTriggered
		cond.Message = fmt.Sprintf("pod %s: %s", pp.NamespacedName, pp.ScaleUpMessage)
		return cond
	}

	cond.Status = metav1.ConditionUnknown
	cond.Reason = v1alpha1.ReasonNoAutoscalerEvents
	cond.Message = fmt.Sprintf("%d pods are unschedulable, but no cluster autoscaler has reported on them", len(p.UnschedulablePods))
	return cond
}
