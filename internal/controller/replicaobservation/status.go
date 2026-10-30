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
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/modelplaneai/cluster-agent/apis/v1alpha1"
	"github.com/modelplaneai/cluster-agent/internal/replica"
)

// Container waiting reasons that mean a container won't start without help.
// The kubelet uses these.
const (
	waitingErrImagePull               = "ErrImagePull"
	waitingImagePullBackOff           = "ImagePullBackOff"
	waitingCrashLoopBackOff           = "CrashLoopBackOff"
	waitingInvalidImageName           = "InvalidImageName"
	waitingCreateContainerConfigError = "CreateContainerConfigError"
	waitingCreateContainerError       = "CreateContainerError"
)

// Status returns the ReplicaObservation status that reports the supplied
// replica. A condition keeps its existing lastTransitionTime unless its
// status changed.
func Status(existing v1alpha1.ReplicaObservationStatus, generation int64, observed replica.Observation) v1alpha1.ReplicaObservationStatus {
	// Condition is a struct of values, so a shallow clone is a deep copy.
	status := v1alpha1.ReplicaObservationStatus{
		ObservedGeneration: generation,
		Conditions:         slices.Clone(existing.Conditions),
	}
	for _, p := range observed.Pods {
		status.Pods.Total++
		if p.Scheduling == replica.Scheduled {
			status.Pods.Scheduled++
		}
		if p.Ready {
			status.Pods.Ready++
		}
		status.Pods.Restarts += p.Restarts
		if t := p.LastTermination; t != nil && (status.LastTermination == nil || t.FinishedAt.After(status.LastTermination.FinishedAt.Time)) {
			status.LastTermination = &v1alpha1.ContainerTermination{
				Pod:        p.Name,
				Container:  t.Container,
				Reason:     t.Reason,
				ExitCode:   t.ExitCode,
				FinishedAt: metav1.NewTime(t.FinishedAt),
			}
		}
	}
	for _, d := range observed.Devices {
		status.Devices = append(status.Devices, v1alpha1.AllocatedDevice{
			Driver:   d.ID.Driver,
			Pool:     d.ID.Pool,
			Device:   d.ID.Device,
			NodeName: d.NodeName,
			UUID:     d.UUID,
		})
	}
	for _, p := range observed.Pools {
		status.Pools = append(status.Pools, v1alpha1.AllocatedPool{Name: p.Name, LastAllocationTime: metav1.NewMicroTime(p.LastAllocation)})
	}

	scheduled := Scheduled(observed)
	scheduled.ObservedGeneration = generation
	meta.SetStatusCondition(&status.Conditions, scheduled)

	ready := Ready(observed)
	ready.ObservedGeneration = generation
	meta.SetStatusCondition(&status.Conditions, ready)

	return status
}

// FailedStatus returns the ReplicaObservation status that reports the replica
// couldn't be observed, for the supplied reason. Its conditions become
// Unknown, and it reports no pods, devices or pools.
func FailedStatus(existing v1alpha1.ReplicaObservationStatus, generation int64, reason string, err error) v1alpha1.ReplicaObservationStatus {
	// Condition is a struct of values, so a shallow clone is a deep copy.
	status := v1alpha1.ReplicaObservationStatus{
		ObservedGeneration: generation,
		Conditions:         slices.Clone(existing.Conditions),
	}
	for _, t := range []string{v1alpha1.TypeScheduled, v1alpha1.TypeReady} {
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:               t,
			Status:             metav1.ConditionUnknown,
			Reason:             reason,
			Message:            err.Error(),
			ObservedGeneration: generation,
		})
	}
	return status
}

// Scheduled returns a replica's Scheduled condition. It's True when every pod
// is bound to a node and the agent has seen each pod's claims allocated.
// Otherwise its reason names the first pod that isn't, preferring one the
// scheduler couldn't place.
func Scheduled(observed replica.Observation) metav1.Condition {
	cond := metav1.Condition{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionFalse}

	if len(observed.Pods) == 0 {
		cond.Reason = v1alpha1.ReasonNoPods
		return cond
	}

	for _, want := range []struct {
		scheduling replica.Scheduling
		reason     string
	}{
		{scheduling: replica.Unschedulable, reason: v1alpha1.ReasonUnschedulable},
		{scheduling: replica.Gated, reason: v1alpha1.ReasonSchedulingGated},
		{scheduling: replica.Pending, reason: v1alpha1.ReasonSchedulingPending},
	} {
		i := slices.IndexFunc(observed.Pods, func(p replica.Pod) bool { return p.Scheduling == want.scheduling })
		if i < 0 {
			continue
		}
		cond.Reason = want.reason
		cond.Message = "pod " + observed.Pods[i].Name
		if m := observed.Pods[i].SchedulingMessage; m != "" {
			cond.Message += ": " + m
		}
		return cond
	}

	// A pod is only bound once the scheduler has allocated its claims, but the
	// agent's cache of claims can lag its cache of pods.
	if i := slices.IndexFunc(observed.Pods, func(p replica.Pod) bool { return len(p.UnallocatedClaims) > 0 }); i >= 0 {
		cond.Reason = v1alpha1.ReasonAllocationNotSeen
		cond.Message = fmt.Sprintf("pod %s: ResourceClaim %s not yet seen allocated", observed.Pods[i].Name, observed.Pods[i].UnallocatedClaims[0])
		return cond
	}

	cond.Status = metav1.ConditionTrue
	cond.Reason = v1alpha1.ReasonPodsScheduled
	return cond
}

// Ready returns a replica's Ready condition. It's True when every pod is
// ready. Otherwise its reason says what the replica is waiting on: pods to be
// scheduled, an image it can't pull, a container that keeps crashing or can't
// be created, or containers that are still starting.
func Ready(observed replica.Observation) metav1.Condition {
	cond := metav1.Condition{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse}

	if len(observed.Pods) == 0 {
		cond.Reason = v1alpha1.ReasonNoPods
		return cond
	}

	ready, scheduled := 0, 0
	for _, p := range observed.Pods {
		if p.Ready {
			ready++
		}
		if p.Scheduling == replica.Scheduled {
			scheduled++
		}
	}

	if ready == len(observed.Pods) {
		cond.Status = metav1.ConditionTrue
		cond.Reason = v1alpha1.ReasonPodsReady
		return cond
	}

	if scheduled < len(observed.Pods) {
		cond.Reason = v1alpha1.ReasonNotScheduled
		cond.Message = fmt.Sprintf("%d of %d pods scheduled", scheduled, len(observed.Pods))
		return cond
	}

	for _, want := range []struct {
		reasons []string
		reason  string
	}{
		{reasons: []string{waitingErrImagePull, waitingImagePullBackOff, waitingInvalidImageName}, reason: v1alpha1.ReasonImagePullBackOff},
		{reasons: []string{waitingCrashLoopBackOff}, reason: v1alpha1.ReasonCrashLoopBackOff},
		{reasons: []string{waitingCreateContainerConfigError, waitingCreateContainerError}, reason: v1alpha1.ReasonCreateContainerError},
	} {
		for _, p := range observed.Pods {
			i := slices.IndexFunc(p.Waiting, func(w replica.Waiting) bool { return slices.Contains(want.reasons, w.Reason) })
			if i < 0 {
				continue
			}
			cond.Reason = want.reason
			cond.Message = fmt.Sprintf("pod %s container %s: %s: %s", p.Name, p.Waiting[i].Container, p.Waiting[i].Reason, p.Waiting[i].Message)
			return cond
		}
	}

	cond.Reason = v1alpha1.ReasonStarting
	cond.Message = fmt.Sprintf("%d of %d pods ready", ready, len(observed.Pods))
	return cond
}
