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

package v1alpha1

// Condition types.
const (
	// TypeReady reports whether a replica's pods are ready.
	TypeReady = "Ready"

	// TypeHealthy reports whether a pool's nodes and devices are healthy.
	TypeHealthy = "Healthy"

	// TypeScaleUpBlocked reports whether the cluster autoscaler can add
	// nodes to a pool for its unschedulable pods.
	TypeScaleUpBlocked = "ScaleUpBlocked"

	// TypeScheduled reports whether a replica's pods are bound to nodes.
	TypeScheduled = "Scheduled"
)

// ReasonObservationFailed is the reason for every condition of an observation
// the agent couldn't make.
const ReasonObservationFailed = "ObservationFailed"

// Reasons for a pool's Healthy condition.
const (
	ReasonPoolHealthy    = "PoolHealthy"
	ReasonNodesNotReady  = "NodesNotReady"
	ReasonNodesCordoned  = "NodesCordoned"
	ReasonDevicesTainted = "DevicesTainted"
)

// Reasons for a pool's ScaleUpBlocked condition.
const (
	ReasonNoUnschedulablePods = "NoUnschedulablePods"
	ReasonScaleUpTriggered    = "ScaleUpTriggered"
	ReasonScaleUpNotTriggered = "ScaleUpNotTriggered"
	ReasonNoAutoscalerEvents  = "NoAutoscalerEvents"
)

// Reasons for a ReplicaObservation's Scheduled and Ready conditions.
const (
	ReasonInvalidPodSelector   = "InvalidPodSelector"
	ReasonNoPods               = "NoPods"
	ReasonPodsScheduled        = "PodsScheduled"
	ReasonUnschedulable        = "Unschedulable"
	ReasonSchedulingGated      = "SchedulingGated"
	ReasonSchedulingPending    = "SchedulingPending"
	ReasonPodsReady            = "PodsReady"
	ReasonNotScheduled         = "NotScheduled"
	ReasonImagePullBackOff     = "ImagePullBackOff"
	ReasonCrashLoopBackOff     = "CrashLoopBackOff"
	ReasonCreateContainerError = "CreateContainerError"
	ReasonStarting             = "Starting"
)
