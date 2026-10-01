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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A ReplicaObservation reports the state of one replica's pods and the devices
// they were allocated. The agent writes its status.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,categories=modelplane
// +kubebuilder:printcolumn:name="SCHEDULED",type="string",JSONPath=".status.conditions[?(@.type=='Scheduled')].status"
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
type ReplicaObservation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ReplicaObservationSpec `json:"spec"`

	// +optional
	Status ReplicaObservationStatus `json:"status,omitempty"`
}

// ReplicaObservationSpec specifies which pods make up the replica.
type ReplicaObservationSpec struct {
	// PodSelector selects the replica's pods in the ReplicaObservation's
	// namespace. The agent only sees pods that carry a modelplane.ai/replica
	// label, so a selector that matches other pods matches nothing.
	PodSelector metav1.LabelSelector `json:"podSelector"`
}

// ReplicaObservationStatus is what the agent observed about the replica.
type ReplicaObservationStatus struct {
	// ObservedGeneration is the generation of the spec the status reports on.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions of the replica. Scheduled is True when every pod is bound to
	// a node, which means its ResourceClaims are allocated. Ready is True
	// when every pod is ready, and otherwise its reason says what the replica
	// is waiting on. Each condition's lastTransitionTime is when its status
	// last changed.
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=8
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Pods counts the replica's pods.
	Pods PodCounts `json:"pods"`

	// Devices are the DRA devices allocated to the replica's pods.
	//
	// +optional
	// +listType=map
	// +listMapKey=driver
	// +listMapKey=pool
	// +listMapKey=device
	// +kubebuilder:validation:MaxItems=4096
	Devices []AllocatedDevice `json:"devices,omitempty"`
}

// PodCounts counts a replica's pods.
type PodCounts struct {
	// Total counts every pod the replica's selector matches.
	//
	// +kubebuilder:validation:Minimum=0
	Total int32 `json:"total"`

	// Scheduled counts the pods bound to a node.
	//
	// +kubebuilder:validation:Minimum=0
	Scheduled int32 `json:"scheduled"`

	// Ready counts the pods whose Ready condition is True.
	//
	// +kubebuilder:validation:Minimum=0
	Ready int32 `json:"ready"`
}

// An AllocatedDevice is a DRA device allocated to one of a replica's pods.
type AllocatedDevice struct {
	// Driver that publishes the device.
	//
	// +kubebuilder:validation:MaxLength=63
	Driver string `json:"driver"`

	// Pool is the DRA resource pool the device belongs to. This is the
	// driver's pool, not a Modelplane GPU pool.
	//
	// +kubebuilder:validation:MaxLength=253
	Pool string `json:"pool"`

	// Device is the device's name within its DRA pool.
	//
	// +kubebuilder:validation:MaxLength=63
	Device string `json:"device"`

	// NodeName is the node the device is attached to.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=253
	NodeName string `json:"nodeName,omitempty"`

	// UUID is the device's uuid attribute, when the driver publishes one.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=64
	UUID string `json:"uuid,omitempty"`
}

// ReplicaObservationList is a list of ReplicaObservations.
//
// +kubebuilder:object:root=true
type ReplicaObservationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ReplicaObservation `json:"items"`
}
