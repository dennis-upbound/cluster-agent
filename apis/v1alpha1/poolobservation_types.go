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
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterObservationName is the name of the one ClusterObservation the agent
// reports on.
const ClusterObservationName = "cluster"

// A ClusterObservation reports the state of the cluster's GPU pools. The agent
// writes its status. A cluster has at most one ClusterObservation, named
// cluster.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,categories=modelplane
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:validation:XValidation:rule="self.metadata.name == 'cluster'",message="a ClusterObservation must be named cluster"
type ClusterObservation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec ClusterObservationSpec `json:"spec,omitempty"`

	// +optional
	Status ClusterObservationStatus `json:"status,omitempty"`
}

// ClusterObservationSpec is empty. The agent decides what to observe.
type ClusterObservationSpec struct{}

// ClusterObservationStatus is what the agent observed about the cluster.
type ClusterObservationStatus struct {
	// Conditions of the observation. Ready is True when the agent's most
	// recent observation succeeded. When it's False, the pools are stale.
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=8
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Pools are the cluster's GPU pools. A pool is every node that carries a
	// modelplane.ai/pool label with the pool's name. A pool with no nodes
	// appears when unschedulable pods are pinned to it.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=64
	Pools []PoolStatus `json:"pools,omitempty"`
}

// PoolStatus is what the agent observed about one GPU pool.
type PoolStatus struct {
	// Name of the pool: the value of its nodes' modelplane.ai/pool label.
	//
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// Nodes counts the pool's nodes.
	Nodes NodeCounts `json:"nodes"`

	// UnschedulablePods counts the pods pinned to the pool by a
	// modelplane.ai/pool node selector that the scheduler couldn't find a
	// node for. Only pods that carry a modelplane.ai/replica label count.
	//
	// +kubebuilder:validation:Minimum=0
	UnschedulablePods int32 `json:"unschedulablePods"`

	// Devices are the DRA devices the pool's nodes publish, by the
	// DeviceClass they belong to. A device that belongs to more than one
	// DeviceClass counts toward each.
	//
	// +optional
	// +listType=map
	// +listMapKey=deviceClassName
	// +kubebuilder:validation:MaxItems=32
	Devices []DeviceClassStatus `json:"devices,omitempty"`

	// ExtendedResources sums the extended resources, such as nvidia.com/gpu,
	// that the pool's nodes advertise as allocatable. A device plugin
	// advertises these. One advertising the same hardware a DRA driver
	// publishes gives the two allocators separate books over one set of
	// devices.
	//
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	ExtendedResources map[corev1.ResourceName]resource.Quantity `json:"extendedResources,omitempty"`

	// Conditions of the pool. Healthy is False when any of the pool's nodes
	// is not ready or cordoned, or any of its devices is tainted.
	// ScaleUpBlocked is True when the cluster autoscaler reports it can't add
	// nodes for an unschedulable pod. Each condition's lastTransitionTime is
	// when its status last changed.
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=8
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// NodeCounts counts a pool's nodes.
type NodeCounts struct {
	// Total counts every node in the pool.
	//
	// +kubebuilder:validation:Minimum=0
	Total int32 `json:"total"`

	// Ready counts the nodes whose Ready condition is True.
	//
	// +kubebuilder:validation:Minimum=0
	Ready int32 `json:"ready"`

	// Schedulable counts the ready nodes that aren't cordoned.
	//
	// +kubebuilder:validation:Minimum=0
	Schedulable int32 `json:"schedulable"`
}

// DeviceClassStatus describes a pool's devices of one DeviceClass.
type DeviceClassStatus struct {
	// DeviceClassName is the DeviceClass the devices belong to.
	//
	// +kubebuilder:validation:MaxLength=253
	DeviceClassName string `json:"deviceClassName"`

	// Nodes counts the pool's nodes that publish at least one device of the
	// class, whatever their state.
	//
	// +kubebuilder:validation:Minimum=0
	Nodes int32 `json:"nodes"`

	// Allocatable counts the class's untainted devices on schedulable nodes.
	//
	// +kubebuilder:validation:Minimum=0
	Allocatable int32 `json:"allocatable"`

	// Allocated counts the allocatable devices a ResourceClaim has allocated.
	//
	// +kubebuilder:validation:Minimum=0
	Allocated int32 `json:"allocated"`

	// NodesByFreeDevices is a histogram of the schedulable nodes that
	// publish devices of the class, by how many of those devices are
	// allocatable and not allocated.
	//
	// +optional
	// +listType=map
	// +listMapKey=freeDevices
	// +kubebuilder:validation:MaxItems=129
	NodesByFreeDevices []FreeDevicesBucket `json:"nodesByFreeDevices,omitempty"`

	// Attributes are the device attributes every device of the class in the
	// pool agrees on, keyed by the attribute names the DRA driver publishes.
	//
	// +optional
	// +kubebuilder:validation:MaxProperties=32
	Attributes map[resourcev1.QualifiedName]resourcev1.DeviceAttribute `json:"attributes,omitempty"`
}

// A FreeDevicesBucket counts the nodes with a number of free devices.
type FreeDevicesBucket struct {
	// FreeDevices is the number of free devices.
	//
	// +kubebuilder:validation:Minimum=0
	FreeDevices int32 `json:"freeDevices"`

	// Nodes counts the nodes with that many free devices.
	//
	// +kubebuilder:validation:Minimum=1
	Nodes int32 `json:"nodes"`
}

// ClusterObservationList is a list of ClusterObservations.
//
// +kubebuilder:object:root=true
type ClusterObservationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ClusterObservation `json:"items"`
}
