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

// Package v1alpha1 contains the cluster agent's observation API. The agent
// reports what it observes about the cluster it runs on in the status of these
// resources.
//
// +kubebuilder:object:generate=true
// +groupName=agent.modelplane.ai
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Group and version of the observation API.
const (
	Group   = "agent.modelplane.ai"
	Version = "v1alpha1"
)

// SchemeGroupVersion is the group and version of the observation API.
var SchemeGroupVersion = schema.GroupVersion{Group: Group, Version: Version}

// Kinds of the observation API.
var (
	PoolObservationGroupVersionKind    = SchemeGroupVersion.WithKind("PoolObservation")
	ReplicaObservationGroupVersionKind = SchemeGroupVersion.WithKind("ReplicaObservation")
)

var schemeBuilder = runtime.NewSchemeBuilder(func(s *runtime.Scheme) error {
	s.AddKnownTypes(SchemeGroupVersion,
		&PoolObservation{},
		&PoolObservationList{},
		&ReplicaObservation{},
		&ReplicaObservationList{},
	)
	metav1.AddToGroupVersion(s, SchemeGroupVersion)
	return nil
})

// AddToScheme adds the observation API's types to a scheme.
var AddToScheme = schemeBuilder.AddToScheme
