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
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/modelplaneai/cluster-agent/apis/v1alpha1"
	"github.com/modelplaneai/cluster-agent/internal/replica"
)

var (
	errBoom    = errors.New("boom")
	errInvalid = kerrors.NewInvalid(schema.GroupKind{Group: v1alpha1.Group, Kind: "ReplicaObservation"}, "llama", field.ErrorList{field.TooMany(field.NewPath("status", "devices"), 4097, 4096)})
)

// The fake client sets a resourceVersion on every object it stores.
var ignoreResourceVersion = cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ResourceVersion")

func TestReconcile(t *testing.T) {
	now := metav1.Now()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ml", Name: "llama"}}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	type params struct {
		objects     []client.Object
		interceptor interceptor.Funcs
		observation replica.Observation
	}
	type want struct {
		r      reconcile.Result
		err    error
		inputs replica.Inputs
		status *v1alpha1.ReplicaObservationStatus
	}

	cases := map[string]struct {
		reason string
		params params
		want   want
	}{
		"NotFound": {
			reason: "There's nothing to do until the ReplicaObservation exists.",
			params: params{},
			want:   want{},
		},
		"InvalidSelector": {
			reason: "An invalid pod selector should be reported in status rather than retried.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ReplicaObservation{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama", Generation: 2},
						Spec: v1alpha1.ReplicaObservationSpec{PodSelector: metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "modelplane.ai/replica", Operator: "Sometimes"}},
						}},
					},
				},
			},
			want: want{
				status: &v1alpha1.ReplicaObservationStatus{
					ObservedGeneration: 2,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonInvalidPodSelector, Message: `"Sometimes" is not a valid label selector operator`, ObservedGeneration: 2, LastTransitionTime: now},
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonInvalidPodSelector, Message: `"Sometimes" is not a valid label selector operator`, ObservedGeneration: 2, LastTransitionTime: now},
					},
				},
			},
		},
		"ListError": {
			reason: "An error listing the replica's pods should be returned, without changing the status.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ReplicaObservation{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama"}},
				},
				interceptor: interceptor.Funcs{
					List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
						return errBoom
					},
				},
			},
			want: want{
				err:    cmpopts.AnyError,
				status: &v1alpha1.ReplicaObservationStatus{},
			},
		},
		"ReadsReplica": {
			reason: "Only the replica's pods and the ResourceClaims they name should be observed. A pod the selector matches that isn't part of a replica shouldn't be, and a claim that doesn't exist yet should be skipped.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ReplicaObservation{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama", Generation: 1},
						Spec:       v1alpha1.ReplicaObservationSpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "llama"}}},
					},
					&corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0", Labels: map[string]string{"app": "llama", replica.LabelReplica: "llama"}},
						Spec:       corev1.PodSpec{ResourceClaims: []corev1.PodResourceClaim{{Name: "shared", ResourceClaimName: new("not-yet-created")}}},
						Status:     corev1.PodStatus{ResourceClaimStatuses: []corev1.PodResourceClaimStatus{{Name: "gpu", ResourceClaimName: new("llama-0-gpu")}}},
					},
					&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "qwen-0", Labels: map[string]string{"app": "qwen", replica.LabelReplica: "qwen"}}},
					&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-debug", Labels: map[string]string{"app": "llama"}}},
					&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "llama-0", Labels: map[string]string{"app": "llama", replica.LabelReplica: "llama"}}},
					&resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0-gpu"}},
					&resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "qwen-0-gpu"}},
					&resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "llama-0-gpu"}},
					&resourcev1.ResourceSlice{ObjectMeta: metav1.ObjectMeta{Name: "node-a-gpu"}, Spec: resourcev1.ResourceSliceSpec{Driver: "gpu.nvidia.com", Pool: resourcev1.ResourcePool{Name: "node-a"}}},
				},
			},
			want: want{
				inputs: replica.Inputs{
					Pods: []corev1.Pod{{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0", Labels: map[string]string{"app": "llama", replica.LabelReplica: "llama"}},
						Spec:       corev1.PodSpec{ResourceClaims: []corev1.PodResourceClaim{{Name: "shared", ResourceClaimName: new("not-yet-created")}}},
						Status:     corev1.PodStatus{ResourceClaimStatuses: []corev1.PodResourceClaimStatus{{Name: "gpu", ResourceClaimName: new("llama-0-gpu")}}},
					}},
					Claims: []resourcev1.ResourceClaim{
						{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0-gpu"}},
					},
					Slices: []resourcev1.ResourceSlice{
						{ObjectMeta: metav1.ObjectMeta{Name: "node-a-gpu"}, Spec: resourcev1.ResourceSliceSpec{Driver: "gpu.nvidia.com", Pool: resourcev1.ResourcePool{Name: "node-a"}}},
					},
				},
				status: &v1alpha1.ReplicaObservationStatus{
					ObservedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoPods, ObservedGeneration: 1, LastTransitionTime: now},
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoPods, ObservedGeneration: 1, LastTransitionTime: now},
					},
				},
			},
		},
		"AppliesStatus": {
			reason: "The observed replica should be applied to the ReplicaObservation's status.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ReplicaObservation{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama", Generation: 1}},
				},
				observation: replica.Observation{Pods: []replica.Pod{{Name: "llama-0", Scheduling: replica.Scheduled, Ready: true}}},
			},
			want: want{
				status: &v1alpha1.ReplicaObservationStatus{
					ObservedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPodsScheduled, ObservedGeneration: 1, LastTransitionTime: now},
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPodsReady, ObservedGeneration: 1, LastTransitionTime: now},
					},
					Pods: v1alpha1.PodCounts{Total: 1, Scheduled: 1, Ready: 1},
				},
			},
		},
		"StatusRejected": {
			reason: "A status the API server rejects as invalid should be reported as unknown conditions rather than retried.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ReplicaObservation{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama", Generation: 1}},
				},
				interceptor: interceptor.Funcs{
					// Reject any status that doesn't report the failure.
					SubResourceApply: func(ctx context.Context, c client.Client, sub string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
						j, err := json.Marshal(obj)
						if err != nil {
							return err
						}
						if !strings.Contains(string(j), v1alpha1.ReasonObservationFailed) {
							return errInvalid
						}
						return c.SubResource(sub).Apply(ctx, obj, opts...)
					},
				},
				observation: replica.Observation{Pods: []replica.Pod{{Name: "llama-0", Scheduling: replica.Scheduled, Ready: true}}},
			},
			want: want{
				status: &v1alpha1.ReplicaObservationStatus{
					ObservedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeScheduled, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonObservationFailed, Message: "cannot apply status: " + errInvalid.Error(), ObservedGeneration: 1, LastTransitionTime: now},
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonObservationFailed, Message: "cannot apply status: " + errInvalid.Error(), ObservedGeneration: 1, LastTransitionTime: now},
					},
				},
			},
		},
		"ApplyError": {
			reason: "An error applying the status should be returned.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ReplicaObservation{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama"}},
				},
				interceptor: interceptor.Funcs{
					SubResourceApply: func(_ context.Context, _ client.Client, _ string, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
						return errBoom
					},
				},
			},
			want: want{
				err:    cmpopts.AnyError,
				status: &v1alpha1.ReplicaObservationStatus{},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.params.objects...).
				WithStatusSubresource(&v1alpha1.ReplicaObservation{}).
				WithInterceptorFuncs(tc.params.interceptor).
				Build()

			var inputs replica.Inputs
			r := NewReconciler(c, WithReplicaObserver(ReplicaObserverFn(func(in replica.Inputs) replica.Observation {
				inputs = in
				return tc.params.observation
			})))
			got, err := r.Reconcile(t.Context(), req)
			if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nr.Reconcile(...): -want error, +got error:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.r, got); diff != "" {
				t.Errorf("\n%s\nr.Reconcile(...): -want, +got:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.inputs, inputs, cmpopts.EquateEmpty(), ignoreResourceVersion); diff != "" {
				t.Errorf("\n%s\nr.Reconcile(...): -want observed inputs, +got observed inputs:\n%s", tc.reason, diff)
			}
			var status *v1alpha1.ReplicaObservationStatus
			ro := &v1alpha1.ReplicaObservation{}
			if err := c.Get(t.Context(), req.NamespacedName, ro); err == nil {
				status = &ro.Status
			}
			if diff := cmp.Diff(tc.want.status, status, equateNow, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("\n%s\nr.Reconcile(...): -want status, +got status:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestRequestsFor(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	type args struct {
		objects   []client.Object
		namespace string
		labels    map[string]string
	}

	cases := map[string]struct {
		reason string
		args   args
		want   []reconcile.Request
	}{
		"Matches": {
			reason: "A pod should map to the ReplicaObservations in its namespace whose pod selector matches it.",
			args: args{
				objects: []client.Object{
					&v1alpha1.ReplicaObservation{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama"},
						Spec:       v1alpha1.ReplicaObservationSpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"modelplane.ai/replica": "llama"}}},
					},
					&v1alpha1.ReplicaObservation{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "qwen"},
						Spec:       v1alpha1.ReplicaObservationSpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"modelplane.ai/replica": "qwen"}}},
					},
					&v1alpha1.ReplicaObservation{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "invalid"},
						Spec: v1alpha1.ReplicaObservationSpec{PodSelector: metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "modelplane.ai/replica", Operator: "Sometimes"}},
						}},
					},
					&v1alpha1.ReplicaObservation{
						ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "llama"},
						Spec:       v1alpha1.ReplicaObservationSpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"modelplane.ai/replica": "llama"}}},
					},
				},
				namespace: "ml",
				labels:    map[string]string{"modelplane.ai/replica": "llama"},
			},
			want: []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "ml", Name: "llama"}}},
		},
		"NoMatch": {
			reason: "A pod no pod selector matches should map to nothing.",
			args: args{
				objects: []client.Object{
					&v1alpha1.ReplicaObservation{
						ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "qwen"},
						Spec:       v1alpha1.ReplicaObservationSpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"modelplane.ai/replica": "qwen"}}},
					},
				},
				namespace: "ml",
				labels:    map[string]string{"modelplane.ai/replica": "llama"},
			},
			want: nil,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.args.objects...).Build()
			got := RequestsFor(t.Context(), c, tc.args.namespace, tc.args.labels)
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("\n%s\nRequestsFor(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestClaimRequests(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	objects := []client.Object{
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0", Labels: map[string]string{"modelplane.ai/replica": "llama"}}},
		&v1alpha1.ReplicaObservation{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama"},
			Spec:       v1alpha1.ReplicaObservationSpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"modelplane.ai/replica": "llama"}}},
		},
	}

	cases := map[string]struct {
		reason string
		claim  client.Object
		want   []reconcile.Request
	}{
		"Reserved": {
			reason: "A claim should map to the ReplicaObservations of the pods it's reserved for.",
			claim: &resourcev1.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0-gpu"},
				Status: resourcev1.ResourceClaimStatus{ReservedFor: []resourcev1.ResourceClaimConsumerReference{
					{Resource: "pods", Name: "llama-0", UID: "a"},
					{Resource: "pods", Name: "gone", UID: "b"},
					{APIGroup: "example.org", Resource: "things", Name: "llama-0", UID: "c"},
				}},
			},
			want: []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "ml", Name: "llama"}}},
		},
		"Unreserved": {
			reason: "A claim that isn't reserved for a pod should map to nothing.",
			claim:  &resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0-gpu"}},
			want:   nil,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			got := ClaimRequests(c)(t.Context(), tc.claim)
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("\n%s\nClaimRequests(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}
