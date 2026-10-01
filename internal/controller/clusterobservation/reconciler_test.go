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

package clusterobservation

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
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
	"github.com/modelplaneai/cluster-agent/internal/pool"
	"github.com/modelplaneai/cluster-agent/internal/replica"
)

var (
	errBoom    = errors.New("boom")
	errInvalid = kerrors.NewInvalid(schema.GroupKind{Group: v1alpha1.Group, Kind: "ClusterObservation"}, v1alpha1.ClusterObservationName, field.ErrorList{field.TooMany(field.NewPath("status", "pools"), 65, 64)})
)

// The fake client sets a resourceVersion on every object it stores.
var ignoreResourceVersion = cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ResourceVersion")

func TestReconcile(t *testing.T) {
	then := metav1.NewTime(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	now := metav1.Now()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: v1alpha1.ClusterObservationName}}

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
		pools       []pool.Observation
		observeErr  error
	}
	type want struct {
		r      reconcile.Result
		err    error
		inputs pool.Inputs
		status *v1alpha1.ClusterObservationStatus
	}

	cases := map[string]struct {
		reason string
		params params
		want   want
	}{
		"NotFound": {
			reason: "There's nothing to do until the ClusterObservation exists.",
			params: params{},
			want:   want{},
		},
		"GetError": {
			reason: "An error getting the ClusterObservation should be returned.",
			params: params{
				interceptor: interceptor.Funcs{
					Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
						return errBoom
					},
				},
			},
			want: want{err: cmpopts.AnyError},
		},
		"ListError": {
			reason: "An error listing what to observe should be returned, without changing the status.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ClusterObservation{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ClusterObservationName}},
				},
				interceptor: interceptor.Funcs{
					List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
						return errBoom
					},
				},
			},
			want: want{
				err:    cmpopts.AnyError,
				status: &v1alpha1.ClusterObservationStatus{},
			},
		},
		"ObserveError": {
			reason: "A failed observation should be reported as Ready False, keeping the stale pools, rather than retried.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ClusterObservation{
						ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ClusterObservationName},
						Status: v1alpha1.ClusterObservationStatus{
							Conditions: []metav1.Condition{
								{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: then},
							},
							Pools: []v1alpha1.PoolStatus{
								{Name: "gpu", Nodes: v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1}},
							},
						},
					},
				},
				observeErr: errBoom,
			},
			want: want{
				status: &v1alpha1.ClusterObservationStatus{
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonObservationFailed, Message: "cannot observe pools: boom", LastTransitionTime: now},
					},
					Pools: []v1alpha1.PoolStatus{
						{Name: "gpu", Nodes: v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1}},
					},
				},
			},
		},
		"ReadsPoolsAndReplicas": {
			reason: "Only nodes in a pool and pods of a replica should be observed.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ClusterObservation{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ClusterObservationName}},
					&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-0", Labels: map[string]string{pool.LabelPool: "gpu"}}},
					&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "control-plane-0"}},
					&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0", Labels: map[string]string{replica.LabelReplica: "llama"}}},
					&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "coredns-0"}},
				},
			},
			want: want{
				inputs: pool.Inputs{
					Nodes: []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "gpu-0", Labels: map[string]string{pool.LabelPool: "gpu"}}}},
					Pods:  []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0", Labels: map[string]string{replica.LabelReplica: "llama"}}}},
				},
				status: &v1alpha1.ClusterObservationStatus{
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: now},
					},
				},
			},
		},
		"AppliesStatus": {
			reason: "The observed pools should be applied to the ClusterObservation's status.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ClusterObservation{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ClusterObservationName}},
				},
				pools: []pool.Observation{{Name: "gpu", Nodes: pool.Nodes{Total: 1, Ready: 1, Schedulable: 1}}},
			},
			want: want{
				status: &v1alpha1.ClusterObservationStatus{
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: now},
					},
					Pools: []v1alpha1.PoolStatus{
						{
							Name:  "gpu",
							Nodes: v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1},
							Conditions: []metav1.Condition{
								{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy, LastTransitionTime: now},
								{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, LastTransitionTime: now},
							},
						},
					},
				},
			},
		},
		"StatusRejected": {
			reason: "A status the API server rejects as invalid should be reported as Ready False, keeping the stale pools, rather than retried.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ClusterObservation{
						ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ClusterObservationName},
						Status: v1alpha1.ClusterObservationStatus{
							Conditions: []metav1.Condition{
								{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: then},
							},
							Pools: []v1alpha1.PoolStatus{
								{Name: "gpu", Nodes: v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1}},
							},
						},
					},
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
				pools: []pool.Observation{{Name: "gpu", Nodes: pool.Nodes{Total: 2, Ready: 2, Schedulable: 2}}},
			},
			want: want{
				status: &v1alpha1.ClusterObservationStatus{
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonObservationFailed, Message: "cannot apply status: " + errInvalid.Error(), LastTransitionTime: now},
					},
					Pools: []v1alpha1.PoolStatus{
						{Name: "gpu", Nodes: v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1}},
					},
				},
			},
		},
		"ApplyError": {
			reason: "An error applying the status should be returned.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ClusterObservation{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ClusterObservationName}},
				},
				interceptor: interceptor.Funcs{
					SubResourceApply: func(_ context.Context, _ client.Client, _ string, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
						return errBoom
					},
				},
			},
			want: want{
				err:    cmpopts.AnyError,
				status: &v1alpha1.ClusterObservationStatus{},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.params.objects...).
				WithStatusSubresource(&v1alpha1.ClusterObservation{}).
				WithInterceptorFuncs(tc.params.interceptor).
				Build()

			var inputs pool.Inputs
			r := NewReconciler(c, WithPoolObserver(PoolObserverFn(func(_ context.Context, in pool.Inputs) ([]pool.Observation, error) {
				inputs = in
				return tc.params.pools, tc.params.observeErr
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
			var status *v1alpha1.ClusterObservationStatus
			co := &v1alpha1.ClusterObservation{}
			if err := c.Get(t.Context(), req.NamespacedName, co); err == nil {
				status = &co.Status
			}
			if diff := cmp.Diff(tc.want.status, status, equateNow, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("\n%s\nr.Reconcile(...): -want status, +got status:\n%s", tc.reason, diff)
			}
		})
	}
}
