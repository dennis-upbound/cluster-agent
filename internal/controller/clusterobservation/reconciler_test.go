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
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/modelplaneai/cluster-agent/apis/v1alpha1"
	"github.com/modelplaneai/cluster-agent/internal/pool"
)

var errBoom = errors.New("boom")

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
		observer    PoolObserver
	}
	type want struct {
		r      reconcile.Result
		err    error
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
			reason: "A failed observation should be returned, and reported as Ready False while keeping the stale pools.",
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
				observer: PoolObserverFn(func(_ context.Context, _ pool.Inputs) ([]pool.Observation, error) {
					return nil, errBoom
				}),
			},
			want: want{
				err: cmpopts.AnyError,
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
		"AppliesStatus": {
			reason: "The observed pools should be applied to the ClusterObservation's status.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ClusterObservation{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ClusterObservationName}},
				},
				observer: PoolObserverFn(func(_ context.Context, _ pool.Inputs) ([]pool.Observation, error) {
					return []pool.Observation{{Name: "gpu", Nodes: pool.Nodes{Total: 1, Ready: 1, Schedulable: 1}}}, nil
				}),
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
								{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonNodesHealthy, LastTransitionTime: now},
								{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoPendingPods, LastTransitionTime: now},
							},
						},
					},
				},
			},
		},
		"StatusUnchanged": {
			reason: "A status that already reports what was observed shouldn't be applied again.",
			params: params{
				objects: []client.Object{
					&v1alpha1.ClusterObservation{
						ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ClusterObservationName},
						Status: v1alpha1.ClusterObservationStatus{
							Conditions: []metav1.Condition{
								{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: then},
							},
							Pools: []v1alpha1.PoolStatus{
								{
									Name:  "gpu",
									Nodes: v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1},
									Conditions: []metav1.Condition{
										{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonNodesHealthy, LastTransitionTime: then},
										{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoPendingPods, LastTransitionTime: then},
									},
								},
							},
						},
					},
				},
				interceptor: interceptor.Funcs{
					SubResourceApply: func(_ context.Context, _ client.Client, _ string, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
						return errBoom
					},
				},
				observer: PoolObserverFn(func(_ context.Context, _ pool.Inputs) ([]pool.Observation, error) {
					return []pool.Observation{{Name: "gpu", Nodes: pool.Nodes{Total: 1, Ready: 1, Schedulable: 1}}}, nil
				}),
			},
			want: want{
				status: &v1alpha1.ClusterObservationStatus{
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonObserved, LastTransitionTime: then},
					},
					Pools: []v1alpha1.PoolStatus{
						{
							Name:  "gpu",
							Nodes: v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1},
							Conditions: []metav1.Condition{
								{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonNodesHealthy, LastTransitionTime: then},
								{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoPendingPods, LastTransitionTime: then},
							},
						},
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
				observer: PoolObserverFn(func(_ context.Context, _ pool.Inputs) ([]pool.Observation, error) {
					return nil, nil
				}),
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

			r := NewReconciler(c, WithPoolObserver(tc.params.observer))
			got, err := r.Reconcile(t.Context(), req)

			if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nr.Reconcile(...): -want error, +got error:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.r, got); diff != "" {
				t.Errorf("\n%s\nr.Reconcile(...): -want, +got:\n%s", tc.reason, diff)
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
