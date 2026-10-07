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
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

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
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/modelplaneai/cluster-agent/apis/v1alpha1"
	"github.com/modelplaneai/cluster-agent/internal/pool"
	"github.com/modelplaneai/cluster-agent/internal/replica"
)

var (
	errBoom    = errors.New("boom")
	errInvalid = kerrors.NewInvalid(schema.GroupKind{Group: v1alpha1.Group, Kind: "PoolObservation"}, "gpu", field.ErrorList{field.TooMany(field.NewPath("status", "devices"), 33, 32)})
)

// The fake client sets a resourceVersion on every object it stores.
var ignoreResourceVersion = cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ResourceVersion")

func TestReconcile(t *testing.T) {
	then := metav1.NewTime(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	now := metav1.Now()

	// The PoolObservation's name differs from its pool's, so the reconciler
	// can't confuse the two.
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "llama-cluster-gpu"}}
	meta := metav1.ObjectMeta{Name: "llama-cluster-gpu", Generation: 1}
	spec := v1alpha1.PoolObservationSpec{PoolName: "gpu"}

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
		observation pool.Observation
		observeErr  error
	}
	// What the reconciler asked its observer to observe.
	type observed struct {
		Pool   string
		Inputs pool.Inputs
	}
	type want struct {
		r        reconcile.Result
		err      error
		observed observed
		status   *v1alpha1.PoolObservationStatus
	}

	cases := map[string]struct {
		reason string
		params params
		want   want
	}{
		"NotFound": {
			reason: "There's nothing to do until the PoolObservation exists.",
			params: params{},
			want:   want{},
		},
		"GetError": {
			reason: "An error getting the PoolObservation should be returned.",
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
				objects: []client.Object{&v1alpha1.PoolObservation{ObjectMeta: meta, Spec: spec}},
				interceptor: interceptor.Funcs{
					List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
						return errBoom
					},
				},
			},
			want: want{
				err:    cmpopts.AnyError,
				status: &v1alpha1.PoolObservationStatus{},
			},
		},
		"ObserveError": {
			reason: "A failed observation should be reported as unknown conditions, with no nodes or devices, rather than retried.",
			params: params{
				objects: []client.Object{
					&v1alpha1.PoolObservation{
						ObjectMeta: meta,
						Spec:       spec,
						Status: v1alpha1.PoolObservationStatus{
							ObservedGeneration: 1,
							Conditions: []metav1.Condition{
								{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy, ObservedGeneration: 1, LastTransitionTime: then},
								{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, ObservedGeneration: 1, LastTransitionTime: then},
							},
							Nodes: v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1},
						},
					},
				},
				observeErr: errBoom,
			},
			want: want{
				observed: observed{Pool: "gpu"},
				status: &v1alpha1.PoolObservationStatus{
					ObservedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonObservationFailed, Message: "cannot observe pool: boom", ObservedGeneration: 1, LastTransitionTime: now},
						{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonObservationFailed, Message: "cannot observe pool: boom", ObservedGeneration: 1, LastTransitionTime: now},
					},
				},
			},
		},
		"ReadsPool": {
			reason: "The pool the spec names should be observed, from its nodes, the pods of replicas, and every ResourceSlice, DeviceClass, ResourceClaim and event.",
			params: params{
				objects: []client.Object{
					&v1alpha1.PoolObservation{ObjectMeta: meta, Spec: spec},
					&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-0", Labels: map[string]string{pool.LabelPool: "gpu"}}},
					&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "system-0", Labels: map[string]string{pool.LabelPool: "system"}}},
					&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "control-plane-0"}},
					&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0", Labels: map[string]string{replica.LabelReplica: "llama"}}},
					&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "coredns-0"}},
					&resourcev1.ResourceSlice{ObjectMeta: metav1.ObjectMeta{Name: "gpu-0-gpu"}},
					&resourcev1.DeviceClass{ObjectMeta: metav1.ObjectMeta{Name: "gpu.nvidia.com"}},
					&resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0-gpu"}},
					&corev1.Event{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0.scale-up"}},
				},
			},
			want: want{
				observed: observed{
					Pool: "gpu",
					Inputs: pool.Inputs{
						Nodes:   []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "gpu-0", Labels: map[string]string{pool.LabelPool: "gpu"}}}},
						Slices:  []resourcev1.ResourceSlice{{ObjectMeta: metav1.ObjectMeta{Name: "gpu-0-gpu"}}},
						Classes: []resourcev1.DeviceClass{{ObjectMeta: metav1.ObjectMeta{Name: "gpu.nvidia.com"}}},
						Claims:  []resourcev1.ResourceClaim{{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0-gpu"}}},
						Pods:    []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0", Labels: map[string]string{replica.LabelReplica: "llama"}}}},
						Events:  []corev1.Event{{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "llama-0.scale-up"}}},
					},
				},
				status: &v1alpha1.PoolObservationStatus{
					ObservedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy, ObservedGeneration: 1, LastTransitionTime: now},
						{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, ObservedGeneration: 1, LastTransitionTime: now},
					},
				},
			},
		},
		"AppliesStatus": {
			reason: "The observed pool should be applied to the PoolObservation's status.",
			params: params{
				objects:     []client.Object{&v1alpha1.PoolObservation{ObjectMeta: meta, Spec: spec}},
				observation: pool.Observation{Nodes: pool.Nodes{Total: 1, Ready: 1, Schedulable: 1}},
			},
			want: want{
				observed: observed{Pool: "gpu"},
				status: &v1alpha1.PoolObservationStatus{
					ObservedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPoolHealthy, ObservedGeneration: 1, LastTransitionTime: now},
						{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNoUnschedulablePods, ObservedGeneration: 1, LastTransitionTime: now},
					},
					Nodes: v1alpha1.NodeCounts{Total: 1, Ready: 1, Schedulable: 1},
				},
			},
		},
		"StatusRejected": {
			reason: "A status the API server rejects as invalid should be reported as unknown conditions rather than retried.",
			params: params{
				objects: []client.Object{&v1alpha1.PoolObservation{ObjectMeta: meta, Spec: spec}},
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
				observation: pool.Observation{Nodes: pool.Nodes{Total: 2, Ready: 2, Schedulable: 2}},
			},
			want: want{
				observed: observed{Pool: "gpu"},
				status: &v1alpha1.PoolObservationStatus{
					ObservedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: v1alpha1.TypeHealthy, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonObservationFailed, Message: "cannot apply status: " + errInvalid.Error(), ObservedGeneration: 1, LastTransitionTime: now},
						{Type: v1alpha1.TypeScaleUpBlocked, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonObservationFailed, Message: "cannot apply status: " + errInvalid.Error(), ObservedGeneration: 1, LastTransitionTime: now},
					},
				},
			},
		},
		"ObserveErrorApplyError": {
			reason: "An error applying the status that reports a failed observation should be returned.",
			params: params{
				objects: []client.Object{&v1alpha1.PoolObservation{ObjectMeta: meta, Spec: spec}},
				interceptor: interceptor.Funcs{
					SubResourceApply: func(_ context.Context, _ client.Client, _ string, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
						return errBoom
					},
				},
				observeErr: errBoom,
			},
			want: want{
				err:      cmpopts.AnyError,
				observed: observed{Pool: "gpu"},
				status:   &v1alpha1.PoolObservationStatus{},
			},
		},
		"StatusRejectedApplyError": {
			reason: "An error applying the status that reports a rejected status should be returned.",
			params: params{
				objects: []client.Object{&v1alpha1.PoolObservation{ObjectMeta: meta, Spec: spec}},
				interceptor: interceptor.Funcs{
					SubResourceApply: func(_ context.Context, _ client.Client, _ string, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
						return errInvalid
					},
				},
			},
			want: want{
				err:      cmpopts.AnyError,
				observed: observed{Pool: "gpu"},
				status:   &v1alpha1.PoolObservationStatus{},
			},
		},
		"ApplyError": {
			reason: "An error applying the status should be returned.",
			params: params{
				objects: []client.Object{&v1alpha1.PoolObservation{ObjectMeta: meta, Spec: spec}},
				interceptor: interceptor.Funcs{
					SubResourceApply: func(_ context.Context, _ client.Client, _ string, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
						return errBoom
					},
				},
			},
			want: want{
				err:      cmpopts.AnyError,
				observed: observed{Pool: "gpu"},
				status:   &v1alpha1.PoolObservationStatus{},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.params.objects...).
				WithStatusSubresource(&v1alpha1.PoolObservation{}).
				WithInterceptorFuncs(tc.params.interceptor).
				Build()
			gotObserved := observed{}
			r := NewReconciler(c, WithPoolObserver(PoolObserverFn(func(_ context.Context, name string, in pool.Inputs) (pool.Observation, error) {
				gotObserved = observed{Pool: name, Inputs: in}
				return tc.params.observation, tc.params.observeErr
			})))
			got, err := r.Reconcile(t.Context(), req)
			if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nr.Reconcile(...): -want error, +got error:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.r, got); diff != "" {
				t.Errorf("\n%s\nr.Reconcile(...): -want, +got:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.observed, gotObserved, cmpopts.EquateEmpty(), ignoreResourceVersion); diff != "" {
				t.Errorf("\n%s\nr.Reconcile(...): -want observed, +got observed:\n%s", tc.reason, diff)
			}
			var status *v1alpha1.PoolObservationStatus
			po := &v1alpha1.PoolObservation{}
			if err := c.Get(t.Context(), req.NamespacedName, po); err == nil {
				status = &po.Status
			}
			if diff := cmp.Diff(tc.want.status, status, equateNow, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("\n%s\nr.Reconcile(...): -want status, +got status:\n%s", tc.reason, diff)
			}
		})
	}
}

// A delayingQueue records the requests added to it after a delay.
type delayingQueue struct {
	workqueue.TypedRateLimitingInterface[reconcile.Request]

	Added map[reconcile.Request]time.Duration
}

func (q *delayingQueue) AddAfter(r reconcile.Request, d time.Duration) {
	q.Added[r] = d
}

func TestEnqueueAllAfter(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	objects := []client.Object{
		&v1alpha1.PoolObservation{ObjectMeta: metav1.ObjectMeta{Name: "gpu"}, Spec: v1alpha1.PoolObservationSpec{PoolName: "gpu"}},
		&v1alpha1.PoolObservation{ObjectMeta: metav1.ObjectMeta{Name: "gpu-spot"}, Spec: v1alpha1.PoolObservationSpec{PoolName: "gpu-spot"}},
	}
	all := map[reconcile.Request]time.Duration{
		{NamespacedName: types.NamespacedName{Name: "gpu"}}:      10 * time.Second,
		{NamespacedName: types.NamespacedName{Name: "gpu-spot"}}: 10 * time.Second,
	}

	type queue = workqueue.TypedRateLimitingInterface[reconcile.Request]

	cases := map[string]struct {
		reason string
		send   func(ctx context.Context, h handler.Funcs, q queue)
		want   map[reconcile.Request]time.Duration
	}{
		"Create": {
			reason: "A created object should enqueue every PoolObservation after the delay.",
			send: func(ctx context.Context, h handler.Funcs, q queue) {
				h.Create(ctx, event.CreateEvent{}, q)
			},
			want: all,
		},
		"Update": {
			reason: "An updated object should enqueue every PoolObservation after the delay.",
			send: func(ctx context.Context, h handler.Funcs, q queue) {
				h.Update(ctx, event.UpdateEvent{}, q)
			},
			want: all,
		},
		"Delete": {
			reason: "A deleted object should enqueue every PoolObservation after the delay.",
			send: func(ctx context.Context, h handler.Funcs, q queue) {
				h.Delete(ctx, event.DeleteEvent{}, q)
			},
			want: all,
		},
		"Generic": {
			reason: "A generic event should enqueue every PoolObservation after the delay.",
			send: func(ctx context.Context, h handler.Funcs, q queue) {
				h.Generic(ctx, event.GenericEvent{}, q)
			},
			want: all,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			q := &delayingQueue{Added: map[reconcile.Request]time.Duration{}}
			tc.send(t.Context(), EnqueueAllAfter(c, 10*time.Second), q)
			if diff := cmp.Diff(tc.want, q.Added); diff != "" {
				t.Errorf("\n%s\nEnqueueAllAfter(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}
