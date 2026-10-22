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

// Package poolobservation reports the state of a GPU pool in the status of a
// PoolObservation.
package poolobservation

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/modelplaneai/cluster-agent/apis/v1alpha1"
	"github.com/modelplaneai/cluster-agent/internal/allocation"
	"github.com/modelplaneai/cluster-agent/internal/pool"
	"github.com/modelplaneai/cluster-agent/internal/replica"
)

// +kubebuilder:rbac:groups="",resources=nodes;pods;events,verbs=get;list;watch
// +kubebuilder:rbac:groups=resource.k8s.io,resources=resourceslices;resourceclaims;deviceclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=agent.modelplane.ai,resources=poolobservations,verbs=get;list;watch
// +kubebuilder:rbac:groups=agent.modelplane.ai,resources=poolobservations/status,verbs=get;patch

const (
	timeout = 1 * time.Minute

	// FieldOwner owns the status fields the agent applies.
	FieldOwner = "cluster-agent.modelplane.ai"
)

// A PoolObserver observes a GPU pool. It must not modify its inputs, which
// the Reconciler reads straight from its cache.
type PoolObserver interface {
	ObservePool(ctx context.Context, name string, in pool.Inputs) (pool.Observation, error)
}

// A PoolObserverFn is a function that satisfies PoolObserver.
type PoolObserverFn func(ctx context.Context, name string, in pool.Inputs) (pool.Observation, error)

// ObservePool calls fn.
func (fn PoolObserverFn) ObservePool(ctx context.Context, name string, in pool.Inputs) (pool.Observation, error) {
	return fn(ctx, name, in)
}

// A Reconciler reports the state of a GPU pool in the status of a
// PoolObservation.
type Reconciler struct {
	client   client.Client
	record   *allocation.Record
	observer PoolObserver
	log      *slog.Logger
}

// A ReconcilerOption configures a Reconciler.
type ReconcilerOption func(*Reconciler)

// WithLogger sets how the Reconciler logs. It doesn't log by default.
func WithLogger(l *slog.Logger) ReconcilerOption {
	return func(r *Reconciler) { r.log = l }
}

// WithPoolObserver sets how the Reconciler observes pools. It uses a
// pool.Observer by default.
func WithPoolObserver(o PoolObserver) ReconcilerOption {
	return func(r *Reconciler) { r.observer = o }
}

// NewReconciler returns a Reconciler that reads and writes through the
// supplied client, and reads ResourceClaims through the supplied Record.
func NewReconciler(c client.Client, record *allocation.Record, opts ...ReconcilerOption) *Reconciler {
	r := &Reconciler{
		client:   c,
		record:   record,
		observer: pool.NewObserver(),
		log:      slog.New(slog.DiscardHandler),
	}
	for _, fn := range opts {
		fn(r)
	}
	return r
}

// Setup adds a PoolObservation controller to the manager. The controller
// observes every pool whenever an object it reads changes, but waits for the
// supplied delay first, so that a burst of changes produces one observation
// of each pool. It reads ResourceClaims through the supplied Record.
func Setup(mgr ctrl.Manager, record *allocation.Record, log *slog.Logger, delay time.Duration) error {
	c := mgr.GetClient()
	enqueue := EnqueueAllAfter(c, delay)
	err := ctrl.NewControllerManagedBy(mgr).
		Named("poolobservation").
		For(&v1alpha1.PoolObservation{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&corev1.Node{}, enqueue).
		Watches(&resourcev1.ResourceSlice{}, enqueue).
		Watches(&resourcev1.DeviceClass{}, enqueue).
		Watches(&resourcev1.ResourceClaim{}, enqueue).
		Watches(&corev1.Pod{}, enqueue).
		Watches(&corev1.Event{}, enqueue).
		Complete(NewReconciler(c, record, WithLogger(log.With("controller", "poolobservation"))))
	if err != nil {
		return fmt.Errorf("cannot create controller: %w", err)
	}
	return nil
}

// EnqueueAllAfter returns an event handler that enqueues every PoolObservation
// after the supplied delay, whatever the event. A burst of events enqueues
// each PoolObservation once, because a work queue keeps the earliest time it's
// due to process a request it already holds.
func EnqueueAllAfter(c client.Reader, delay time.Duration) handler.Funcs {
	type queue = workqueue.TypedRateLimitingInterface[reconcile.Request]
	enqueue := func(ctx context.Context, q queue) {
		l := &v1alpha1.PoolObservationList{}
		if err := c.List(ctx, l, client.UnsafeDisableDeepCopy); err != nil {
			return
		}
		for _, po := range l.Items {
			q.AddAfter(reconcile.Request{NamespacedName: types.NamespacedName{Name: po.GetName()}}, delay)
		}
	}
	return handler.Funcs{
		CreateFunc:  func(ctx context.Context, _ event.CreateEvent, q queue) { enqueue(ctx, q) },
		UpdateFunc:  func(ctx context.Context, _ event.UpdateEvent, q queue) { enqueue(ctx, q) },
		DeleteFunc:  func(ctx context.Context, _ event.DeleteEvent, q queue) { enqueue(ctx, q) },
		GenericFunc: func(ctx context.Context, _ event.GenericEvent, q queue) { enqueue(ctx, q) },
	}
}

// Reconcile observes a GPU pool and reports it in the status of the requested
// PoolObservation.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := r.log.With("request", req)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	observation := &v1alpha1.PoolObservation{}
	if err := r.client.Get(ctx, req.NamespacedName, observation); err != nil {
		// Creating the PoolObservation triggers a reconcile, so there's
		// nothing to do until it exists.
		if kerrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("cannot get PoolObservation: %w", err)
	}
	name := observation.Spec.PoolName

	// The observer mustn't modify its inputs, so read them straight from the
	// cache rather than copying them.
	nodes := &corev1.NodeList{}
	if err := r.client.List(ctx, nodes, client.MatchingLabels{pool.LabelPool: name}, client.UnsafeDisableDeepCopy); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot list nodes: %w", err)
	}
	rslices := &resourcev1.ResourceSliceList{}
	if err := r.client.List(ctx, rslices, client.UnsafeDisableDeepCopy); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot list ResourceSlices: %w", err)
	}
	classes := &resourcev1.DeviceClassList{}
	if err := r.client.List(ctx, classes, client.UnsafeDisableDeepCopy); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot list DeviceClasses: %w", err)
	}
	claims := &resourcev1.ResourceClaimList{}
	allocatedAt, err := r.record.Read(ctx, func() ([]resourcev1.ResourceClaim, error) {
		if err := r.client.List(ctx, claims, client.UnsafeDisableDeepCopy); err != nil {
			return nil, fmt.Errorf("cannot list ResourceClaims: %w", err)
		}
		return claims.Items, nil
	})
	if err != nil {
		return reconcile.Result{}, err //nolint:wrapcheck // Read wraps its own errors, and returns the read function's unchanged.
	}
	pods := &corev1.PodList{}
	if err := r.client.List(ctx, pods, client.HasLabels{replica.LabelReplica}, client.UnsafeDisableDeepCopy); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot list pods: %w", err)
	}
	events := &corev1.EventList{}
	if err := r.client.List(ctx, events, client.UnsafeDisableDeepCopy); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot list events: %w", err)
	}

	o, err := r.observer.ObservePool(ctx, name, pool.Inputs{
		Nodes:   nodes.Items,
		Slices:  rslices.Items,
		Classes: classes.Items,
		Claims:  claims.Items,
		Pods:    pods.Items,
		Events:  events.Items,

		AllocatedAt: allocatedAt,
	})
	if err != nil {
		// Observing the same objects again would fail again. A change to
		// any of them triggers a reconcile.
		log.Debug("Cannot observe pool", "error", err)
		s := FailedStatus(observation.Status, observation.GetGeneration(), fmt.Errorf("cannot observe pool: %w", err))
		return reconcile.Result{}, ApplyStatus(ctx, r.client, req.NamespacedName, s)
	}

	err = ApplyStatus(ctx, r.client, req.NamespacedName, Status(observation.Status, observation.GetGeneration(), o))
	if kerrors.IsInvalid(err) {
		// The API server rejects a status that exceeds the schema's bounds,
		// and will until the pool changes.
		log.Debug("Status was rejected", "error", err)
		s := FailedStatus(observation.Status, observation.GetGeneration(), err)
		return reconcile.Result{}, ApplyStatus(ctx, r.client, req.NamespacedName, s)
	}
	if err != nil {
		return reconcile.Result{}, err
	}

	log.Debug("Applied status", "pool", name)
	return reconcile.Result{}, nil
}

// ApplyStatus applies the supplied status to the PoolObservation with the
// supplied key. The status is applied whole, so fields it omits are removed.
func ApplyStatus(ctx context.Context, c client.Client, key client.ObjectKey, s v1alpha1.PoolObservationStatus) error {
	status, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&s)
	if err != nil {
		return fmt.Errorf("cannot convert status to unstructured: %w", err)
	}
	u := &unstructured.Unstructured{Object: map[string]any{"status": status}}
	u.SetGroupVersionKind(v1alpha1.PoolObservationGroupVersionKind)
	u.SetNamespace(key.Namespace)
	u.SetName(key.Name)
	if err := c.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(FieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("cannot apply status: %w", err)
	}
	return nil
}
