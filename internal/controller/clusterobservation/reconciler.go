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

// Package clusterobservation reports the state of a cluster's GPU pools in
// the status of its ClusterObservation.
package clusterobservation

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
	"github.com/modelplaneai/cluster-agent/internal/pool"
)

// +kubebuilder:rbac:groups="",resources=nodes;pods;events,verbs=get;list;watch
// +kubebuilder:rbac:groups=resource.k8s.io,resources=resourceslices;resourceclaims;deviceclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=agent.modelplane.ai,resources=clusterobservations,verbs=get;list;watch
// +kubebuilder:rbac:groups=agent.modelplane.ai,resources=clusterobservations/status,verbs=get;patch

const (
	timeout = 1 * time.Minute

	// FieldOwner owns the status fields the agent applies.
	FieldOwner = "cluster-agent.modelplane.ai"
)

// A PoolObserver observes a cluster's GPU pools.
type PoolObserver interface {
	ObservePools(ctx context.Context, in pool.Inputs) ([]pool.Observation, error)
}

// A PoolObserverFn is a function that satisfies PoolObserver.
type PoolObserverFn func(ctx context.Context, in pool.Inputs) ([]pool.Observation, error)

// ObservePools calls fn.
func (fn PoolObserverFn) ObservePools(ctx context.Context, in pool.Inputs) ([]pool.Observation, error) {
	return fn(ctx, in)
}

// A Reconciler reports the state of a cluster's GPU pools in the status of
// its ClusterObservation.
type Reconciler struct {
	client   client.Client
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
// supplied client.
func NewReconciler(c client.Client, opts ...ReconcilerOption) *Reconciler {
	r := &Reconciler{
		client:   c,
		observer: pool.NewObserver(),
		log:      slog.New(slog.DiscardHandler),
	}
	for _, fn := range opts {
		fn(r)
	}
	return r
}

// Setup adds a ClusterObservation controller to the manager. The controller
// observes the cluster's pools whenever an object it reads changes, but waits
// for the supplied delay first, so that a burst of changes produces one
// observation.
func Setup(mgr ctrl.Manager, log *slog.Logger, delay time.Duration) error {
	enqueue := EnqueueAfter(reconcile.Request{NamespacedName: types.NamespacedName{Name: v1alpha1.ClusterObservationName}}, delay)
	err := ctrl.NewControllerManagedBy(mgr).
		Named("clusterobservation").
		For(&v1alpha1.ClusterObservation{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&corev1.Node{}, enqueue).
		Watches(&resourcev1.ResourceSlice{}, enqueue).
		Watches(&resourcev1.DeviceClass{}, enqueue).
		Watches(&resourcev1.ResourceClaim{}, enqueue).
		Watches(&corev1.Pod{}, enqueue).
		Watches(&corev1.Event{}, enqueue).
		Complete(NewReconciler(mgr.GetClient(), WithLogger(log.With("controller", "clusterobservation"))))
	if err != nil {
		return fmt.Errorf("cannot create controller: %w", err)
	}
	return nil
}

// EnqueueAfter returns an event handler that enqueues the supplied request
// after the supplied delay, whatever the event. A burst of events enqueues
// the request once, because a work queue keeps the earliest time it's due to
// process a request it already holds.
func EnqueueAfter(req reconcile.Request, delay time.Duration) handler.Funcs {
	type queue = workqueue.TypedRateLimitingInterface[reconcile.Request]
	return handler.Funcs{
		CreateFunc:  func(_ context.Context, _ event.CreateEvent, q queue) { q.AddAfter(req, delay) },
		UpdateFunc:  func(_ context.Context, _ event.UpdateEvent, q queue) { q.AddAfter(req, delay) },
		DeleteFunc:  func(_ context.Context, _ event.DeleteEvent, q queue) { q.AddAfter(req, delay) },
		GenericFunc: func(_ context.Context, _ event.GenericEvent, q queue) { q.AddAfter(req, delay) },
	}
}

// Reconcile observes the cluster's GPU pools and reports them in the status
// of the requested ClusterObservation.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := r.log.With("request", req)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	observation := &v1alpha1.ClusterObservation{}
	if err := r.client.Get(ctx, req.NamespacedName, observation); err != nil {
		// Creating the ClusterObservation triggers a reconcile, so there's
		// nothing to do until it exists.
		if kerrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("cannot get ClusterObservation: %w", err)
	}

	nodes := &corev1.NodeList{}
	if err := r.client.List(ctx, nodes); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot list nodes: %w", err)
	}
	// ResourceSlices are the largest objects the agent reads, and nothing
	// modifies them, so read them without copying them out of the cache.
	rslices := &resourcev1.ResourceSliceList{}
	if err := r.client.List(ctx, rslices, client.UnsafeDisableDeepCopy); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot list ResourceSlices: %w", err)
	}
	classes := &resourcev1.DeviceClassList{}
	if err := r.client.List(ctx, classes); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot list DeviceClasses: %w", err)
	}
	claims := &resourcev1.ResourceClaimList{}
	if err := r.client.List(ctx, claims); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot list ResourceClaims: %w", err)
	}
	pods := &corev1.PodList{}
	if err := r.client.List(ctx, pods); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot list pods: %w", err)
	}
	events := &corev1.EventList{}
	if err := r.client.List(ctx, events); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot list events: %w", err)
	}

	pools, err := r.observer.ObservePools(ctx, pool.Inputs{
		Nodes:   nodes.Items,
		Slices:  rslices.Items,
		Classes: classes.Items,
		Claims:  claims.Items,
		Pods:    pods.Items,
		Events:  events.Items,
	})
	if err != nil {
		// Observing the same objects again would fail again. A change to
		// any of them triggers a reconcile.
		log.Debug("Cannot observe pools", "error", err)
		s := FailedStatus(observation.Status, fmt.Errorf("cannot observe pools: %w", err))
		return reconcile.Result{}, ApplyStatus(ctx, r.client, req.NamespacedName, s)
	}

	err = ApplyStatus(ctx, r.client, req.NamespacedName, Status(observation.Status, pools))
	if kerrors.IsInvalid(err) {
		// The API server rejects a status that exceeds the schema's bounds,
		// and will until the cluster changes.
		log.Debug("Status was rejected", "error", err)
		s := FailedStatus(observation.Status, err)
		return reconcile.Result{}, ApplyStatus(ctx, r.client, req.NamespacedName, s)
	}
	if err != nil {
		return reconcile.Result{}, err
	}

	log.Debug("Applied status", "pools", len(pools))
	return reconcile.Result{}, nil
}

// ApplyStatus applies the supplied status to the ClusterObservation with the
// supplied key. The status is applied whole, so fields it omits are removed.
func ApplyStatus(ctx context.Context, c client.Client, key client.ObjectKey, s v1alpha1.ClusterObservationStatus) error {
	status, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&s)
	if err != nil {
		return fmt.Errorf("cannot convert status to unstructured: %w", err)
	}
	u := &unstructured.Unstructured{Object: map[string]any{"status": status}}
	u.SetGroupVersionKind(v1alpha1.ClusterObservationGroupVersionKind)
	u.SetNamespace(key.Namespace)
	u.SetName(key.Name)
	if err := c.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(FieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("cannot apply status: %w", err)
	}
	return nil
}
