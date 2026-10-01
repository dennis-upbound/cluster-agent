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

// Package replicaobservation reports the state of a replica's pods and their
// devices in the status of its ReplicaObservation.
package replicaobservation

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/modelplaneai/cluster-agent/apis/v1alpha1"
	"github.com/modelplaneai/cluster-agent/internal/replica"
)

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=resource.k8s.io,resources=resourceslices;resourceclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=agent.modelplane.ai,resources=replicaobservations,verbs=get;list;watch
// +kubebuilder:rbac:groups=agent.modelplane.ai,resources=replicaobservations/status,verbs=get;patch

const (
	timeout = 1 * time.Minute

	// FieldOwner owns the status fields the agent applies.
	FieldOwner = "cluster-agent.modelplane.ai"
)

// A ReplicaObserver observes a replica. It must not modify its inputs, which
// the Reconciler reads straight from its cache.
type ReplicaObserver interface {
	ObserveReplica(in replica.Inputs) replica.Observation
}

// A ReplicaObserverFn is a function that satisfies ReplicaObserver.
type ReplicaObserverFn func(in replica.Inputs) replica.Observation

// ObserveReplica calls fn.
func (fn ReplicaObserverFn) ObserveReplica(in replica.Inputs) replica.Observation {
	return fn(in)
}

// A Reconciler reports the state of a replica's pods and their devices in
// the status of its ReplicaObservation.
type Reconciler struct {
	client   client.Client
	observer ReplicaObserver
	log      *slog.Logger
}

// A ReconcilerOption configures a Reconciler.
type ReconcilerOption func(*Reconciler)

// WithLogger sets how the Reconciler logs. It doesn't log by default.
func WithLogger(l *slog.Logger) ReconcilerOption {
	return func(r *Reconciler) { r.log = l }
}

// WithReplicaObserver sets how the Reconciler observes replicas. It uses
// replica.Observe by default.
func WithReplicaObserver(o ReplicaObserver) ReconcilerOption {
	return func(r *Reconciler) { r.observer = o }
}

// NewReconciler returns a Reconciler that reads and writes through the
// supplied client.
func NewReconciler(c client.Client, opts ...ReconcilerOption) *Reconciler {
	r := &Reconciler{
		client:   c,
		observer: ReplicaObserverFn(replica.Observe),
		log:      slog.New(slog.DiscardHandler),
	}
	for _, fn := range opts {
		fn(r)
	}
	return r
}

// Setup adds a ReplicaObservation controller to the manager.
func Setup(mgr ctrl.Manager, log *slog.Logger) error {
	c := mgr.GetClient()
	err := ctrl.NewControllerManagedBy(mgr).
		Named("replicaobservation").
		For(&v1alpha1.ReplicaObservation{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(PodRequests(c))).
		// The scheduler allocates a pod's claims before it binds the pod,
		// but the claim cache can lag the pod cache. Watching claims catches
		// an allocation the pod's binding raced ahead of.
		Watches(&resourcev1.ResourceClaim{}, handler.EnqueueRequestsFromMapFunc(ClaimRequests(c))).
		Complete(NewReconciler(c, WithLogger(log.With("controller", "replicaobservation"))))
	if err != nil {
		return fmt.Errorf("cannot create controller: %w", err)
	}
	return nil
}

// PodRequests returns a function that maps a pod to the ReplicaObservations
// in its namespace whose pod selector matches it.
func PodRequests(c client.Reader) handler.MapFunc {
	return func(ctx context.Context, o client.Object) []reconcile.Request {
		return RequestsFor(ctx, c, o.GetNamespace(), o.GetLabels())
	}
}

// ClaimRequests returns a function that maps a ResourceClaim to the
// ReplicaObservations whose pod selector matches a pod the claim is reserved
// for. The scheduler reserves a claim for a pod when it allocates the claim.
func ClaimRequests(c client.Reader) handler.MapFunc {
	return func(ctx context.Context, o client.Object) []reconcile.Request {
		claim, ok := o.(*resourcev1.ResourceClaim)
		if !ok {
			return nil
		}
		var reqs []reconcile.Request
		for _, ref := range claim.Status.ReservedFor {
			if ref.APIGroup != "" || ref.Resource != "pods" {
				continue
			}
			pod := &corev1.Pod{}
			if err := c.Get(ctx, types.NamespacedName{Namespace: claim.GetNamespace(), Name: ref.Name}, pod); err != nil {
				continue
			}
			reqs = append(reqs, RequestsFor(ctx, c, pod.GetNamespace(), pod.GetLabels())...)
		}
		return reqs
	}
}

// RequestsFor returns requests for the ReplicaObservations in the namespace
// whose pod selector matches the supplied pod labels.
func RequestsFor(ctx context.Context, c client.Reader, namespace string, podLabels map[string]string) []reconcile.Request {
	// Nothing here modifies the ReplicaObservations, so read them straight
	// from the cache rather than copying them.
	l := &v1alpha1.ReplicaObservationList{}
	if err := c.List(ctx, l, client.InNamespace(namespace), client.UnsafeDisableDeepCopy); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for _, ro := range l.Items {
		sel, err := metav1.LabelSelectorAsSelector(&ro.Spec.PodSelector)
		if err != nil || !sel.Matches(labels.Set(podLabels)) {
			continue
		}
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ro.GetNamespace(), Name: ro.GetName()}})
	}
	return reqs
}

// Reconcile observes a replica and reports it in the status of the requested
// ReplicaObservation.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := r.log.With("request", req)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	observation := &v1alpha1.ReplicaObservation{}
	if err := r.client.Get(ctx, req.NamespacedName, observation); err != nil {
		// Creating the ReplicaObservation triggers a reconcile, so there's
		// nothing to do until it exists.
		if kerrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("cannot get ReplicaObservation: %w", err)
	}

	sel, err := metav1.LabelSelectorAsSelector(&observation.Spec.PodSelector)
	if err != nil {
		// Whoever wrote the selector has to fix it, and their fix will
		// trigger a reconcile.
		log.Debug("Invalid pod selector", "error", err)
		s := FailedStatus(observation.Status, observation.GetGeneration(), v1alpha1.ReasonInvalidPodSelector, err)
		return reconcile.Result{}, ApplyStatus(ctx, r.client, req.NamespacedName, s)
	}

	// The observer mustn't modify its inputs, so read them straight from the
	// cache rather than copying them.
	pods := &corev1.PodList{}
	if err := r.client.List(ctx, pods, client.InNamespace(observation.GetNamespace()), client.MatchingLabelsSelector{Selector: sel}, client.HasLabels{replica.LabelReplica}, client.UnsafeDisableDeepCopy); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot list pods: %w", err)
	}
	var claims []resourcev1.ResourceClaim
	for _, p := range pods.Items {
		for _, name := range replica.ClaimNames(p) {
			claim := &resourcev1.ResourceClaim{}
			err := r.client.Get(ctx, types.NamespacedName{Namespace: p.GetNamespace(), Name: name}, claim, client.UnsafeDisableDeepCopy)
			if kerrors.IsNotFound(err) {
				// A claim that doesn't exist hasn't allocated any devices.
				continue
			}
			if err != nil {
				return reconcile.Result{}, fmt.Errorf("cannot get ResourceClaim %q: %w", name, err)
			}
			claims = append(claims, *claim)
		}
	}
	rslices := &resourcev1.ResourceSliceList{}
	if err := r.client.List(ctx, rslices, client.UnsafeDisableDeepCopy); err != nil {
		return reconcile.Result{}, fmt.Errorf("cannot list ResourceSlices: %w", err)
	}

	o := r.observer.ObserveReplica(replica.Inputs{Pods: pods.Items, Claims: claims, Slices: rslices.Items})
	err = ApplyStatus(ctx, r.client, req.NamespacedName, Status(observation.Status, observation.GetGeneration(), o))
	if kerrors.IsInvalid(err) {
		// The API server rejects a status that exceeds the schema's bounds,
		// and will until the replica changes.
		log.Debug("Status was rejected", "error", err)
		s := FailedStatus(observation.Status, observation.GetGeneration(), v1alpha1.ReasonObservationFailed, err)
		return reconcile.Result{}, ApplyStatus(ctx, r.client, req.NamespacedName, s)
	}
	if err != nil {
		return reconcile.Result{}, err
	}

	log.Debug("Applied status")
	return reconcile.Result{}, nil
}

// ApplyStatus applies the supplied status to the ReplicaObservation with the
// supplied key. The status is applied whole, so fields it omits are removed.
func ApplyStatus(ctx context.Context, c client.Client, key client.ObjectKey, s v1alpha1.ReplicaObservationStatus) error {
	status, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&s)
	if err != nil {
		return fmt.Errorf("cannot convert status to unstructured: %w", err)
	}
	u := &unstructured.Unstructured{Object: map[string]any{"status": status}}
	u.SetGroupVersionKind(v1alpha1.ReplicaObservationGroupVersionKind)
	u.SetNamespace(key.Namespace)
	u.SetName(key.Name)
	if err := c.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(FieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("cannot apply status: %w", err)
	}
	return nil
}
