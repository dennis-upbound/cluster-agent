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

// The cluster-agent command observes a Modelplane InferenceCluster and reports
// what it sees in the status of ClusterObservations and ReplicaObservations.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/modelplaneai/cluster-agent/apis/v1alpha1"
	"github.com/modelplaneai/cluster-agent/internal/controller/clusterobservation"
	"github.com/modelplaneai/cluster-agent/internal/controller/replicaobservation"
	"github.com/modelplaneai/cluster-agent/internal/pool"
)

func main() {
	var (
		debug   = flag.Bool("debug", false, "Log debug messages.")
		delay   = flag.Duration("observation-delay", 10*time.Second, "How long to wait after the cluster changes before observing its pools. Changes made while waiting are observed together.")
		metrics = flag.String("metrics-bind-address", ":8080", "The address the metrics endpoint binds to.")
		probe   = flag.String("health-probe-bind-address", ":8081", "The address the health probe endpoint binds to.")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	log := slog.New(h)
	ctrl.SetLogger(logr.FromSlogHandler(h))

	if err := run(ctrl.SetupSignalHandler(), log, *delay, *metrics, *probe); err != nil {
		log.Error("Cluster agent failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger, delay time.Duration, metrics, probe string) error {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		return fmt.Errorf("cannot add Kubernetes types to scheme: %w", err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		return fmt.Errorf("cannot add observation types to scheme: %w", err)
	}

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("cannot get Kubernetes client config: %w", err)
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 s,
		Cache:                  CacheOptions(),
		Metrics:                metricsserver.Options{BindAddress: metrics},
		HealthProbeBindAddress: probe,
	})
	if err != nil {
		return fmt.Errorf("cannot create manager: %w", err)
	}

	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return fmt.Errorf("cannot add health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("ping", healthz.Ping); err != nil {
		return fmt.Errorf("cannot add ready check: %w", err)
	}

	if err := clusterobservation.Setup(mgr, log, delay); err != nil {
		return fmt.Errorf("cannot set up ClusterObservation controller: %w", err)
	}
	if err := replicaobservation.Setup(mgr, log); err != nil {
		return fmt.Errorf("cannot set up ReplicaObservation controller: %w", err)
	}

	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("cannot run manager: %w", err)
	}
	return nil
}

// CacheOptions returns options that keep the agent's cache to the objects it
// needs: nodes in a pool, pods of a replica, and the cluster autoscaler's
// events.
func CacheOptions() cache.Options {
	return cache.Options{
		DefaultTransform: cache.TransformStripManagedFields(),
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Node{}: {
				Label:     labels.NewSelector().Add(exists(pool.LabelPool)),
				Transform: StripNode,
			},
			&corev1.Pod{}: {
				Label: labels.NewSelector().Add(exists(pool.LabelReplica)),
			},
			&corev1.Event{}: {
				Field: fields.OneTermEqualSelector("source", "cluster-autoscaler"),
			},
		},
	}
}

// StripNode removes the fields the agent doesn't read and that make nodes
// large: managed fields, and the images the node has pulled.
func StripNode(o any) (any, error) {
	n, ok := o.(*corev1.Node)
	if !ok {
		return o, nil
	}
	n.ManagedFields = nil
	n.Status.Images = nil
	return n, nil
}

func exists(key string) labels.Requirement {
	r, err := labels.NewRequirement(key, selection.Exists, nil)
	if err != nil {
		// Only an invalid label key makes this fail.
		panic(err)
	}
	return *r
}
