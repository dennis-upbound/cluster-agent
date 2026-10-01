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
	"log/slog"
	"os"
	"time"

	"github.com/alecthomas/kong"
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
	"github.com/modelplaneai/cluster-agent/internal/replica"
)

type cli struct {
	Debug                  bool          `help:"Log debug messages." short:"d"`
	ObservationDelay       time.Duration `default:"10s"              help:"How long to wait after the cluster changes before observing its pools. Changes made while waiting are observed together."`
	MetricsBindAddress     string        `default:":8080"            help:"The address the metrics endpoint binds to."`
	HealthProbeBindAddress string        `default:":8081"            help:"The address the health probe endpoint binds to."`
}

func main() {
	flags := &cli{}
	kctx := kong.Parse(flags,
		kong.Name("cluster-agent"),
		kong.Description("Observe a Modelplane InferenceCluster and report what it sees."),
		kong.UsageOnError(),
	)

	level := slog.LevelInfo
	if flags.Debug {
		level = slog.LevelDebug
	}
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	log := slog.New(h)
	ctrl.SetLogger(logr.FromSlogHandler(h))

	s := runtime.NewScheme()
	kctx.FatalIfErrorf(clientgoscheme.AddToScheme(s), "cannot add Kubernetes types to scheme")
	kctx.FatalIfErrorf(v1alpha1.AddToScheme(s), "cannot add observation types to scheme")

	cfg, err := ctrl.GetConfig()
	kctx.FatalIfErrorf(err, "cannot get Kubernetes client config")

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 s,
		Cache:                  CacheOptions(),
		Metrics:                metricsserver.Options{BindAddress: flags.MetricsBindAddress},
		HealthProbeBindAddress: flags.HealthProbeBindAddress,
	})
	kctx.FatalIfErrorf(err, "cannot create manager")

	kctx.FatalIfErrorf(mgr.AddHealthzCheck("ping", healthz.Ping), "cannot add health check")
	kctx.FatalIfErrorf(mgr.AddReadyzCheck("ping", healthz.Ping), "cannot add ready check")

	kctx.FatalIfErrorf(clusterobservation.Setup(mgr, log, flags.ObservationDelay), "cannot set up ClusterObservation controller")
	kctx.FatalIfErrorf(replicaobservation.Setup(mgr, log), "cannot set up ReplicaObservation controller")

	kctx.FatalIfErrorf(mgr.Start(ctrl.SetupSignalHandler()), "cannot run manager")
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
				Label: labels.NewSelector().Add(exists(replica.LabelReplica)),
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
