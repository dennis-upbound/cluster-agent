# cluster-agent

The cluster agent runs on a Modelplane InferenceCluster and reports what it
observes there, for the Modelplane control plane to read. It's an
implementation detail of Modelplane: if you're looking to use Modelplane, start
with the [Modelplane README](https://github.com/modelplaneai/modelplane).

## What it reports

The agent reports in the status of two custom resources, which the control
plane creates for it. The examples below are from a test EKS cluster with a
pool of 4-GPU NVIDIA L4 nodes. They omit each condition's `lastTransitionTime`.

A `PoolObservation` reports on one pool: the nodes that share a
`modelplane.ai/pool` label.

```yaml
apiVersion: agent.modelplane.ai/v1alpha1
kind: PoolObservation
metadata:
  name: gpu-l4x4-a
spec:
  poolName: gpu-l4x4-a
status:
  observedGeneration: 1
  conditions:
  - type: Healthy             # False when a node is unready or cordoned, or a
    status: "True"            # device is tainted.
    reason: PoolHealthy
    observedGeneration: 1
  - type: ScaleUpBlocked
    status: "True"
    reason: ScaleUpNotTriggered
    message: "pod ml-test/too-big-5859cf75cf-jzm8n: pod didn't trigger
      scale-up: 1 node(s) didn't match Pod's node affinity/selector, 1 not
      ready for scale-up, 1 max node group size reached"
    observedGeneration: 1
  nodes:
    total: 1
    ready: 1
    schedulable: 1            # Ready, not cordoned, and not tainted against
                              # Modelplane's GPU pods.
  unschedulablePods: 1        # Pinned pods the scheduler can't place.
  devices:
  - deviceClassName: gpu.nvidia.com
    nodes: 1
    allocatable: 4
    allocated: 3
    attributes:               # Values every GPU in the pool shares.
      productName: {string: NVIDIA L4}
      driverVersion: {version: 580.178.4}
      # Six more omitted for brevity.
  nodesByFreeDevices:         # Schedulable nodes, by free devices of each class.
  - freeDevices:
      gpu.nvidia.com: 1
    nodes: 1
  lastAllocationTime: "2026-10-20T10:05:40.318204Z"
```

The fleet scheduler uses `nodesByFreeDevices` to check whether a replica fits
([modelplane#172](https://github.com/modelplaneai/modelplane/issues/172)). Here
a pod claiming one GPU fits, and one claiming two doesn't. Nodes share a bucket
only when they have the same number of free devices of every class, so a pod
that claims GPUs and network devices fits only a node with enough of both. The
histogram's size depends on devices per node, not on how many nodes the pool
has. `ScaleUpBlocked` says the cluster autoscaler won't add a node for a pinned
pod. The pool's `attributes` show what hardware it really has, to check against
its InferenceClass.

A `ReplicaObservation` reports on the pods its selector matches.

```yaml
apiVersion: agent.modelplane.ai/v1alpha1
kind: ReplicaObservation
metadata:
  name: too-big
  namespace: ml-test
spec:
  podSelector:                # The agent only sees pods with a
    matchLabels:              # modelplane.ai/replica label.
      modelplane.ai/replica: too-big
status:
  observedGeneration: 1
  pods:
    total: 1
    scheduled: 0
    ready: 0
  conditions:
  - type: Scheduled
    status: "False"
    reason: Unschedulable
    message: "pod too-big-5859cf75cf-jzm8n: 0/2 nodes are available: 1 cannot
      allocate all claims, 1 node(s) didn't match Pod's node affinity/selector.
      still not schedulable, preemption: 0/2 nodes are available: 2 Preemption
      is not helpful for scheduling."
    observedGeneration: 1
  - type: Ready               # Other reasons include ImagePullBackOff and
    status: "False"           # CrashLoopBackOff.
    reason: NotScheduled
    message: 0 of 1 pods scheduled
    observedGeneration: 1
```

`Scheduled` and `Ready` say why a replica isn't serving. A replica that doesn't
fit might run on another cluster, but one that can't pull its image probably
won't run anywhere. A scheduled replica also lists the DRA devices allocated to
its pods, with each device's node and GPU UUID, and the pools those nodes are
in.

The agent records when it first saw each ResourceClaim allocated. A pool and a
replica each report the latest of those times for their claims on the pool as
`lastAllocationTime`, though a pool's never goes back. A pool whose time is no
earlier than a replica's time for it accounts for the replica's devices in
`nodesByFreeDevices`, unless they've since been deallocated. The fleet scheduler
then knows not to charge the pool for them again.

## Development

The agent is built with Nix. `nix develop` opens a shell with the tools you
need, and `nix flake check` runs the tests, linters and a check that generated
files are current.

- `nix run .#generate` regenerates deepcopy methods, CRDs and RBAC after you
  change the API or a controller's RBAC markers.
- `nix run .#tidy` runs `go mod tidy` and refreshes the flake's Go module hash
  after you change a dependency.
- `nix run .#push -- <registry>/cluster-agent` pushes a multi-architecture image
  and the Helm chart to that repository.
- `nix build .#chart` packages the Helm chart.

To run the agent on a cluster, push an image, install the chart, and create a
PoolObservation for each pool:

```shell
nix run .#push -- ttl.sh/cluster-agent
helm install cluster-agent cluster/charts/cluster-agent \
  --namespace cluster-agent --create-namespace \
  --set image.repository=ttl.sh/cluster-agent \
  --set image.tag=<the version push printed>
kubectl apply -f - <<EOF
apiVersion: agent.modelplane.ai/v1alpha1
kind: PoolObservation
metadata:
  name: gpu
spec:
  poolName: gpu
EOF
```

## Releasing

Releases are cut from a release branch, like Modelplane's. To release
`v0.1.0`:

1. Create a `release-0.1` branch from `main`.
2. Create a GitHub release targeting that branch, and let it create the tag
   `v0.1.0`.
3. Run the `CI` workflow against the `v0.1.0` tag, with its `tag` input set to
   `v0.1.0`.

The `tag` input makes the workflow push the image and chart at that version,
rather than the dev version it derives from git metadata. A patch release
reuses the `release-0.1` branch.
