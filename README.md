# cluster-agent

The cluster agent runs on a Modelplane InferenceCluster and reports what it
observes there, for the Modelplane control plane to read. It's an
implementation detail of Modelplane: if you're looking to use Modelplane, start
with the [Modelplane README](https://github.com/modelplaneai/modelplane).

## What it reports

The agent reports in the status of two custom resources. The examples below are
from a test EKS cluster with two pools of 4-GPU NVIDIA L4 nodes, one of them
empty. They omit each condition's `lastTransitionTime`.

The `ClusterObservation` named `cluster` reports each pool: the nodes that share
a `modelplane.ai/pool` label.

```yaml
apiVersion: agent.modelplane.ai/v1alpha1
kind: ClusterObservation
metadata:
  name: cluster
spec: {}
status:
  conditions:
  - type: Ready               # False when the pools below are stale.
    status: "True"
    reason: Observed
  pools:
  - name: gpu-l4x4
    nodes:
      total: 0
      ready: 0
      schedulable: 0
    unschedulablePods: 1      # Pinned pods the scheduler can't place.
    conditions:
    - type: Healthy
      status: "True"
      reason: PoolHealthy
    - type: ScaleUpBlocked
      status: "True"
      reason: ScaleUpNotTriggered
      message: "pod ml-test/stocked-out-db588bffc-bzgt8: pod didn't trigger
        scale-up: 1 node(s) didn't match Pod's node affinity/selector, 1 not
        ready for scale-up, 1 max node group size reached"
  - name: gpu-l4x4-a
    nodes:
      total: 1
      ready: 1
      schedulable: 1          # Ready and not cordoned.
    unschedulablePods: 1
    devices:
    - deviceClassName: gpu.nvidia.com
      nodes: 1
      allocatable: 4
      allocated: 3
      nodesByFreeDevices:     # Schedulable nodes, by how many GPUs are free.
      - freeDevices: 1
        nodes: 1
      attributes:             # Values every GPU in the pool shares.
        productName: {string: NVIDIA L4}
        driverVersion: {version: 580.178.4}
        # Six more omitted for brevity.
    conditions:
    - type: Healthy           # False when a node is unready or cordoned, or a
      status: "True"          # device is tainted.
      reason: PoolHealthy
    - type: ScaleUpBlocked
      status: "True"
      reason: ScaleUpNotTriggered
      message: "pod ml-test/too-big-5859cf75cf-jzm8n: pod didn't trigger
        scale-up: 1 node(s) didn't match Pod's node affinity/selector, 1 not
        ready for scale-up, 1 max node group size reached"
  # The system pool is omitted for brevity.
```

The fleet scheduler can use `nodesByFreeDevices` to check whether a replica fits
([modelplane#172](https://github.com/modelplaneai/modelplane/issues/172)). Here
a pod claiming one GPU fits on `gpu-l4x4-a`, and one claiming two doesn't. Its
size depends on GPUs per node, not on how many nodes a pool has.
`ScaleUpBlocked` says the cluster autoscaler won't add a node for a pinned pod,
as in both GPU pools here. A pool's `attributes` show what hardware it really
has, to check against its InferenceClass.

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
its pods, with each device's node and GPU UUID.

## Development

The agent is built with Nix. `nix develop` opens a shell with the tools you
need, and `nix flake check` runs the tests, linters and a check that generated
files are current.

- `nix run .#generate` regenerates deepcopy methods, CRDs and RBAC after you
  change the API or a controller's RBAC markers.
- `nix run .#tidy` runs `go mod tidy` and refreshes the flake's Go module hash
  after you change a dependency.
- `nix run .#push -- <registry/repository>` pushes a multi-architecture image.
- `nix build .#chart` packages the Helm chart.

To run the agent on a cluster, push an image and install the chart:

```shell
nix run .#push -- ttl.sh/cluster-agent
helm install cluster-agent cluster/charts/cluster-agent \
  --namespace cluster-agent --create-namespace \
  --set image.repository=ttl.sh/cluster-agent \
  --set image.tag=<the version push printed>
kubectl apply -f - <<EOF
apiVersion: agent.modelplane.ai/v1alpha1
kind: ClusterObservation
metadata:
  name: cluster
spec: {}
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

The `tag` input makes the workflow push the image at that version, rather than
the dev version it derives from git metadata. A patch release reuses the
`release-0.1` branch.
