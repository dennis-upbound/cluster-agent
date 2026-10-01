# cluster-agent

The cluster agent runs on a Modelplane InferenceCluster and reports what it
observes there, for the Modelplane control plane to read. It's an
implementation detail of Modelplane: if you're looking to use Modelplane, start
with the [Modelplane README](https://github.com/modelplaneai/modelplane).

The agent reports in the status of two custom resources:

- The `ClusterObservation` named `cluster` reports each GPU pool's nodes, a
  histogram of nodes by free DRA devices, the pool's unschedulable pods, and
  whether the cluster autoscaler can add nodes for them.
- A `ReplicaObservation` reports whether a replica's pods are scheduled and
  ready, and the devices allocated to them.

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
