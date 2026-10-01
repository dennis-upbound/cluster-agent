# New to Nix? Start here:
#   Language basics:  https://nix.dev/tutorials/nix-language
#   Flakes intro:     https://zero-to-nix.com/concepts/flakes
{
  description = "Modelplane cluster agent";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
  };

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      # Image architectures. The agent runs on Linux nodes only.
      arches = [
        "amd64"
        "arm64"
      ];

      # A dev version from git metadata. (self ? shortRev tests whether the
      # tree is clean; a dirty tree only has dirtyShortRev.)
      version =
        if self ? shortRev then
          "v0.0.0-${toString self.lastModified}-${self.shortRev}"
        else
          "v0.0.0-${toString self.lastModified}-${self.dirtyShortRev or "dev"}";

      # The hash of the Go module dependencies. Refresh it with nix run .#tidy.
      vendorHash = "sha256-LD3ydub0Yq8b+QO3aOZzPK6WOz+PQ50BdobfDH+ZGH0=";

      # Regenerates deepcopy methods, CRDs and RBAC from the Go source.
      generate = ''
        controller-gen object:headerFile=hack/boilerplate.go.txt paths=./apis/...
        controller-gen crd paths=./apis/... output:crd:artifacts:config=cluster/charts/cluster-agent/crds
        controller-gen rbac:roleName=cluster-agent paths=./internal/... output:rbac:artifacts:config=cluster/charts/cluster-agent/templates
      '';

      build =
        pkgs:
        let
          inherit (pkgs) go;

          src = pkgs.lib.fileset.toSource {
            root = ./.;
            fileset = pkgs.lib.fileset.unions [
              ./go.mod
              ./go.sum
              ./.golangci.yml
              ./apis
              ./cluster
              ./cmd
              ./hack
              ./internal
            ];
          };

          # buildGoModule downloads the module dependencies once per
          # derivation name. Download them once and share them with every
          # build, test and check.
          modules =
            ((pkgs.buildGoModule.override { inherit go; }) {
              pname = "cluster-agent";
              version = "modules";
              inherit src vendorHash;
              proxyVendor = true;
            }).goModules;

          # Build with the shared modules. CGO is off, so building for another
          # architecture only needs GOOS and GOARCH, which buildGoModule reads
          # from the go package.
          goModule =
            goAttrs: args:
            ((pkgs.buildGoModule.override { go = goAttrs; }) (
              {
                inherit src version vendorHash;
                proxyVendor = true;
                env.CGO_ENABLED = "0";
              }
              // args
            )).overrideAttrs
              { goModules = modules; };

          agent =
            arch:
            goModule
              (
                go
                // {
                  GOOS = "linux";
                  GOARCH = arch;
                }
              )
              {
                pname = "cluster-agent-${arch}";
                subPackages = [ "cmd/cluster-agent" ];
                ldflags = [
                  "-s"
                  "-w"
                ];
                doCheck = false;
                # go install puts binaries for another platform in a
                # subdirectory.
                postInstall = ''
                  if [ -d $out/bin/linux_${arch} ]; then
                    mv $out/bin/linux_${arch}/* $out/bin/
                    rmdir $out/bin/linux_${arch}
                  fi
                '';
              };

          image =
            arch:
            pkgs.dockerTools.buildLayeredImage {
              name = "modelplane/cluster-agent";
              tag = version;
              architecture = arch;
              # Uncompressed, so crane can push it.
              compressor = "none";
              contents = [ (agent arch) ];
              config = {
                Entrypoint = [ "/bin/cluster-agent" ];
                User = "65532";
                Labels = {
                  "org.opencontainers.image.source" = "https://github.com/modelplaneai/cluster-agent";
                  "org.opencontainers.image.version" = version;
                };
              };
            };

          # The Helm chart, packaged at the agent's version. Helm wants a version
          # without the v prefix.
          chart =
            pkgs.runCommand "cluster-agent-chart-${version}" { nativeBuildInputs = [ pkgs.kubernetes-helm ]; }
              ''
                mkdir -p $out
                helm package ${./cluster/charts/cluster-agent} \
                  --version ${pkgs.lib.removePrefix "v" version} \
                  --app-version ${version} \
                  --destination $out
              '';

          # A check that runs a script against a writable copy of the source.
          check =
            name: nativeBuildInputs: script:
            goModule go {
              pname = "cluster-agent-${name}";
              inherit nativeBuildInputs;
              dontBuild = true;
              checkPhase = ''
                runHook preCheck
                ${script}
                runHook postCheck
              '';
              installPhase = "touch $out";
            };
        in
        {
          inherit
            modules
            agent
            image
            chart
            check
            ;
        };
    in
    {
      packages = forAllSystems (
        pkgs:
        let
          b = build pkgs;
          native = if pkgs.stdenv.hostPlatform.isAarch64 then "arm64" else "amd64";
        in
        {
          default = b.agent native;
          inherit (b) modules chart;
        }
        // builtins.listToAttrs (
          map (a: {
            name = "image-${a}";
            value = b.image a;
          }) arches
        )
      );

      checks = forAllSystems (
        pkgs:
        let
          b = build pkgs;
        in
        {
          test = b.check "test" [ ] "go test ./...";
          go-lint = b.check "go-lint" [ pkgs.golangci-lint ] ''
            export GOLANGCI_LINT_CACHE=$TMPDIR/golangci-lint
            golangci-lint run
          '';
          generate = b.check "generate" [ pkgs.kubernetes-controller-tools ] ''
            cp -r . $TMPDIR/generated
            chmod -R u+w $TMPDIR/generated
            pushd $TMPDIR/generated
            ${generate}
            popd
            diff -r . $TMPDIR/generated || (echo "Generated files are stale. Run nix run .#generate." && exit 1)
          '';
          helm-lint =
            pkgs.runCommand "cluster-agent-helm-lint" { nativeBuildInputs = [ pkgs.kubernetes-helm ]; }
              ''
                helm lint --strict ${./cluster/charts/cluster-agent}
                touch $out
              '';
          nix-lint = pkgs.runCommand "cluster-agent-nix-lint" { nativeBuildInputs = [ pkgs.nixfmt ]; } ''
            nixfmt --check ${./flake.nix}
            touch $out
          '';
        }
      );

      apps = forAllSystems (
        pkgs:
        let
          b = build pkgs;
          app = name: description: runtimeInputs: text: {
            type = "app";
            meta.description = description;
            program = pkgs.lib.getExe (
              pkgs.writeShellApplication {
                name = "cluster-agent-${name}";
                inherit runtimeInputs text;
              }
            );
          };
        in
        {
          generate = app "generate" "Regenerate deepcopy methods, CRDs and RBAC" [
            pkgs.go
            pkgs.kubernetes-controller-tools
          ] generate;

          tidy =
            app "tidy" "Run go mod tidy and refresh the Go module hash" [ pkgs.go pkgs.nix pkgs.gnused ]
              ''
                go mod tidy

                # Reset the hash, and read the real one back from the error the
                # build reports.
                sed -i 's|vendorHash = "sha256-[^"]*";|vendorHash = "${pkgs.lib.fakeHash}";|' flake.nix
                got=$(nix build .#modules --no-link --option eval-cache false 2>&1 \
                  | sed -n 's|.*got:[[:space:]]*\(sha256-[A-Za-z0-9+/=]*\).*|\1|p' | head -n1 || true)
                if [ -z "$got" ]; then
                  echo "Could not determine the Go module hash." >&2
                  exit 1
                fi
                sed -i "s|vendorHash = \"sha256-[^\"]*\";|vendorHash = \"$got\";|" flake.nix
                echo "vendorHash = $got"
              '';

          push =
            app "push" "Push a multi-architecture image, e.g. nix run .#push -- ttl.sh/cluster-agent:1h"
              [ pkgs.crane ]
              ''
                ref="''${1:?Usage: nix run .#push -- <registry/repository:tag>}"
                ${pkgs.lib.concatMapStrings (a: ''
                  crane push ${b.image a} "$ref-${a}"
                '') arches}
                crane index append ${pkgs.lib.concatMapStringsSep " " (a: ''-m "$ref-${a}"'') arches} -t "$ref"
                echo "Pushed $ref"
              '';
        }
      );

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          buildInputs = [
            pkgs.go
            pkgs.golangci-lint
            pkgs.kubernetes-controller-tools
            pkgs.kubectl
            pkgs.kubernetes-helm
            pkgs.crane
            pkgs.nixfmt
          ];
        };
      });
    };
}
