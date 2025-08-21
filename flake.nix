{
  description = "Wings development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils = {
      url = "github:numtide/flake-utils";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane = {
      url = "github:ipetkov/crane";
    };
  };

  outputs = { nixpkgs, rust-overlay, flake-utils, crane, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [
          (import rust-overlay)
        ];

        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rustToolchain = (pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml).override {
          extensions = [ "rust-src" "rust-analyzer" ];
        };

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        src = pkgs.lib.cleanSourceWith {
          src = craneLib.path ./.;
          filter = path: type:
            (builtins.match ".*proto$" path != null)
            || (craneLib.filterCargoSources path type);
        };

        commonArgs = {
          inherit src;
          nativeBuildInputs = with pkgs; [
            pkg-config
            protobuf
            openssl.dev
          ] ++ pkgs.lib.optional stdenv.isDarwin (with pkgs.darwin.apple_sdk.frameworks; [
            CoreFoundation
            CoreServices
            Security
            SystemConfiguration
          ]);
        };

        cargoArtifacts = craneLib.buildDepsOnly (commonArgs // {
          pname = "wings";
          version = "0.0.0";
        });

        binary = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          pname = "wings";
          version = "0.0.0";
          doCheck = false;
        });

        dockerImage = pkgs.dockerTools.buildImage {
          name = "ghcr.io/apibara/wings";
          tag = "latest";
          created = "now";
          copyToRoot = pkgs.buildEnv {
            name = "image-root";
            paths = [ binary ];
            pathsToLink = [ "/bin" ];
          };
          config = {
            Entrypoint = "/bin/wings";
            ExposedPorts = {
              "7777" = { };
              "7780" = { };
            };
          };
        };

        dockerArchive = pkgs.stdenv.mkDerivation {
          name = "wings-image";
          buildInputs = [
            pkgs.skopeo
          ];
          phases = [ "installPhase" ];
          installPhase = ''
            mkdir -p $out
            echo '{"default": [{"type": "insecureAcceptAnything"}]}' > /tmp/policy.json
            skopeo copy --policy=/tmp/policy.json --tmpdir=/tmp docker-archive:${dockerImage} docker-archive:$out/wings.tar.gz
          '';
        };

        publishDockerImage = pkgs.writeScriptBin "publish-docker-image" ''
          set -euo pipefail

          echo "Ref: $GITHUB_REF"

          if [[ "''${GITHUB_REF:-}" != "refs/tags/"* ]]; then
            echo "Not a tag"
            exit 0
          fi

          if ! [ -f "''${IMAGE_ARCHIVE_x86_64}" ]; then
            echo "IMAGE_ARCHIVE_x86_64 image does not exist"
            exit 1
          fi

          if ! [ -f "''${IMAGE_ARCHIVE_aarch64}" ]; then
            echo "IMAGE_ARCHIVE_aarch64 image does not exist"
            exit 1
          fi

          skopeo="${pkgs.skopeo}/bin/skopeo"
          echo '{"default": [{"type": "insecureAcceptAnything"}]}' > /tmp/policy.json

          echo "::group::Logging in to ghcr.io"
          echo "::add-mask::''${GHCR_USERNAME}"
          echo "::add-mask::''${GHCR_PASSWORD}"

          skopeo login -u="''${GHCR_USERNAME}" -p="''${GHCR_PASSWORD}" ghcr.io
          echo "::endgroup::"

          tag=''${GITHUB_REF#refs/tags/}
          base="ghcr.io/apibara/wings"

          echo "::group::Pushing docker image to ''${tag}"
          skopeo copy "docker-archive:''${IMAGE_ARCHIVE_x86_64}" "docker://''${base}:''${tag}-x86_64"
          skopeo copy "docker-archive:''${IMAGE_ARCHIVE_aarch64}" "docker://''${base}:''${tag}-aarch64"
          echo "::endgroup::"

          # images=("''${base}:''${tag}-x86_64" "''${base}:''${tag}-aarch64")
          # echo "::group::Create manifest ''${base}:''${tag}"
          # manifest="''${base}:''${tag}"
          # buildah manifest create "''${manifest}" "''${images[@]}"
          # echo "::endgroup::"
        '';
      in
      {
        packages = {
          default = binary;
          image = dockerArchive;
        };

        # development shells. start with `nix develop`.
        devShells = {
          default = pkgs.mkShell {
            LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
              pkgs.stdenv.cc.cc
              pkgs.openssl
            ];
            inputsFrom = [ binary ];
            nativeBuildInputs = with pkgs; [
              opentelemetry-collector
            ];
          };
          ci = pkgs.mkShell {
            buildInputs = [ publishDockerImage ];
          };
        };
      }
    );
}
