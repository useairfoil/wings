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

  outputs =
    {
      nixpkgs,
      rust-overlay,
      flake-utils,
      crane,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [
          (import rust-overlay)
        ];

        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rustToolchain = (pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml).override {
          extensions = [
            "rust-src"
            "rust-analyzer"
          ];
        };

        nightlyToolChain = (pkgs.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default));

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        src = pkgs.lib.cleanSourceWith {
          src = craneLib.path ./.;
          filter =
            path: type: (builtins.match ".*proto$" path != null) || (craneLib.filterCargoSources path type);
        };

        commonArgs = {
          inherit src;
          nativeBuildInputs = with pkgs; [
            pkg-config
            protobuf
            openssl.dev
          ];
        };

        cargoArtifacts = craneLib.buildDepsOnly (
          commonArgs
          // {
            pname = "wings";
            version = "0.0.0";
          }
        );

        binaries = craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;
            pname = "wings";
            version = "0.0.0";
            doCheck = false;
            cargoExtraArgs = "--all";
          }
        );

        binariesWithChecksum = pkgs.stdenv.mkDerivation {
          inherit (binaries) pname version src;

          nativeBuildInputs = [ pkgs.perl ];

          installPhase = ''
            mkdir -p $out
            cp ${binaries}/bin/wings $out/wings
            shasum -b -a 256 $out/wings > $out/wings-hash.txt
            cp ${binaries}/bin/wings-stress $out/wings-stress
            shasum -b -a 256 $out/wings-stress > $out/wings-stress-hash.txt
          '';
        };

        dockerImage = pkgs.dockerTools.buildImage {
          name = "ghcr.io/useairfoil/wings";
          tag = "latest";
          created = "now";
          copyToRoot = pkgs.buildEnv {
            name = "image-root";
            paths = [ binaries ];
            pathsToLink = [ "/bin" ];
          };
          config = {
            Entrypoint = [ "/bin/wings" ];
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

        extractVersionFromRef = pkgs.writeShellApplication {
          name = "extract-version-from-ref";
          runtimeInputs = [ pkgs.semver-tool ];
          text = builtins.readFile ./scripts/extract-version-from-ref.sh;
        };

        publishDockerImage = pkgs.writeShellApplication {
          name = "publish-docker-image";
          runtimeInputs = [
            pkgs.buildah
            pkgs.skopeo
          ];
          text = builtins.readFile ./scripts/publish-docker-image.sh;
        };
      in
      {
        packages = {
          default = binariesWithChecksum;
          image = dockerArchive;
          extract-version-from-ref = extractVersionFromRef;
          publish-docker-image = publishDockerImage;
        };

        # development shells. start with `nix develop`.
        devShells = {
          default = pkgs.mkShell {
            LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
              pkgs.stdenv.cc.cc
              pkgs.openssl
            ];

            inputsFrom = [ binaries ];
          };

          ci = pkgs.mkShell {
            buildInputs = [
              extractVersionFromRef
              publishDockerImage
            ];
          };

          nightly = pkgs.mkShell {
            buildInputs = [
              nightlyToolChain
              pkgs.cargo-udeps
            ];
          };
        };
      }
    );
}
