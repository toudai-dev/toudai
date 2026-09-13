{
  description = "Lighthouse";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };

      in
      {
        devShells.default = pkgs.mkShell {
          nativeBuildInputs = with pkgs; [
            cargo
            clippy
            rust-analyzer
            rustc
            rustfmt

            # Tools
            docker-buildx
            docker-compose
            clickhouse-lts
            goose
            buf
            protobuf
            git-cliff
            asciidoctor

            ## Benchmark tools
            google-cloud-sdk
            terraform
          ];
        };
      }
    );
}
