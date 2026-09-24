{
  description = "Lighthouse";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    # The local benchmark harness, the committed Flox environment, and the cloud
    # benchmark images all pin ClickHouse 26.8.2.7-lts. This revision is the one
    # the Flox lock resolved, so it is pinned separately for ClickHouse only
    # instead of moving the whole development shell off nixos-unstable.
    nixpkgs-clickhouse.url = "github:nixos/nixpkgs/8ce4ef6cb6f871616146b9fe26d2a5ae594e94fe";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      nixpkgs-clickhouse,
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
        clickhousePkgs = import nixpkgs-clickhouse {
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
            clickhousePkgs.clickhouse-lts
            goose
            buf
            protobuf
            git-cliff
            asciidoctor

            ## Benchmark tools
            ## `curl`, `fio`, `jq`, `opentofu`, `packer`, and `sysstat` are also
            ## pinned in the committed Flox environment and required by the
            ## benchmark scripts and cloud runbooks.
            awscli2
            curl
            fio
            google-cloud-sdk
            jq
            opentofu
            packer
            sysstat
          ];
        };
      }
    );
}
