{
  description = "PNA-FS development environment";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/c043004d1c6985732bcc1cbc5a9c9aecbbb4e0f0";

  outputs = { nixpkgs, ... }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      pkgsFor = system: import nixpkgs { inherit system; };
      pnaFor = system:
        let
          pkgs = pkgsFor system;
        in
        pkgs.callPackage ./nix/portable-network-archive.nix { inherit system; };
      devPackagesFor = system:
        let
          pkgs = pkgsFor system;
        in
        with pkgs; [
          rustc
          cargo
          clippy
          rustfmt
          pkg-config
          cmake
          git
          gcc
          fuse3
          (lib.getDev fuse3)
          acl
          (lib.getDev acl)
          (pnaFor system)
        ];
    in
    {
      packages = forAllSystems (system:
        let
          pkgs = pkgsFor system;
          portable-network-archive = pnaFor system;
        in
        {
          inherit portable-network-archive;
          dev-tools = pkgs.buildEnv {
            name = "pnafs-dev-tools";
            paths = devPackagesFor system;
          };
          default = portable-network-archive;
        });

      devShells = forAllSystems (system:
        let
          pkgs = pkgsFor system;
        in
        {
          default = pkgs.mkShell {
            packages = devPackagesFor system;
            CARGO_TARGET_DIR = "/tmp/target";
          };
        });

      apps = forAllSystems (system:
        let
          pkgs = pkgsFor system;
          updater = pkgs.writeShellApplication {
            name = "update-pna";
            runtimeInputs = with pkgs; [ curl jq python3 ];
            text = ''
              exec ${pkgs.bash}/bin/bash ${./scripts/update-pna-package.sh} "$@"
            '';
          };
        in
        {
          update-pna = {
            type = "app";
            program = "${updater}/bin/update-pna";
          };
        });
    };
}
