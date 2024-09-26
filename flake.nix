{
  description = "A buffered streaming algorithm to heuristically solve the graph partitioning problem.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, flake-utils, ... }: flake-utils.lib.eachDefaultSystem (system:
    let
      pkgs = import nixpkgs { inherit system; };

      inputs = builtins.attrValues {
        inherit (pkgs) cmake ninja python3 gcc14 tbb_2021_11 sparsehash mpi pkg-config;
        inherit (pkgs.llvmPackages_18) openmp;
      };

      devShellInputs = builtins.attrValues {
        inherit (pkgs) fish ccache mold-wrapped gdb;
      };
    in
    {
      devShells = rec {
        default = gcc;

        gcc = pkgs.mkShell {
          packages = inputs ++ devShellInputs;

          shellHook = ''
            exec fish
          '';
        };

        clang = (pkgs.mkShell.override { stdenv = pkgs.llvmPackages_18.stdenv; }) {
          packages = (pkgs.lib.lists.remove pkgs.gcc14 inputs) ++ devShellInputs;

          shellHook = ''
            exec fish
          '';
        };
      };
    }
  );
}
