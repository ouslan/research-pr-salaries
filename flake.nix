{
  description = "PR Salaries flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs = { self, nixpkgs }: 
  let
    pkgs = nixpkgs.legacyPackages."x86_64-linux";
  in
  {
    devShells."x86_64-linux".default = pkgs.mkShell {

      packages = [
        pkgs.python3
        pkgs.python3Packages.pandas
        pkgs.python3Packages.pygam
      ];

      shellHook = ''
        zsh
      '';
    };
  };
}
