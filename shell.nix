{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {

  packages = [ pkgs.uv ];


  shellHook = ''
    export LD_LIBRARY_PATH=${pkgs.stdenv.cc.cc.lib}/lib:$LD_LIBRARY_PATH
    zsh
  '';

}
