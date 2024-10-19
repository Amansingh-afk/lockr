{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.go
  ];

  shellHook = ''
  export GOPATH=$HOME/go
  export PATH=$PATH:$GOPATH/bin
  echo "Welcome to the Lockr!"
  echo "Commands:"
  echo "  run: go run cmd/main.go"
  echo "  test: go test ./tests/..."
  echo "  coverage: go test ./tests/... -coverprofile=coverage.out && go tool cover -html=coverage.out"
'';
}