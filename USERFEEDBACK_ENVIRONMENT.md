# Reproducing the task environment

- Operating system: Ubuntu 26.04 LTS on WSL2 (Linux amd64).
- Go: 1.24.9, matching the toolchain directive in go.mod.
- GCC: 15.2.0; CGO_ENABLED=1 for race detection.
- Harness: Codex CLI 0.144.5; provider model auto_model/urm; reasoning high; context window 1000000.
- Module versions are pinned in go.mod and go.sum. No dependency upgrades are part of this task.
- Obtain Go from https://go.dev/dl/go1.24.9.linux-amd64.tar.gz and verify its checksum against the Go download metadata.

## Validation

```sh
go version
go mod download
go test -count=1 -timeout=90s ./...
go test -race -count=1 -timeout=90s ./...
```

The tests use local httptest servers and need no third-party service. Dependency downloads are a preparation step. Model credentials and client configuration stay outside the repository. No external skills or plugins are supplied to the task session.
