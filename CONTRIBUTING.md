# Contributing

## Prerequisites

- Go (see `go.mod` for the version)
- Git
- [golangci-lint](https://golangci-lint.run/) v2, for local linting matching CI (`docker build --target lint .` runs the same check without a local install)

See the [README's Development section](README.md#development) for running the worker locally, and its [Tests section](README.md#tests) for the test suite.

## Local checks

```sh
go build ./...
go test -race ./...
golangci-lint run ./...
```

CI (`.github/workflows/ci.yaml`) runs the same three checks via the Docker `lint`, `test`, and `build` stages, gating the image build on all three passing. It enforces zero lint issues.

## Commits

Commits and PR titles must follow [Conventional Commits](https://www.conventionalcommits.org/) (enforced by the Semantic Pull Requests check, see `.github/semantic.yml`); releases and `CHANGELOG.md` entries are generated from them automatically.
