# temporal

A Go Temporal worker for maevsi's scheduled operations: database backups and outbox purges.
This repo only implements the worker side; the shared self-hosted Temporal Server infrastructure (Temporal Server/UI containers, Prometheus scrape config, Grafana alerting, Docker Swarm secrets) is implemented separately in the `stack` repo and is **not** part of this repo.

## Workflows

Two Temporal Workflows, each with one Activity doing the real work:

| Workflow              | Activity       | Schedule ID              | Cadence |
| --------------------- | -------------- | ------------------------- | ------- |
| `DBBackupWorkflow`    | `DBBackup`     | `dbbackup-daily`          | every 24h |
| `OutboxPurgeWorkflow` | `OutboxPurge`  | `outbox-purge-every-2h`   | every 2h  |

Both workflows call a shared `SentryCheckIn` activity at start (`in_progress`) and completion (`ok` / `error`).
`OutboxPurge` runs directly against Postgres over `pgx`.

## Architecture

```
cmd/worker/main.go       entrypoint: wires config -> clients -> worker, runs until SIGINT/SIGTERM
internal/config          env-var configuration, one struct per concern, fails listing every missing var at once
internal/activities      DBBackup, OutboxPurge, SentryCheckIn: the actual work, interface-mocked for tests
internal/workflows       DBBackupWorkflow, OutboxPurgeWorkflow: orchestration only, no AWS/pgx/HTTP imports
internal/schedule        creates the two Temporal Schedules on startup (idempotent, native Schedule API)
internal/metrics         Prometheus HTTP handler fed by the Temporal Go SDK's native metrics
internal/postgres        pgxpool wiring + adapter to activities.DBExecutor
internal/s3client        AWS S3 client construction from this repo's own config
internal/sentrycrons     minimal Sentry Crons "ping" check-in client
```

Workflows never import `internal/postgres`, `internal/s3client`, or the AWS/pgx SDKs.
Instead they reference activities purely through the `var a *activities.Activities; workflow.ExecuteActivity(ctx, a.DBBackup, ...)` method-expression idiom documented by the Temporal Go SDK.
That keeps activity implementations free to depend on real infrastructure clients while workflow code stays deterministic and easy to unit test with mocked activities.

## Configuration

Everything is read from environment variables.
There is no `/run/secrets/*` file-reading convention baked into this repo; that is a production deployment detail, not something the worker binary should know about.
Whatever supplies the environment (a Docker Swarm `secrets:` mapping translated to env vars by the container runtime, a `.env` file in local dev, systemd's `EnvironmentFile=`) is responsible for turning maevsi's `vibetype_role_service_*`-style Postgres credentials, S3 credentials, and Sentry Crons URLs into the variables below.

The one exception is the outbox table the purge runs against, `vibetype_private.outbox`, which is wired in `cmd/worker/main.go`.
It is a property of the schema this worker is written for rather than of a deployment, so it is not worth an environment variable.

### Temporal

| Variable               | Default                    | Notes                                             |
| ----------------------- | --------------------------- | -------------------------------------------------- |
| `TEMPORAL_HOST_PORT`    | `temporal-server:7233`      | Self-hosted Temporal Server frontend address.      |
| `TEMPORAL_NAMESPACE`    | `default`                   |                                                      |
| `TEMPORAL_TASK_QUEUE`   | `maevsi-jobs`                |                                                      |

### Postgres

| Variable            | Default     | Notes                                                                 |
| -------------------- | ----------- | ----------------------------------------------------------------------|
| `POSTGRES_HOST`      | `localhost` |                                                                        |
| `POSTGRES_PORT`      | `5432`      |                                                                        |
| `POSTGRES_DATABASE`  | *required*  |                                                                        |
| `POSTGRES_USER`      | *required*  | Should be a dedicated `vibetype_role_service_*` role, per stack convention. |
| `POSTGRES_PASSWORD`  | *required*  |                                                                        |
| `POSTGRES_SSLMODE`   | `require`   | Passed straight through to pgx (`disable`, `require`, `verify-full`, ...). |

### S3 (DBBackup)

| Variable               | Default   | Notes                                                          |
| ----------------------- | --------- | ---------------------------------------------------------------|
| `S3_BUCKET`             | *required* |                                                                 |
| `S3_PREFIX`             | `backups` | Object key prefix.                                             |
| `S3_REGION`             | *required* |                                                                 |
| `S3_ACCESS_KEY_ID`      | *required* |                                                                 |
| `S3_SECRET_ACCESS_KEY`  | *required* |                                                                 |
| `S3_ENDPOINT`           | *(unset)* | Override for S3-compatible storage; leave unset for AWS S3.    |
| `S3_USE_PATH_STYLE`     | `false`   | Typically required by non-AWS S3-compatible endpoints.         |
| `BACKUP_SOURCE_DIR`     | `/backups` | Local directory synced to S3.                                  |

The credentials need `s3:PutObject` plus `s3:ListBucket` on the bucket.
`s3:ListBucket` is what makes S3 answer `HeadObject` for a key that does not exist with a 404 instead of a 403; without it the worker cannot tell "object missing" apart from "access denied", and rather than re-uploading everything on every run it fails the sync so the permission problem is visible.

### Sentry Crons

| Variable                    | Default   | Notes                                                                   |
| ----------------------------- | --------- | -------------------------------------------------------------------------|
| `SENTRY_CRONS`                | *(unset)* | DBBackup check-in URL.                                                   |
| `SENTRY_CRONS_OUTBOX_PURGE`   | *(unset)* | OutboxPurge check-in URL.                                                |

Both accept a Sentry Crons monitor "ping" URL (`https://sentry.io/api/0/cron/<monitor-slug>/<client-key>/`); the worker appends `?status=in_progress|ok|error`.
Leaving either unset makes check-ins for that job a no-op rather than an error.
Setting one to something unusable (no scheme, a scheme other than http/https, no host) fails startup instead, since a monitor that silently never checks in is indistinguishable from one that was never configured.

### Schedule cadences

| Variable                       | Default | Notes                                    |
| -------------------------------- | ------- | ------------------------------------------|
| `DBBACKUP_SCHEDULE_EVERY`        | `24h`   | Go duration syntax (`24h`, `30m`, ...).   |
| `OUTBOX_PURGE_SCHEDULE_EVERY`    | `2h`    |                                            |
| `OUTBOX_PURGE_RETENTION`         | `24h`   | Rows older than this duration are deleted. |

### Metrics

| Variable        | Default    | Notes                                    |
| ----------------- | ---------- | ------------------------------------------|
| `METRICS_ADDR`     | `:9090`    | Listen address for the Prometheus HTTP server. |
| `METRICS_PATH`     | `/metrics` | Anything but `/healthz`, which the worker serves itself. |

## Metrics

The worker exposes the Temporal Go SDK's native metrics (activity/workflow execution counts and latencies, task queue backlog, poller counts, etc.) on a Prometheus HTTP handler at `METRICS_ADDR` `METRICS_PATH` (`:9090/metrics` by default), via `go.temporal.io/sdk/contrib/tally` backed by `github.com/uber-go/tally/v4/prometheus`.
Pointing a Prometheus scrape target at this port is all that is needed on the infrastructure side; wiring that scrape target and any Grafana dashboards/alerts is `stack` repo work happening separately.

There is also a plain `/healthz` endpoint (200 OK if the HTTP server is up) used by the container `HEALTHCHECK`; see [Docker image](#docker-image).

## Sentry Crons alerting

Each workflow calls the `SentryCheckIn` activity twice: once with `in_progress` right after the workflow starts, then once more with `ok` or `error` depending on whether the underlying activity (S3 sync / DELETE) succeeded.
A Sentry Crons outage never fails the workflow itself: check-in failures are logged (`workflow.GetLogger(ctx).Warn(...)`) but not propagated, since alerting must not become a reason the actual backup or purge is marked failed.

## Retry policy

Both activities get a `temporal.RetryPolicy` tuned to their shape rather than left at SDK defaults:

- **DBBackup**: `StartToCloseTimeout: 30m`, `HeartbeatTimeout: 2m`, 3 attempts, exponential backoff from 30s up to a 5-minute cap.
  The activity heartbeats once per file and again on every completed upload part, so a dead worker is detected long before the 30-minute timeout while a single large upload still cannot outlast the heartbeat timeout on its own.
- **OutboxPurge**: `StartToCloseTimeout: 2m`, 5 attempts, exponential backoff from 5s up to a 1-minute cap. Short because it's a single `DELETE`, more attempts because transient Postgres connectivity issues are the expected failure mode.
- **SentryCheckIn**: `StartToCloseTimeout: 15s`, 3 attempts. Cheap and fast to retry, but never allowed to block the workflow for long.

Both DBBackup and OutboxPurge treat a configuration problem (missing bucket/schema/table, unreachable source directory) as a non-retryable `ConfigError` `ApplicationError` (`activities.ErrTypeConfig`), since retrying a bad config can't help.
See `RetryPolicy.NonRetryableErrorTypes` in `internal/workflows`.

## Schedules

`internal/schedule.EnsureAll` runs once at worker startup and creates both Temporal Schedules if they don't already exist (`ScheduleClient.Describe` then `Create`, never `Update`).
That's safe to run on every deploy without clobbering a cadence an operator has since changed via `temporal schedule` or the Temporal UI.
Cadences are expressed as `client.ScheduleIntervalSpec` (a native Temporal Schedule concept), not as cron strings interpreted by workflow code, and use `SCHEDULE_OVERLAP_POLICY_SKIP` so a slow run never stacks up concurrent executions.

## Development

Requires Go (see `go.mod` for the minimum version) and, for integration testing, a local Temporal Server (e.g. `temporal server start-dev`) and Postgres instance.

```sh
go build ./...
go vet ./...
go test -race ./...
gofmt -l .   # should print nothing
```

Run the worker locally against a `temporal server start-dev` instance and a local Postgres:

```sh
export POSTGRES_HOST=localhost POSTGRES_DATABASE=vibetype \
       POSTGRES_USER=vibetype_role_service_temporal_worker POSTGRES_PASSWORD=... POSTGRES_SSLMODE=disable
export S3_BUCKET=... S3_REGION=... S3_ACCESS_KEY_ID=... S3_SECRET_ACCESS_KEY=...
export TEMPORAL_HOST_PORT=127.0.0.1:7233
go run ./cmd/worker
```

## Tests

`internal/activities`, `internal/workflows`, `internal/schedule`, `internal/sentrycrons`, `internal/config`, and `internal/metrics` all have unit tests:

- **Activities** are tested via Temporal's `testsuite.TestActivityEnvironment` against hand-rolled fakes of `activities.S3API` (no real AWS) and `activities.DBExecutor` (no real Postgres), covering the upload/skip diffing logic, S3 key building, the retention-interval formatting, and non-retryable config-error paths.
  The S3 fake implements the multipart methods as well as `PutObject`, so the path a real dump file takes is covered rather than assumed, including that progress is reported mid-transfer to keep the activity heartbeating.
- **Workflows** are tested via `testsuite.TestWorkflowEnvironment` with mocked activities, asserting the exact `in_progress` -> `ok`/`error` check-in sequence and that a Sentry Crons failure never fails the workflow.
- **Schedules** are tested as pure functions returning `client.ScheduleOptions`, asserting they use `Intervals` (not `CronExpressions`).

## Docker image

The `Dockerfile` builds a multi-stage image: `base` -> `development` / `prepare` -> `lint` / `test` -> `build` -> `collect` -> `production`, following this org's general container conventions (non-root user with matching `GROUP_ID`/`USER_ID` build args, `WORKDIR /srv/app/`, a `HEALTHCHECK`, an `org.opencontainers.image.description` label).

Unlike this org's other services, the `production` stage is `FROM scratch`: a statically linked (`CGO_ENABLED=0`) Go binary needs nothing else at runtime, so the final image is just the binary, CA certificates (for TLS to Temporal/Postgres/S3), and `/etc/passwd`/`/etc/group` entries for the non-root user, about 44 MB total.
Since `scratch` has no shell, `HEALTHCHECK` execs the binary itself with a `-healthcheck` flag, which loads the same config the running worker would and does a plain HTTP GET against its own `/healthz`.

```sh
docker build --target test .        # go test with the race detector
docker build --target lint .        # golangci-lint
docker build --target production -t temporal .
```

## Known simplifications

- **DBBackup's diffing** compares local file size against S3's `HeadObject` `ContentLength` per file, rather than the AWS CLI's default size+mtime heuristic.
  Dump files get a fresh mtime on every regeneration regardless of whether their content changed, so an mtime check would force a re-upload on every single run and defeat the point of diffing entirely.
  Size-only is close enough for backup files that are either new or fully rewritten, but not byte-for-byte equivalent to `aws s3 sync`.
- **DBBackup uploads through `aws-sdk-go-v2/feature/s3/transfermanager`**, which transparently switches to a multipart upload above its `MultipartUploadThreshold`.
  That default is 16 MiB, not S3's 5 GiB single-`PutObject` limit, so in practice every real dump file is uploaded as 8 MiB parts.
- **Schedules are bootstrapped once and never updated** by this worker.
  Changing a cadence via `DBBACKUP_SCHEDULE_EVERY`/`OUTBOX_PURGE_SCHEDULE_EVERY` after the Schedule already exists requires deleting it first (`temporal schedule delete`) or updating it out-of-band; this is a deliberate choice so an operator's manual Schedule edits are never silently overwritten on redeploy.

## License

[Apache-2.0](LICENSE).
