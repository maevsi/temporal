# syntax=docker/dockerfile:1

########################
# Create base.

FROM golang:1.26.6-alpine AS base

RUN apk add --no-cache ca-certificates

ARG GROUP_ID=1000
ARG USER_ID=1000
ARG USER_NAME=go

RUN addgroup -g $GROUP_ID -S $USER_NAME \
    && adduser -u $USER_ID -S -G $USER_NAME -h /home/$USER_NAME $USER_NAME

WORKDIR /srv/app/


########################
# Serve development.

FROM base AS development

ENV CGO_ENABLED=0

RUN mkdir -p /home/$USER_NAME/go/pkg/mod \
    && chown -R $USER_NAME:$USER_NAME /home/$USER_NAME /srv/app

VOLUME /home/$USER_NAME/go/pkg/mod
VOLUME /srv/app

USER $USER_NAME
CMD ["go", "run", "./cmd/worker"]
EXPOSE 9090
HEALTHCHECK --start-period=30s --timeout=5s CMD ["go", "run", "./cmd/worker", "-healthcheck"]


########################
# Prepare environment.

FROM base AS prepare

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY ./ ./


########################
# Lint code.

FROM golangci/golangci-lint:v2.12.2-alpine AS lint

WORKDIR /srv/app/
COPY --from=prepare /srv/app/ ./
RUN golangci-lint run ./...


########################
# Test code.

FROM prepare AS test

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go test -count=1 ./...


########################
# Build for production.

FROM prepare AS build

ENV CGO_ENABLED=0

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -trimpath -ldflags="-s -w" -o /srv/app/bin/worker ./cmd/worker


########################
# Collect results.

FROM base AS collect

RUN chown $USER_NAME:$USER_NAME .

COPY --from=build --chown=$USER_NAME /srv/app/bin/worker ./worker
COPY --from=lint /srv/app/go.mod /dev/null
COPY --from=test /srv/app/go.mod /dev/null


########################
# Serve production.
#
# The final image is FROM scratch: a statically linked Go binary needs nothing else at runtime, which keeps the attack surface and image size minimal.
# There is deliberately no shell, so HEALTHCHECK execs the binary itself with -healthcheck instead of curl/wget (see cmd/worker/main.go).

FROM scratch AS production

COPY --from=collect /etc/passwd /etc/passwd
COPY --from=collect /etc/group /etc/group
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=collect --chown=go /srv/app/worker /srv/app/worker

WORKDIR /srv/app/
USER go
ENTRYPOINT ["/srv/app/worker"]
EXPOSE 9090
HEALTHCHECK --interval=30s --timeout=5s CMD ["/srv/app/worker", "-healthcheck"]
LABEL org.opencontainers.image.source="https://github.com/maevsi/temporal"
LABEL org.opencontainers.image.description="Temporal worker (Go) running jobs for the Vibetype platform."
