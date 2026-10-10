# Multi-stage so the runtime image carries no toolchain and no source.
#
# The signing material is never baked in: it is read from a secret source at
# start-up, and the image contains no credential of any kind. That is why this
# file has no ARG and no ENV carrying a secret -- an ARG would be readable from
# the image history and would travel with every layer.

# Declared before any FROM so it is a global argument, which is the only scope
# where it can be substituted into a FROM instruction.
#
# Distroless by default: no shell and no package manager, which is worth having
# in an image whose process holds a hundred thousand connections. Overridable
# only so the build can be verified in environments that cannot reach gcr.io.
ARG RUNTIME_BASE=gcr.io/distroless/static-debian12:nonroot

FROM golang:1.26-alpine AS build
WORKDIR /src

# Module fetch proxy. Overridable because a build in a locked-down environment
# may only be able to reach a specific mirror; proxy.golang.org is not always
# reachable and a build that depends on it inherits that unreliability.
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}

# Dependencies first so a source-only change does not re-copy the module tree.
COPY go.mod go.sum ./

# The module tree is vendored, so the build needs no network at all. Fetching
# from proxy.golang.org at build time would make every build as available as the
# network happens to be, which is not a property worth having.
COPY vendor/ ./vendor/

COPY . .
RUN CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway \
  && CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags="-s -w" -o /out/loadgen ./cmd/loadgen

FROM ${RUNTIME_BASE} AS runtime
WORKDIR /app

COPY --from=build /out/gateway /app/gateway
COPY --from=build /out/loadgen /app/loadgen

# The probe port is separate from data-plane traffic so it can be exposed to the
# orchestrator without exposing either the publish or the admin surface.
EXPOSE 8080 8081 8082

# Numeric rather than the name "nonroot", which only exists in a distroless
# passwd. 65532 is the uid distroless:nonroot uses, and a numeric USER is
# resolved without a passwd lookup, so the image runs unprivileged regardless of
# which base it is built on.
USER 65532:65532
ENTRYPOINT ["/app/gateway"]
