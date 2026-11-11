# syntax=docker/dockerfile:1

FROM rust:1-alpine AS chef
RUN apk add --no-cache gcc musl-dev git openssh-client \
    && ssh-keyscan github.com >> /etc/ssh/ssh_known_hosts 2>/dev/null
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN cargo install cargo-chef --locked --version 0.1.78
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG TARGETARCH
RUN case "$TARGETARCH" in \
        amd64) echo x86_64-unknown-linux-musl > /rust-target ;; \
        arm64) echo aarch64-unknown-linux-musl > /rust-target ;; \
        *) echo "unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=ssh --mount=type=secret,id=core_token,required=false \
    if [ -f /run/secrets/core_token ]; then \
        git config --global url."https://x-access-token:$(cat /run/secrets/core_token)@github.com/moshms/core.git".insteadOf "ssh://git@github.com/moshms/core.git"; \
    fi && \
    cargo chef cook --release --target "$(cat /rust-target)" --recipe-path recipe.json
COPY . .
RUN --mount=type=ssh --mount=type=secret,id=core_token,required=false \
    if [ -f /run/secrets/core_token ]; then \
        git config --global url."https://x-access-token:$(cat /run/secrets/core_token)@github.com/moshms/core.git".insteadOf "ssh://git@github.com/moshms/core.git"; \
    fi && \
    cargo build --release --target "$(cat /rust-target)" -p mc-worker \
    && cp "target/$(cat /rust-target)/release/mc-worker" /mc-worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /mc-worker /usr/local/bin/mc-worker
ENV MC_HEALTHCHECK_URL=http://127.0.0.1:8081/readyz
EXPOSE 8081
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=5 CMD ["/usr/local/bin/mc-worker", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/mc-worker"]
