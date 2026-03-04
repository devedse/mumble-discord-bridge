# Build stage - Debian for glibc compatibility with prebuilt libdave
FROM golang:1.26 AS builder

WORKDIR /go/src/app
COPY . .

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git make libopus-dev gcc curl unzip pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install libdave (Discord DAVE E2EE protocol) - uses prebuilt glibc binaries
RUN NON_INTERACTIVE=1 bash scripts/libdave_install.sh v1.1.0

# Build with version info
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN export PKG_CONFIG_PATH="$HOME/.local/lib/pkgconfig:$PKG_CONFIG_PATH" && \
    CGO_ENABLED=1 go build \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
    -o /mumble-discord-bridge \
    ./cmd/mumble-discord-bridge

# Generate licenses
RUN go install github.com/google/go-licenses@latest && \
    go-licenses save ./cmd/mumble-discord-bridge --force --save_path="/LICENSES"

# Runtime stage - Debian trixie to match builder's glibc version
FROM debian:trixie-slim

WORKDIR /opt/
RUN apt-get update && apt-get install -y --no-install-recommends \
    libopus0 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /root/.local/lib/libdave.so /usr/lib/
COPY --from=builder /LICENSES ./LICENSES
COPY --from=builder /mumble-discord-bridge .

CMD ["/opt/mumble-discord-bridge"]
