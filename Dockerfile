# Multi-stage build for Spark + YuniKorn Exporter
# Stage 1: Build
FROM golang:1.23-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Set working directory
WORKDIR /build

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY exporter_combined.go ./

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -ldflags="-w -s" -o exporter .

# Stage 2: Runtime
FROM alpine:3.19

# Install ca-certificates for HTTPS connections
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 exporter && \
    adduser -D -u 1000 -G exporter exporter

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/exporter .

# Change ownership
RUN chown -R exporter:exporter /app

# Switch to non-root user
USER exporter

# Expose metrics port
EXPOSE 9300

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:9300/healthz || exit 1

# Default environment variables
ENV PORT=9300
ENV YUNIKORN_SERVICE_URL=http://yunikorn-svc:9080
ENV YUNIKORN_PARTITION=default
ENV SCRAPE_INTERVAL=15s

# Run the exporter
ENTRYPOINT ["./exporter"]
