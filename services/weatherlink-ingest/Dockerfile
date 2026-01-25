# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install build dependencies for confluent-kafka-go (requires librdkafka)
RUN apk add --no-cache gcc musl-dev pkgconfig librdkafka-dev cyrus-sasl-dev

# Copy weatherlink-lib first (shared library)
COPY weatherlink-lib /app/weatherlink-lib

# Copy weatherlink-ingest
COPY weatherlink-ingest /app/weatherlink-ingest

WORKDIR /app/weatherlink-ingest

# Download dependencies
RUN go mod download

# Build the application with CGO enabled (required for confluent-kafka-go)
# Link against system librdkafka instead of vendored to avoid SASL linking issues
RUN CGO_ENABLED=1 GOOS=linux go build -tags dynamic -a -o weather-service .

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata librdkafka

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/weatherlink-ingest/weather-service .

# Run the service
CMD ["./weather-service"]
