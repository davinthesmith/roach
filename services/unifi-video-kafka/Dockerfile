# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install build dependencies for confluent-kafka-go (requires librdkafka)
RUN apk add --no-cache gcc musl-dev pkgconfig librdkafka-dev cyrus-sasl-dev

# Copy service
COPY unifi-video-kafka /app/unifi-video-kafka

WORKDIR /app/unifi-video-kafka

# Download dependencies
RUN go mod download

# Build the application with CGO enabled (required for confluent-kafka-go)
# Link against system librdkafka instead of vendored to avoid SASL linking issues
RUN CGO_ENABLED=1 GOOS=linux go build -tags dynamic -a -o unifi-video-kafka .

# Runtime stage
FROM alpine:latest

# Install runtime dependencies (ffmpeg for RTSPS stream capture)
RUN apk --no-cache add ca-certificates tzdata librdkafka ffmpeg

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/unifi-video-kafka/unifi-video-kafka .

# Run the service
CMD ["./unifi-video-kafka"]
