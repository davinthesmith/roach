FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install build dependencies for confluent-kafka-go (requires librdkafka)
RUN apk add --no-cache gcc musl-dev pkgconfig librdkafka-dev cyrus-sasl-dev

# Copy weatherlink-lib first (shared library)
COPY weatherlink-lib /app/weatherlink-lib

# Copy weatherlink-backfill
COPY weatherlink-backfill /app/weatherlink-backfill

WORKDIR /app/weatherlink-backfill

# Download dependencies
RUN go mod download

# Build with CGO enabled (required for confluent-kafka-go)
RUN CGO_ENABLED=1 GOOS=linux go build -tags dynamic -o weatherlink-backfill .

FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata librdkafka

WORKDIR /root/

COPY --from=builder /app/weatherlink-backfill/weatherlink-backfill .

ENTRYPOINT ["./weatherlink-backfill"]
