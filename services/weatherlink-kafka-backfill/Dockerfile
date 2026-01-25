FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install build dependencies for confluent-kafka-go (requires librdkafka)
RUN apk add --no-cache gcc musl-dev pkgconfig librdkafka-dev cyrus-sasl-dev

# Copy service
COPY weatherlink-kafka-backfill /app/weatherlink-kafka-backfill

WORKDIR /app/weatherlink-kafka-backfill

# Download dependencies
RUN go mod download

# Build with CGO enabled (required for confluent-kafka-go)
RUN CGO_ENABLED=1 GOOS=linux go build -tags dynamic -o weatherlink-kafka-backfill .

FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata librdkafka

WORKDIR /root/

COPY --from=builder /app/weatherlink-kafka-backfill/weatherlink-kafka-backfill .

ENTRYPOINT ["./weatherlink-kafka-backfill"]
