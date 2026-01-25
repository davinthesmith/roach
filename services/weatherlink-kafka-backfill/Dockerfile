FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc musl-dev

# Copy source files (context is ./services/weatherlink-kafka-backfill)
COPY . /app/weatherlink-kafka-backfill

WORKDIR /app/weatherlink-kafka-backfill

# Download dependencies
RUN go mod download

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o weatherlink-kafka-backfill .

FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

COPY --from=builder /app/weatherlink-kafka-backfill/weatherlink-kafka-backfill .

ENTRYPOINT ["./weatherlink-kafka-backfill"]
