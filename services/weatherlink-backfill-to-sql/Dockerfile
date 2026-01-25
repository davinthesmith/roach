FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc musl-dev

# Copy source files (context is ./services/weatherlink-backfill-to-sql)
COPY . /app/weatherlink-backfill-to-sql

WORKDIR /app/weatherlink-backfill-to-sql

# Download dependencies
RUN go mod download

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o weatherlink-backfill-to-sql .

FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

COPY --from=builder /app/weatherlink-backfill-to-sql/weatherlink-backfill-to-sql .

ENTRYPOINT ["./weatherlink-backfill-to-sql"]
