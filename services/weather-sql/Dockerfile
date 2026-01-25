# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files and source
COPY go.mod ./
COPY . .

# Generate go.sum and download dependencies
RUN go mod tidy && go mod download

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o weather-sql .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy binary from builder
COPY --from=builder /app/weather-sql .

# Run the service
CMD ["./weather-sql"]
