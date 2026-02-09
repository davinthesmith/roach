# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy service
COPY homeassistant-command /app/homeassistant-command

WORKDIR /app/homeassistant-command

# Download dependencies
RUN go mod download

# Build the application (no CGO needed — pure Go dependencies)
RUN CGO_ENABLED=0 GOOS=linux go build -a -o homeassistant-command .

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/homeassistant-command/homeassistant-command .

# Run the service
CMD ["./homeassistant-command"]
