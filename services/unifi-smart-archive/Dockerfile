# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY unifi-smart-archive /app/unifi-smart-archive

WORKDIR /app/unifi-smart-archive

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -o unifi-smart-archive .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

COPY --from=builder /app/unifi-smart-archive/unifi-smart-archive .

CMD ["./unifi-smart-archive"]
