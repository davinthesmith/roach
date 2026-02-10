# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY unifi-video-jpg /app/unifi-video-jpg

WORKDIR /app/unifi-video-jpg

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -o unifi-video-jpg .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata ffmpeg

WORKDIR /root/

COPY --from=builder /app/unifi-video-jpg/unifi-video-jpg .

CMD ["./unifi-video-jpg"]
