# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY ubiquiti-video-jpg /app/ubiquiti-video-jpg

WORKDIR /app/ubiquiti-video-jpg

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -o ubiquiti-video-jpg .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata ffmpeg

WORKDIR /root/

COPY --from=builder /app/ubiquiti-video-jpg/ubiquiti-video-jpg .

CMD ["./ubiquiti-video-jpg"]
