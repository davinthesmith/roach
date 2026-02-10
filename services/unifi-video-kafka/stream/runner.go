package stream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"time"

	"unifi-video-kafka/kafka"
)

// MJPEG markers
var (
	jpegSOI = []byte{0xFF, 0xD8} // Start Of Image
	jpegEOI = []byte{0xFF, 0xD9} // End Of Image
)

// Runner captures frames from an RTSPS stream via ffmpeg and publishes them to Kafka.
type Runner struct {
	cameraID   string
	cameraName string
	topic      string
	producer   *kafka.Producer
	logLevel   string
}

// NewRunner creates a new stream runner for a single camera.
func NewRunner(cameraID, cameraName, topic string, producer *kafka.Producer, logLevel string) *Runner {
	return &Runner{
		cameraID:   cameraID,
		cameraName: cameraName,
		topic:      topic,
		producer:   producer,
		logLevel:   logLevel,
	}
}

// Run starts ffmpeg for the given RTSPS URL, reads MJPEG frames from stdout,
// and publishes each frame to Kafka. Blocks until ffmpeg exits or context is cancelled.
func (r *Runner) Run(ctx context.Context, rtspsURL string) error {
	// Build ffmpeg command:
	//   -tls_verify 0        — skip TLS cert verification (NVR uses self-signed certs)
	//   -rtsp_transport tcp  — use TCP for RTSPS (more reliable than UDP)
	//   -i <url>             — input RTSPS stream
	//   -vf fps=1,scale=...  — 1 frame/sec, scale to 1280px wide (keep aspect ratio)
	//   -f image2pipe        — output frames as a continuous stream to stdout
	//   -vcodec mjpeg        — encode output frames as MJPEG (JPEG)
	//   -q:v 8               — JPEG quality (2=best, 31=worst; 8 = good quality, reasonable size)
	//   pipe:1               — write to stdout
	args := []string{
		"-tls_verify", "0",
		"-rtsp_transport", "tcp",
		"-i", rtspsURL,
		"-vf", "fps=1,scale=1280:-1",
		"-f", "image2pipe",
		"-vcodec", "mjpeg",
		"-q:v", "8",
		"-loglevel", "warning",
		"pipe:1",
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("create stdout pipe: %w", err)
	}

	// Capture stderr for diagnostics
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start ffmpeg: %w", err)
	}

	log.Printf("[%s] ffmpeg started (pid=%d)", r.cameraName, cmd.Process.Pid)

	// Read frames from stdout and publish
	frameCount, err := r.readFrames(ctx, stdout)

	// Wait for ffmpeg to exit
	cmdErr := cmd.Wait()

	if ctx.Err() != nil {
		log.Printf("[%s] Stopped (context cancelled, %d frames published)", r.cameraName, frameCount)
		return ctx.Err()
	}

	stderr := stderrBuf.String()
	if stderr != "" {
		log.Printf("[%s] ffmpeg stderr: %s", r.cameraName, stderr)
	}

	if err != nil {
		return fmt.Errorf("read frames: %w (ffmpeg exit: %v, stderr: %s)", err, cmdErr, stderr)
	}
	if cmdErr != nil {
		return fmt.Errorf("ffmpeg exited: %w (stderr: %s)", cmdErr, stderr)
	}

	return fmt.Errorf("ffmpeg exited cleanly after %d frames", frameCount)
}

// readFrames reads MJPEG frames from the ffmpeg stdout pipe by scanning for
// SOI (0xFFD8) and EOI (0xFFD9) markers.
func (r *Runner) readFrames(ctx context.Context, reader io.Reader) (int, error) {
	// Read in chunks; accumulate into a buffer and extract complete frames
	buf := make([]byte, 0, 512*1024) // 512KB initial capacity
	chunk := make([]byte, 64*1024)   // 64KB read chunks
	frameCount := 0

	for {
		if ctx.Err() != nil {
			return frameCount, ctx.Err()
		}

		n, err := reader.Read(chunk)
		if n > 0 {
			buf = append(buf, chunk[:n]...)

			// Extract all complete frames from the buffer
			for {
				frame, remaining, found := extractFrame(buf)
				if !found {
					break
				}
				buf = remaining

				ts := time.Now().Unix()
				if pubErr := r.publishFrame(ctx, frame, ts); pubErr != nil {
					log.Printf("[%s] Publish error: %v", r.cameraName, pubErr)
				} else {
					frameCount++
					if r.logLevel == "debug" {
						log.Printf("[%s] Frame %d published (%d bytes, ts=%d)",
							r.cameraName, frameCount, len(frame), ts)
					} else if frameCount%60 == 0 {
						log.Printf("[%s] %d frames published", r.cameraName, frameCount)
					}
				}
			}

			// Prevent unbounded buffer growth — if buffer is large and has no SOI,
			// discard everything before the last potential marker start
			if len(buf) > 2*1024*1024 {
				if idx := bytes.LastIndex(buf, jpegSOI); idx > 0 {
					buf = buf[idx:]
				} else {
					buf = buf[:0]
				}
			}
		}

		if err != nil {
			if err == io.EOF {
				return frameCount, nil
			}
			return frameCount, fmt.Errorf("read stdout: %w", err)
		}
	}
}

// extractFrame finds the first complete JPEG frame (SOI...EOI) in data.
// Returns the frame bytes, the remaining data after the frame, and whether a frame was found.
func extractFrame(data []byte) (frame []byte, remaining []byte, found bool) {
	soiIdx := bytes.Index(data, jpegSOI)
	if soiIdx < 0 {
		return nil, data, false
	}

	// Search for EOI after the SOI
	eoiIdx := bytes.Index(data[soiIdx+2:], jpegEOI)
	if eoiIdx < 0 {
		return nil, data, false
	}

	// eoiIdx is relative to soiIdx+2, so absolute EOI end = soiIdx + 2 + eoiIdx + 2
	frameEnd := soiIdx + 2 + eoiIdx + 2
	frame = make([]byte, frameEnd-soiIdx)
	copy(frame, data[soiIdx:frameEnd])

	return frame, data[frameEnd:], true
}

// publishFrame sends a single JPEG frame to Kafka.
func (r *Runner) publishFrame(ctx context.Context, frame []byte, timestamp int64) error {
	key := fmt.Sprintf("%s:%d", r.cameraID, timestamp)

	headers := map[string]string{
		"schema_version": "1",
		"camera_id":      r.cameraID,
		"camera_name":    r.cameraName,
		"timestamp":      fmt.Sprintf("%d", timestamp),
		"source":         "unifi-protect-video",
		"content_type":   "image/jpeg",
	}

	return r.producer.PublishFrame(ctx, r.topic, key, frame, headers)
}
