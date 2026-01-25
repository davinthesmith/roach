package service

import (
	"context"
	"log"
	"sync"
	"time"
)

// RateLimiter implements a token bucket rate limiter with exponential backoff
type RateLimiter struct {
	tokensPerSecond int
	burstSize       int
	hourlyLimit     int
	
	// Token bucket state
	tokens      float64
	lastRefill  time.Time
	mutex       sync.Mutex
	
	// Hourly tracking
	hourlyRequests int
	hourStart      time.Time
	
	// Backoff state
	consecutiveErrors int
}

// NewRateLimiter creates a new rate limiter
// tokensPerSecond: requests per second (default: 8)
// hourlyLimit: requests per hour (default: 1000)
func NewRateLimiter(tokensPerSecond int, hourlyLimit int) *RateLimiter {
	if tokensPerSecond <= 0 {
		tokensPerSecond = 8 // Conservative default
	}
	if hourlyLimit <= 0 {
		hourlyLimit = 1000
	}
	
	burstSize := tokensPerSecond * 2 // Allow short bursts
	
	return &RateLimiter{
		tokensPerSecond:   tokensPerSecond,
		burstSize:         burstSize,
		hourlyLimit:       hourlyLimit,
		tokens:            float64(burstSize), // Start with full bucket
		lastRefill:        time.Now(),
		hourlyRequests:    0,
		hourStart:         time.Now(),
		consecutiveErrors: 0,
	}
}

// Wait blocks until a token is available
func (r *RateLimiter) Wait(ctx context.Context) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	// Reset hourly counter if hour has passed
	if time.Since(r.hourStart) >= time.Hour {
		log.Printf("Hourly rate limit reset: %d requests in last hour", r.hourlyRequests)
		r.hourlyRequests = 0
		r.hourStart = time.Now()
	}
	
	// Check hourly limit (leave 10% buffer)
	if r.hourlyRequests >= int(float64(r.hourlyLimit)*0.9) {
		sleepTime := time.Until(r.hourStart.Add(time.Hour))
		log.Printf("Approaching hourly limit (%d/%d), sleeping for %s", 
			r.hourlyRequests, r.hourlyLimit, sleepTime)
		
		r.mutex.Unlock()
		select {
		case <-time.After(sleepTime):
		case <-ctx.Done():
			return ctx.Err()
		}
		r.mutex.Lock()
	}
	
	// Refill tokens based on time elapsed
	now := time.Now()
	elapsed := now.Sub(r.lastRefill)
	tokensToAdd := float64(r.tokensPerSecond) * elapsed.Seconds()
	r.tokens = minFloat64(r.tokens+tokensToAdd, float64(r.burstSize))
	r.lastRefill = now
	
	// Wait until we have at least 1 token
	for r.tokens < 1.0 {
		// Calculate how long until we have a token
		timeToWait := time.Duration(float64(time.Second) / float64(r.tokensPerSecond))
		
		r.mutex.Unlock()
		select {
		case <-time.After(timeToWait):
		case <-ctx.Done():
			return ctx.Err()
		}
		r.mutex.Lock()
		
		// Refill again after waiting
		now = time.Now()
		elapsed = now.Sub(r.lastRefill)
		tokensToAdd = float64(r.tokensPerSecond) * elapsed.Seconds()
		r.tokens = minFloat64(r.tokens+tokensToAdd, float64(r.burstSize))
		r.lastRefill = now
	}
	
	// Consume one token
	r.tokens -= 1.0
	
	return nil
}

// RecordRequest records a successful request
func (r *RateLimiter) RecordRequest() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	r.hourlyRequests++
	r.consecutiveErrors = 0 // Reset error counter on success
	
	// Log progress every 100 requests
	if r.hourlyRequests%100 == 0 {
		log.Printf("Rate limit status: %d/%d requests this hour", r.hourlyRequests, r.hourlyLimit)
	}
}

// RecordError records an error response and returns backoff duration
// statusCode: HTTP status code (429 = rate limit error)
// Returns: duration to wait before retrying
func (r *RateLimiter) RecordError(statusCode int) time.Duration {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	if statusCode == 429 {
		// Rate limit error - exponential backoff
		r.consecutiveErrors++
		
		// Exponential backoff: 1s, 2s, 4s, 8s, capped at 8s
		backoff := time.Second * time.Duration(1<<min(r.consecutiveErrors-1, 3))
		
		log.Printf("Rate limit error (429), backing off for %s (error #%d)", 
			backoff, r.consecutiveErrors)
		
		return backoff
	}
	
	// Other errors - linear backoff
	r.consecutiveErrors++
	backoff := time.Second * time.Duration(min(r.consecutiveErrors, 5))
	
	log.Printf("API error (%d), backing off for %s (error #%d)", 
		statusCode, backoff, r.consecutiveErrors)
	
	return backoff
}

// GetStats returns current rate limiter statistics
func (r *RateLimiter) GetStats() (hourlyRequests int, tokensAvailable float64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	return r.hourlyRequests, r.tokens
}

// minFloat64 returns the minimum of two float64 values
func minFloat64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// min returns the minimum of two values
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
