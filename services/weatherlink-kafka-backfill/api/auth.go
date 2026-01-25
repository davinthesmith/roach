package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// generateSignature generates an HMAC-SHA256 signature for the WeatherLink API
func generateSignature(apiSecret string, params map[string]string) string {
	// Sort parameters and create signature string
	var sortedParams []string
	for key, value := range params {
		sortedParams = append(sortedParams, key+value)
	}

	// WeatherLink v2 API uses HMAC-SHA256
	h := hmac.New(sha256.New, []byte(apiSecret))
	paramString := strings.Join(sortedParams, "")
	h.Write([]byte(paramString))
	return hex.EncodeToString(h.Sum(nil))
}
