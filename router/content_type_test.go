package router

import (
	"testing"

	"github.com/Rene-Roscher/wings/server/backup"
)

func TestIsValidBackupContentType(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		expected    bool
	}{
		// GZIP formats
		{"GZIP x-gzip", "application/x-gzip", true},
		{"GZIP gzip", "application/gzip", true},
		{"GZIP x-compressed", "application/x-compressed", true},
		{"GZIP x-gtar", "application/x-gtar", true},
		
		// ZSTD formats
		{"ZSTD x-zstd", "application/x-zstd", true},
		{"ZSTD zstd", "application/zstd", true},
		{"ZSTD x-zstandard", "application/x-zstandard", true},
		
		// TAR formats
		{"TAR x-tar", "application/x-tar", true},
		{"TAR tar", "application/tar", true},
		
		// Generic formats
		{"Octet stream", "application/octet-stream", true},
		{"Binary octet", "binary/octet-stream", true},
		
		// Backup specific
		{"Compressed tar", "application/x-compressed-tar", true},
		{"TGZ", "application/x-tgz", true},
		
		// Case insensitive
		{"Uppercase GZIP", "APPLICATION/X-GZIP", true},
		{"Mixed case", "Application/Octet-Stream", true},
		
		// With charset parameters
		{"GZIP with charset", "application/x-gzip; charset=binary", true},
		{"Octet with boundary", "application/octet-stream; boundary=something", true},
		
		// Invalid types
		{"Plain text", "text/plain", false},
		{"HTML", "text/html", false},
		{"JSON", "application/json", false},
		{"Image", "image/png", false},
		{"Unknown", "application/unknown", false},
		{"Empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := backup.IsValidBackupContentType(tt.contentType)
			if result != tt.expected {
				t.Errorf("backup.IsValidBackupContentType(%q) = %v, expected %v",
					tt.contentType, result, tt.expected)
			}
		})
	}
}