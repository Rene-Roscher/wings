package backup

import (
	"strings"
)

// CompressionFormat represents a backup compression format
type CompressionFormat string

const (
	CompressionGzip CompressionFormat = "gzip"
	CompressionZstd CompressionFormat = "zstd"
	CompressionTar  CompressionFormat = "tar"
	CompressionNone CompressionFormat = "none"
)

// CompressionAdapter defines the interface for compression formats
// This provides extensibility for adding new compression formats in the future
type CompressionAdapter interface {
	// Format returns the compression format identifier
	Format() CompressionFormat
	
	// Extension returns the file extension for this format
	Extension() string
	
	// ContentTypes returns the list of valid MIME types for this format
	ContentTypes() []string
	
	// IsSupported checks if the format is supported for backup operations
	IsSupported() bool
	
	// Description returns a human-readable description of the format
	Description() string
}

// gzipAdapter implements CompressionAdapter for GZIP format
type gzipAdapter struct{}

func (g *gzipAdapter) Format() CompressionFormat { return CompressionGzip }
func (g *gzipAdapter) Extension() string         { return ".gz" }
func (g *gzipAdapter) ContentTypes() []string {
	return []string{
		"application/x-gzip",
		"application/gzip",
		"application/x-compressed",
		"application/x-gtar",
		"application/x-compressed-tar",
		"application/x-tgz",
	}
}
func (g *gzipAdapter) IsSupported() bool   { return true }
func (g *gzipAdapter) Description() string { return "GZIP compression" }

// zstdAdapter implements CompressionAdapter for ZSTD format
type zstdAdapter struct{}

func (z *zstdAdapter) Format() CompressionFormat { return CompressionZstd }
func (z *zstdAdapter) Extension() string         { return ".zst" }
func (z *zstdAdapter) ContentTypes() []string {
	return []string{
		"application/x-zstd",
		"application/zstd",
		"application/x-zstandard",
	}
}
func (z *zstdAdapter) IsSupported() bool   { return true }
func (z *zstdAdapter) Description() string { return "ZSTD compression (high performance)" }

// tarAdapter implements CompressionAdapter for TAR format
type tarAdapter struct{}

func (t *tarAdapter) Format() CompressionFormat { return CompressionTar }
func (t *tarAdapter) Extension() string         { return ".tar" }
func (t *tarAdapter) ContentTypes() []string {
	return []string{
		"application/x-tar",
		"application/tar",
	}
}
func (t *tarAdapter) IsSupported() bool   { return true }
func (t *tarAdapter) Description() string { return "TAR archive format" }

// noneAdapter implements CompressionAdapter for uncompressed format
type noneAdapter struct{}

func (n *noneAdapter) Format() CompressionFormat { return CompressionNone }
func (n *noneAdapter) Extension() string         { return "" }
func (n *noneAdapter) ContentTypes() []string {
	return []string{
		"application/octet-stream",
		"binary/octet-stream",
	}
}
func (n *noneAdapter) IsSupported() bool   { return true }
func (n *noneAdapter) Description() string { return "Uncompressed data" }

// CompressionRegistry manages available compression formats
type CompressionRegistry struct {
	adapters map[CompressionFormat]CompressionAdapter
}

// NewCompressionRegistry creates a new registry with default compression formats
func NewCompressionRegistry() *CompressionRegistry {
	registry := &CompressionRegistry{
		adapters: make(map[CompressionFormat]CompressionAdapter),
	}
	
	// Register default compression formats
	registry.Register(&gzipAdapter{})
	registry.Register(&zstdAdapter{}) 
	registry.Register(&tarAdapter{})
	registry.Register(&noneAdapter{})
	
	return registry
}

// Register adds a new compression format to the registry
func (r *CompressionRegistry) Register(adapter CompressionAdapter) {
	r.adapters[adapter.Format()] = adapter
}

// Get returns the adapter for the specified format
func (r *CompressionRegistry) Get(format CompressionFormat) (CompressionAdapter, bool) {
	adapter, exists := r.adapters[format]
	return adapter, exists
}

// GetByContentType returns the adapter that matches the given content type
func (r *CompressionRegistry) GetByContentType(contentType string) (CompressionAdapter, bool) {
	// Normalize content type
	ctBase := strings.Split(contentType, ";")[0]
	ctBase = strings.TrimSpace(strings.ToLower(ctBase))
	
	for _, adapter := range r.adapters {
		for _, ct := range adapter.ContentTypes() {
			if ct == ctBase {
				return adapter, true
			}
		}
	}
	
	return nil, false
}

// GetByExtension returns the adapter that matches the given file extension
func (r *CompressionRegistry) GetByExtension(extension string) (CompressionAdapter, bool) {
	extension = strings.ToLower(extension)
	
	for _, adapter := range r.adapters {
		if adapter.Extension() == extension {
			return adapter, true
		}
	}
	
	return nil, false
}

// GetSupported returns all supported compression formats
func (r *CompressionRegistry) GetSupported() []CompressionAdapter {
	var supported []CompressionAdapter
	for _, adapter := range r.adapters {
		if adapter.IsSupported() {
			supported = append(supported, adapter)
		}
	}
	return supported
}

// IsValidContentType checks if the given content type is supported
func (r *CompressionRegistry) IsValidContentType(contentType string) bool {
	_, exists := r.GetByContentType(contentType)
	return exists
}

// Global compression registry instance
var DefaultCompressionRegistry = NewCompressionRegistry()

// IsValidBackupContentType validates if the given content type is supported for backup restoration
// This function uses the CompressionRegistry for extensible format support
func IsValidBackupContentType(contentType string) bool {
	return DefaultCompressionRegistry.IsValidContentType(contentType)
}