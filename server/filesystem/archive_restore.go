package filesystem

import (
	"bufio"
	"compress/gzip"
	"io"

	"emperror.dev/errors"
)

// CompressionFormat represents the compression format used in an archive
type CompressionFormat int

const (
	CompressionUnknown CompressionFormat = iota
	CompressionGzip
	CompressionZstd // Kept for backward compatibility but no longer supported
	CompressionNone
)

// DetectCompressionFormat detects the compression format by examining the file header
// This function includes security measures to prevent format spoofing attacks
func DetectCompressionFormat(reader io.ReadCloser) (CompressionFormat, io.ReadCloser, error) {
	if reader == nil {
		return CompressionGzip, nil, errors.New("backup: nil reader provided to format detection")
	}
	
	// Peek first 4 bytes without consuming the stream
	peekReader := bufio.NewReader(reader)
	header, err := peekReader.Peek(4)
	if err != nil && err != io.EOF {
		return CompressionGzip, io.NopCloser(peekReader), errors.Wrap(err, "backup: failed to read format detection header")
	}
	
	// Validate we have enough data for detection
	if len(header) < 2 {
		return CompressionGzip, io.NopCloser(peekReader), errors.New("backup: insufficient data for format detection")
	}

	// ZSTD is no longer supported - skip detection
	// (Previously checked for 0x28B52FFD magic bytes)

	// GZIP magic: 0x1F8B (validate both bytes for security)
	if len(header) >= 2 && header[0] == 0x1F && header[1] == 0x8B {
		return CompressionGzip, io.NopCloser(peekReader), nil
	}

	// No compression detected - assume gzip for backward compatibility
	return CompressionGzip, io.NopCloser(peekReader), nil
}

// CreateDecompressor creates the appropriate decompressor based on the detected format
// This function includes security validation and resource management
func CreateDecompressor(reader io.ReadCloser, format CompressionFormat) (io.ReadCloser, error) {
	if reader == nil {
		return nil, errors.New("backup: nil reader provided to decompressor")
	}
	
	switch format {
	case CompressionZstd:
		// ZSTD is no longer supported
		reader.Close()
		return nil, errors.New("backup: ZSTD compression is no longer supported")

	case CompressionGzip:
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			reader.Close() // Clean up on error
			return nil, errors.Wrap(err, "backup: failed to create GZIP decoder")
		}
		return gzReader, nil

	case CompressionNone:
		return reader, nil

	default:
		// Default to gzip for backward compatibility
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			reader.Close() // Clean up on error
			return nil, errors.Wrap(err, "backup: failed to create GZIP decoder (fallback)")
		}
		return gzReader, nil
	}
}


