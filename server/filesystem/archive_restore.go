package filesystem

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
	"runtime"

	"emperror.dev/errors"
	"github.com/klauspost/compress/zstd"
)

// CompressionFormat represents the compression format used in an archive
type CompressionFormat int

const (
	CompressionUnknown CompressionFormat = iota
	CompressionGzip
	CompressionZstd
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

	// ZSTD magic: 0x28B52FFD (validate all 4 bytes for security)
	if len(header) >= 4 && bytes.Equal(header[:4], []byte{0x28, 0xB5, 0x2F, 0xFD}) {
		return CompressionZstd, io.NopCloser(peekReader), nil
	}

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
		// Create ZSTD decoder with memory limits for security
		decoder, err := zstd.NewReader(reader,
			zstd.WithDecoderConcurrency(min(2, runtime.NumCPU())), // Limit to 2 threads max
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxMemory(256*1024*1024), // 256MB memory limit
		)
		if err != nil {
			reader.Close() // Clean up on error
			return nil, errors.Wrap(err, "backup: failed to create ZSTD decoder")
		}
		return &zstdReadCloser{decoder, reader}, nil

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

// zstdReadCloser wraps a zstd decoder to provide proper Close functionality
type zstdReadCloser struct {
	decoder *zstd.Decoder
	closer  io.Closer
}

func (zrc *zstdReadCloser) Read(p []byte) (n int, err error) {
	return zrc.decoder.Read(p)
}

func (zrc *zstdReadCloser) Close() error {
	zrc.decoder.Close()
	if zrc.closer != nil {
		return zrc.closer.Close()
	}
	return nil
}

// min returns the minimum of two integers (Go 1.21+ has this built-in)
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
