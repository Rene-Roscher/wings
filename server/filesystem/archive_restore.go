package filesystem

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
	"runtime"

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
func DetectCompressionFormat(reader io.ReadCloser) (CompressionFormat, io.ReadCloser, error) {
	// Peek first 4 bytes without consuming the stream
	peekReader := bufio.NewReader(reader)
	header, err := peekReader.Peek(4)
	if err != nil {
		return CompressionUnknown, reader, err
	}

	// ZSTD magic: 0x28B52FFD
	if len(header) >= 4 && bytes.Equal(header, []byte{0x28, 0xB5, 0x2F, 0xFD}) {
		return CompressionZstd, io.NopCloser(peekReader), nil
	}

	// GZIP magic: 0x1F8B
	if len(header) >= 2 && header[0] == 0x1F && header[1] == 0x8B {
		return CompressionGzip, io.NopCloser(peekReader), nil
	}

	// No compression detected - assume raw TAR or gzip for backward compatibility
	return CompressionGzip, io.NopCloser(peekReader), nil
}

// CreateDecompressor creates the appropriate decompressor based on the detected format
func CreateDecompressor(reader io.ReadCloser, format CompressionFormat) (io.ReadCloser, error) {
	switch format {
	case CompressionZstd:
		decoder, err := zstd.NewReader(reader,
			zstd.WithDecoderConcurrency(min(4, runtime.NumCPU())),
			zstd.WithDecoderLowmem(true),
		)
		if err != nil {
			return nil, err
		}
		return &zstdReadCloser{decoder, reader}, nil

	case CompressionGzip:
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		return gzReader, nil

	case CompressionNone:
		return reader, nil

	default:
		// Default to gzip for backward compatibility
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, err
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
