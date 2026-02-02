package filesystem

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestDetectCompressionFormat(t *testing.T) {
	tests := []struct {
		name           string
		data           []byte
		expectedFormat CompressionFormat
	}{
		{
			name:           "GZIP format",
			data:           []byte{0x1F, 0x8B, 0x08, 0x00}, // GZIP magic
			expectedFormat: CompressionGzip,
		},
		{
			name:           "ZSTD format (no longer supported, falls back to GZIP)",
			data:           []byte{0x28, 0xB5, 0x2F, 0xFD}, // ZSTD magic
			expectedFormat: CompressionGzip,                 // Falls back to GZIP since ZSTD is not supported
		},
		{
			name:           "Unknown format defaults to GZIP",
			data:           []byte{0x00, 0x00, 0x00, 0x00},
			expectedFormat: CompressionGzip,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := io.NopCloser(bytes.NewReader(tt.data))
			format, _, err := DetectCompressionFormat(reader)

			if err != nil {
				t.Errorf("DetectCompressionFormat() error = %v", err)
				return
			}

			if format != tt.expectedFormat {
				t.Errorf("DetectCompressionFormat() = %v, want %v", format, tt.expectedFormat)
			}
		})
	}
}

func TestCreateDecompressor(t *testing.T) {
	// Test GZIP decompressor
	t.Run("GZIP decompressor", func(t *testing.T) {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, err := gw.Write([]byte("test data"))
		if err != nil {
			t.Fatal(err)
		}
		gw.Close()

		reader := io.NopCloser(bytes.NewReader(buf.Bytes()))
		decompressor, err := CreateDecompressor(reader, CompressionGzip)
		if err != nil {
			t.Errorf("CreateDecompressor() error = %v", err)
			return
		}
		defer decompressor.Close()

		data, err := io.ReadAll(decompressor)
		if err != nil {
			t.Errorf("Failed to read from GZIP decompressor: %v", err)
			return
		}

		if string(data) != "test data" {
			t.Errorf("GZIP decompression failed: got %s, want 'test data'", string(data))
		}
	})

	// Test ZSTD decompressor (should fail as ZSTD is no longer supported)
	t.Run("ZSTD decompressor", func(t *testing.T) {
		var buf bytes.Buffer
		zw, err := zstd.NewWriter(&buf)
		if err != nil {
			t.Fatal(err)
		}
		_, err = zw.Write([]byte("test data"))
		if err != nil {
			t.Fatal(err)
		}
		zw.Close()

		reader := io.NopCloser(bytes.NewReader(buf.Bytes()))
		decompressor, err := CreateDecompressor(reader, CompressionZstd)

		// ZSTD is no longer supported, should return an error
		if err == nil {
			if decompressor != nil {
				decompressor.Close()
			}
			t.Error("CreateDecompressor() should return error for ZSTD format (no longer supported)")
			return
		}

		// Verify the error message contains expected text
		expectedErrMsg := "ZSTD compression is no longer supported"
		if !bytes.Contains([]byte(err.Error()), []byte(expectedErrMsg)) {
			t.Errorf("CreateDecompressor() error = %v, should contain %q", err, expectedErrMsg)
		}
	})
}
