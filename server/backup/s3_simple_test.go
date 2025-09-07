package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3RestoreDirectoryHandling tests that S3 restore can handle directories correctly
func TestS3RestoreDirectoryHandling(t *testing.T) {
	// Create test TAR+GZIP data in memory (simulating S3 backup content)
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	
	// Create test entries including the problematic cases
	entries := []struct {
		name    string
		content string
		isDir   bool
	}{
		{".", "", true},                         // Root directory (causes the original error)
		{"empty_dir", "", true},                // Empty directory
		{"test_dir", "", true},                 // Regular directory
		{"file1.txt", "content1", false},       // Regular file
		{"test_dir/file2.txt", "content2", false}, // Nested file
	}
	
	// Write entries to TAR
	for _, entry := range entries {
		var header *tar.Header
		
		if entry.isDir {
			header = &tar.Header{
				Name:     entry.name,
				Mode:     0755,
				Typeflag: tar.TypeDir,
				ModTime:  time.Now(),
			}
		} else {
			header = &tar.Header{
				Name:     entry.name,
				Mode:     0644,
				Size:     int64(len(entry.content)),
				Typeflag: tar.TypeReg,
				ModTime:  time.Now(),
			}
		}
		
		require.NoError(t, tw.WriteHeader(header))
		
		if !entry.isDir {
			_, err := tw.Write([]byte(entry.content))
			require.NoError(t, err)
		}
	}
	
	require.NoError(t, tw.Close())
	require.NoError(t, gw.Close())
	
	// Test S3 restore functionality - create minimal S3Backup without client dependencies
	s3backup := &S3Backup{
		Backup: Backup{
			Uuid:    "test-s3",
			adapter: S3BackupAdapter,
		},
	}
	
	// Test restore with our fixed logic
	ctx := context.Background()
	restoreReader := bytes.NewReader(buf.Bytes())
	
	var processedEntries []string
	var errorCount int
	
	err := s3backup.Restore(ctx, restoreReader, func(file string, info os.FileInfo, r io.ReadCloser) error {
		defer r.Close()
		
		t.Logf("S3 processing entry: '%s' (isDir: %v, size: %d)", file, info.IsDir(), info.Size())
		processedEntries = append(processedEntries, file)
		
		// Simulate the same logic as in server/backup.go restore callback
		if file == "." || file == "" || file == "/" {
			t.Logf("Correctly skipped problematic entry: '%s'", file)
			return nil
		}
		
		if info.IsDir() {
			t.Logf("Would create directory: %s", file)
		} else {
			content, err := io.ReadAll(r)
			if err != nil {
				errorCount++
				return err
			}
			t.Logf("Would write file: %s (%d bytes)", file, len(content))
		}
		
		return nil
	})
	
	require.NoError(t, err, "S3 restore should complete without errors")
	assert.Equal(t, 0, errorCount, "Should have no processing errors")
	
	// Verify all entries were processed
	assert.Len(t, processedEntries, 5, "Should process all 5 entries")
	
	// Verify specific entries
	assert.Contains(t, processedEntries, ".", "Root directory should be processed (but skipped)")
	assert.Contains(t, processedEntries, "empty_dir", "Empty directory should be processed")
	assert.Contains(t, processedEntries, "test_dir", "Test directory should be processed")
	assert.Contains(t, processedEntries, "file1.txt", "File1 should be processed")
	assert.Contains(t, processedEntries, "test_dir/file2.txt", "Nested file should be processed")
	
	t.Logf("SUCCESS: S3 restore processed %d entries correctly", len(processedEntries))
}

// TestS3RestoreCompressionDetection verifies S3 can detect and handle different compression formats
func TestS3RestoreCompressionDetection(t *testing.T) {
	// Test only GZIP for now (most common format)
	t.Run("GZIP_Detection", func(t *testing.T) {
		// Create simple GZIP compressed TAR
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		tw := tar.NewWriter(gw)
		
		// Single test entry
		header := &tar.Header{
			Name:     "test.txt",
			Mode:     0644,
			Size:     4,
			Typeflag: tar.TypeReg,
			ModTime:  time.Now(),
		}
		
		require.NoError(t, tw.WriteHeader(header))
		_, err := tw.Write([]byte("test"))
		require.NoError(t, err)
		require.NoError(t, tw.Close())
		require.NoError(t, gw.Close())
		
		// Test S3 restore can detect and process GZIP
		s3backup := &S3Backup{
			Backup: Backup{
				Uuid:    "test-gzip",
				adapter: S3BackupAdapter,
			},
		}
		
		ctx := context.Background()
		restoreReader := bytes.NewReader(buf.Bytes())
		
		var detectedContent string
		
		err = s3backup.Restore(ctx, restoreReader, func(file string, info os.FileInfo, r io.ReadCloser) error {
			defer r.Close()
			
			if strings.HasSuffix(file, ".txt") {
				content, err := io.ReadAll(r)
				if err != nil {
					return err
				}
				detectedContent = string(content)
			}
			
			return nil
		})
		
		require.NoError(t, err, "S3 restore should handle GZIP compression")
		assert.Equal(t, "test", detectedContent, "Content should be correctly decompressed")
		
		t.Log("SUCCESS: S3 restore correctly detected and processed GZIP compression")
	})
}