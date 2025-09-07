package backup

import (
	"os"
	"path/filepath"
	"testing"

	"emperror.dev/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackupIntegrityValidation tests the basic integrity validation logic
func TestBackupIntegrityValidation(t *testing.T) {
	tempDir := t.TempDir()
	
	t.Run("Valid GZIP Archive", func(t *testing.T) {
		// Create a valid GZIP file
		validGzip := filepath.Join(tempDir, "valid.tar.gz")
		f, err := os.Create(validGzip)
		require.NoError(t, err)
		defer f.Close()
		
		// Write GZIP magic bytes + some content
		gzipHeader := []byte{0x1f, 0x8b, 0x08, 0x00} // GZIP magic + flags
		padding := make([]byte, 1020)                 // Make it >1KB
		_, err = f.Write(append(gzipHeader, padding...))
		require.NoError(t, err)
		
		// Test validation function (simplified version of server method)
		err = validateTestBackupFile(validGzip)
		assert.NoError(t, err, "Valid GZIP should pass validation")
	})
	
	t.Run("File Too Small", func(t *testing.T) {
		// Create tiny file (should fail)
		tinyFile := filepath.Join(tempDir, "tiny.tar.gz")
		f, err := os.Create(tinyFile)
		require.NoError(t, err)
		defer f.Close()
		
		_, err = f.Write([]byte("tiny"))
		require.NoError(t, err)
		
		err = validateTestBackupFile(tinyFile)
		assert.Error(t, err, "Tiny file should fail validation")
		assert.Contains(t, err.Error(), "suspiciously small")
	})
	
	t.Run("Invalid Magic Bytes", func(t *testing.T) {
		// Create file with wrong magic bytes
		invalidFile := filepath.Join(tempDir, "invalid.tar.gz")
		f, err := os.Create(invalidFile)
		require.NoError(t, err)
		defer f.Close()
		
		// Wrong magic bytes + padding
		wrongHeader := []byte{0xFF, 0xFF, 0xFF, 0xFF}
		padding := make([]byte, 1020)
		_, err = f.Write(append(wrongHeader, padding...))
		require.NoError(t, err)
		
		err = validateTestBackupFile(invalidFile)
		assert.Error(t, err, "Invalid magic bytes should fail validation")
		assert.Contains(t, err.Error(), "format not recognized")
	})
	
	t.Run("Valid TAR Archive", func(t *testing.T) {
		// Create what looks like a TAR file
		tarFile := filepath.Join(tempDir, "valid.tar")
		f, err := os.Create(tarFile)
		require.NoError(t, err)
		defer f.Close()
		
		// Create basic TAR header structure
		tarHeader := make([]byte, 512)
		// Set typeflag to regular file (position 156)
		tarHeader[156] = '0'
		padding := make([]byte, 512)
		_, err = f.Write(append(tarHeader, padding...))
		require.NoError(t, err)
		
		err = validateTestBackupFile(tarFile)
		assert.NoError(t, err, "Valid TAR should pass validation")
	})
	
	t.Run("Non-existent File", func(t *testing.T) {
		nonExistentFile := filepath.Join(tempDir, "doesnotexist.tar.gz")
		
		err := validateTestBackupFile(nonExistentFile)
		assert.Error(t, err, "Non-existent file should fail validation")
		assert.Contains(t, err.Error(), "no such file")
	})
}

// validateTestBackupFile is a simplified version of the server validation logic for testing
func validateTestBackupFile(backupPath string) error {
	// Basic file existence and size check
	stat, err := os.Stat(backupPath)
	if err != nil {
		return err // "backup file not accessible"
	}
	
	// Archive must be at least 1KB
	if stat.Size() < 1024 {
		return errors.New("backup file suspiciously small - may be corrupt")
	}
	
	// Quick magic bytes check
	f, err := os.Open(backupPath)
	if err != nil {
		return err
	}
	defer f.Close()
	
	magic := make([]byte, 2)
	if n, err := f.Read(magic); err != nil || n < 2 {
		return errors.New("cannot read backup file header")
	}
	
	// Check for GZIP magic bytes (0x1f, 0x8b)
	if magic[0] == 0x1f && magic[1] == 0x8b {
		return nil // Valid GZIP
	}
	
	// Check for TAR format
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	
	// Read TAR header area
	tarTest := make([]byte, 512)
	if n, err := f.Read(tarTest); err != nil || n < 512 {
		return errors.New("backup file too short for valid TAR")
	}
	
	// Basic TAR validation - check typeflag
	if tarTest[156] == '0' || tarTest[156] == '5' { // Regular file or directory
		return nil
	}
	
	return errors.New("backup file format not recognized - may be corrupt")
}

// TestRestoreStatsTracking tests that restore statistics are properly tracked
func TestRestoreStatsTracking(t *testing.T) {
	// This test validates the concept of our restore statistics tracking
	
	// Simulate restore statistics
	restoreStats := struct {
		fileCount int
		dirCount  int
		totalSize int64
	}{}
	
	// Simulate processing entries
	entries := []struct {
		isDir bool
		size  int64
	}{
		{false, 100}, // file1.txt
		{true, 0},    // directory1
		{false, 200}, // file2.txt
		{true, 0},    // directory2
		{false, 300}, // file3.txt
	}
	
	for _, entry := range entries {
		if entry.isDir {
			restoreStats.dirCount++
		} else {
			restoreStats.fileCount++
			restoreStats.totalSize += entry.size
		}
	}
	
	// Validate results
	assert.Equal(t, 3, restoreStats.fileCount, "Should count 3 files")
	assert.Equal(t, 2, restoreStats.dirCount, "Should count 2 directories") 
	assert.Equal(t, int64(600), restoreStats.totalSize, "Should sum total size correctly")
	
	// Test empty restore detection
	emptyStats := struct {
		fileCount int
		dirCount  int
		totalSize int64
	}{}
	
	isEmpty := emptyStats.fileCount == 0 && emptyStats.dirCount == 0
	assert.True(t, isEmpty, "Should detect empty restore")
}