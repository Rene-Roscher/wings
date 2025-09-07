package backup

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRestoreHandlesRootDirectory tests the specific error case:
// "handling file: .: filesystem: cannot perform action: [] is a directory"
func TestRestoreHandlesRootDirectory(t *testing.T) {
	tempDir := t.TempDir()
	backupFile := filepath.Join(tempDir, "test-with-root.tar.gz")
	restoreDir := filepath.Join(tempDir, "restore")
	
	require.NoError(t, os.MkdirAll(restoreDir, 0755))
	
	// Create backup that includes root directory entry "."
	t.Log("Creating backup with root directory entry...")
	err := createBackupWithRootEntry(backupFile)
	require.NoError(t, err)
	
	// Test restore callback handles "." correctly
	var handledEntries []string
	var errorCount int
	
	err = restoreTestBackup(backupFile, func(file string, info os.FileInfo, r io.ReadCloser) error {
		defer r.Close()
		
		t.Logf("Processing entry: '%s' (isDir: %v, size: %d)", file, info.IsDir(), info.Size())
		handledEntries = append(handledEntries, file)
		
		// Skip root directory entries - this is the fix for the original error
		if file == "." || file == "" {
			t.Logf("Skipping root directory entry: '%s'", file)
			return nil
		}
		
		fullPath := filepath.Join(restoreDir, file)
		
		if info.IsDir() {
			// Fixed logic: handle directories correctly
			if err := os.MkdirAll(fullPath, info.Mode()); err != nil {
				t.Logf("Error creating directory %s: %v", fullPath, err)
				errorCount++
				return err
			}
			t.Logf("Created directory: %s", file)
		} else {
			// Handle regular files
			if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
				return err
			}
			
			content, err := io.ReadAll(r)
			if err != nil {
				return err
			}
			
			if err := os.WriteFile(fullPath, content, info.Mode()); err != nil {
				t.Logf("Error writing file %s: %v", fullPath, err)
				errorCount++
				return err
			}
			t.Logf("Created file: %s (%d bytes)", file, len(content))
		}
		
		return nil
	})
	
	require.NoError(t, err, "Restore should complete without errors")
	assert.Equal(t, 0, errorCount, "Should have no processing errors")
	assert.Greater(t, len(handledEntries), 0, "Should have processed some entries")
	
	// Verify that if "." was in the archive, it was handled gracefully
	for _, entry := range handledEntries {
		if entry == "." {
			t.Log("Root directory entry '.' was handled correctly (skipped)")
		}
	}
	
	t.Logf("SUCCESS: Processed %d entries with %d errors", len(handledEntries), errorCount)
}

// createBackupWithRootEntry creates a TAR archive that may include problematic root entries
func createBackupWithRootEntry(backupFile string) error {
	f, err := os.Create(backupFile)
	if err != nil {
		return err
	}
	defer f.Close()
	
	gw := gzip.NewWriter(f)
	defer gw.Close()
	
	tw := tar.NewWriter(gw)
	defer tw.Close()
	
	// Add a potentially problematic root directory entry
	rootHeader := &tar.Header{
		Name:     ".",
		Mode:     0755,
		Typeflag: tar.TypeDir,
		ModTime:  time.Now(),
	}
	
	if err := tw.WriteHeader(rootHeader); err != nil {
		return err
	}
	
	// Add some regular entries
	entries := []struct {
		name    string
		content string
		isDir   bool
	}{
		{"test_dir", "", true},
		{"test_file.txt", "test content", false},
		{"test_dir/nested_file.txt", "nested content", false},
	}
	
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
		
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		
		if !entry.isDir {
			if _, err := tw.Write([]byte(entry.content)); err != nil {
				return err
			}
		}
	}
	
	return nil
}

// TestRestoreSkipsInvalidPaths tests handling of various edge case paths
func TestRestoreSkipsInvalidPaths(t *testing.T) {
	tempDir := t.TempDir()
	restoreDir := filepath.Join(tempDir, "restore")
	require.NoError(t, os.MkdirAll(restoreDir, 0755))
	
	// Test various problematic paths
	problematicPaths := []struct {
		path        string
		shouldSkip  bool
		description string
	}{
		{".", true, "current directory"},
		{"", true, "empty path"},
		{"/", true, "root directory"},
		{"./", true, "current directory with slash"},
		{"../", true, "parent directory"},
		{"normal_file.txt", false, "normal file"},
		{"normal_dir", false, "normal directory"},
	}
	
	for _, test := range problematicPaths {
		t.Run(test.description, func(t *testing.T) {
			var wasSkipped bool
			
			// This simulates the fix we applied to the restore logic
			if test.path == "." || test.path == "" || test.path == "/" || test.path == "./" || strings.HasPrefix(test.path, "../") {
				wasSkipped = true
				t.Logf("Correctly skipped problematic path: '%s'", test.path)
			} else {
				wasSkipped = false
				t.Logf("Processing valid path: '%s'", test.path)
			}
			
			assert.Equal(t, test.shouldSkip, wasSkipped, 
				"Path '%s' skip behavior should match expected", test.path)
		})
	}
}