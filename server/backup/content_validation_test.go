package backup

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestContentValidationFileCount tests the file/directory count validation
func TestContentValidationFileCount(t *testing.T) {
	tempDir := t.TempDir()
	originalDir := filepath.Join(tempDir, "original")
	backupFile := filepath.Join(tempDir, "backup.tar.gz")
	
	require.NoError(t, os.MkdirAll(originalDir, 0755))
	
	// Create test structure
	testStructure := map[string]string{
		"file1.txt":                    "content1",
		"file2.txt":                    "content2", 
		"level1/file3.txt":             "content3",
		"level1/level2/file4.txt":      "content4",
		"empty_dir/.keep":              "",
		"level1/empty_subdir/.keep":    "",
	}
	
	// Create original files and directories
	for path, content := range testStructure {
		fullPath := filepath.Join(originalDir, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0755))
		
		if strings.HasSuffix(path, ".keep") {
			// Create and remove .keep to leave empty directory
			require.NoError(t, os.WriteFile(fullPath, []byte(content), 0644))
			require.NoError(t, os.Remove(fullPath))
		} else {
			require.NoError(t, os.WriteFile(fullPath, []byte(content), 0644))
		}
	}
	
	// Test 1: Create correct backup - should validate successfully
	t.Run("Correct Backup Validates", func(t *testing.T) {
		err := createCorrectBackup(originalDir, backupFile)
		require.NoError(t, err)
		
		// Test validation functions directly
		originalStats, err := countServerFiles(originalDir)
		require.NoError(t, err)
		
		backupStats, err := countBackupEntries(backupFile)
		require.NoError(t, err)
		
		assert.Equal(t, originalStats.FileCount, backupStats.FileCount, "File counts should match")
		assert.Equal(t, originalStats.DirCount, backupStats.DirCount, "Directory counts should match")
		
		// Expected counts based on test structure
		assert.Equal(t, 4, originalStats.FileCount, "Should count 4 files")
		assert.Equal(t, 4, originalStats.DirCount, "Should count 4 directories (level1, level1/level2, empty_dir, level1/empty_subdir)")
	})
	
	// Test 2: Incomplete backup - should fail validation
	t.Run("Incomplete Backup Fails Validation", func(t *testing.T) {
		err := createIncompleteBackup(originalDir, backupFile+"_incomplete")
		require.NoError(t, err)
		
		originalStats, err := countServerFiles(originalDir)
		require.NoError(t, err)
		
		backupStats, err := countBackupEntries(backupFile + "_incomplete")
		require.NoError(t, err)
		
		// Incomplete backup should have fewer entries
		assert.Less(t, backupStats.FileCount, originalStats.FileCount, "Incomplete backup should have fewer files")
	})
}

// Helper functions to simulate the server methods for testing

// countServerFiles simulates Server.countServerFilesAndDirs for testing
func countServerFiles(serverPath string) (*fileStats, error) {
	stats := &fileStats{}
	
	err := filepath.Walk(serverPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip unreadable files
		}
		
		// Skip root directory
		if path == serverPath {
			return nil
		}
		
		if info.IsDir() {
			stats.DirCount++
		} else {
			stats.FileCount++
		}
		
		return nil
	})
	
	return stats, err
}

// countBackupEntries simulates Server.countBackupEntries for testing
func countBackupEntries(backupPath string) (*fileStats, error) {
	stats := &fileStats{}
	
	f, err := os.Open(backupPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	
	// Simple GZIP reader for test
	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	
	// Scan TAR headers
	tarReader := tar.NewReader(gr)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		
		// Skip root entries (same logic as real implementation)
		if header.Name == "." || header.Name == "" || header.Name == "/" || 
		   header.Name == "./" || strings.HasPrefix(header.Name, "../") {
			continue
		}
		
		if header.FileInfo().IsDir() {
			stats.DirCount++
		} else {
			stats.FileCount++
		}
	}
	
	return stats, nil
}

// fileStats matches the struct from backup.go
type fileStats struct {
	FileCount int
	DirCount  int
}

// createCorrectBackup creates a complete backup of the source directory
func createCorrectBackup(sourceDir, backupFile string) error {
	f, err := os.Create(backupFile)
	if err != nil {
		return err
	}
	defer f.Close()
	
	gw := gzip.NewWriter(f)
	defer gw.Close()
	
	tw := tar.NewWriter(gw)
	defer tw.Close()
	
	// Walk and archive everything (including directories)
	return filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		if path == sourceDir {
			return nil // Skip root
		}
		
		rel, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		
		// Create TAR header
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = rel
		
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		
		// Write file content for regular files
		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			
			_, err = io.Copy(tw, file)
			if err != nil {
				return err
			}
		}
		
		return nil
	})
}

// createIncompleteBackup creates a backup missing some entries
func createIncompleteBackup(sourceDir, backupFile string) error {
	f, err := os.Create(backupFile)
	if err != nil {
		return err
	}
	defer f.Close()
	
	gw := gzip.NewWriter(f)
	defer gw.Close()
	
	tw := tar.NewWriter(gw)
	defer tw.Close()
	
	// Only backup some files (skip files containing "level2")
	return filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		if path == sourceDir {
			return nil
		}
		
		rel, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		
		// Skip level2 entries to simulate incomplete backup
		if strings.Contains(rel, "level2") {
			return nil
		}
		
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = rel
		
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		
		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			
			_, err = io.Copy(tw, file)
			if err != nil {
				return err
			}
		}
		
		return nil
	})
}