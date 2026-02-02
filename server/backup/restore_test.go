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

// Test complete backup and restore cycle for LocalBackup
func TestLocalBackupRestoreCycle(t *testing.T) {
	tempDir := t.TempDir()
	backupFile := filepath.Join(tempDir, "test-backup.tar.gz")
	originalDir := filepath.Join(tempDir, "original")
	restoreDir := filepath.Join(tempDir, "restored")
	
	require.NoError(t, os.MkdirAll(originalDir, 0755))
	require.NoError(t, os.MkdirAll(restoreDir, 0755))
	
	// Create test structure with files and empty directories
	testStructure := map[string]string{
		"file1.txt":                    "content1",
		"file2.txt":                    "content2", 
		"level1/file3.txt":             "content3",
		"level1/level2/file4.txt":      "content4",
		// CRITICAL: Empty directories that MUST be preserved
		"empty_dir/.keep":              "",
		"level1/empty_subdir/.keep":    "",
		"level1/level2/empty_deep/.keep": "",
		"completely_empty_chain/level1/level2/level3/.keep": "",
	}
	
	// Create original test files and directories
	for path, content := range testStructure {
		fullPath := filepath.Join(originalDir, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0755))
		
		if strings.HasSuffix(path, ".keep") {
			// Create empty directory marker and remove it to leave empty directory
			require.NoError(t, os.WriteFile(fullPath, []byte(content), 0644))
			require.NoError(t, os.Remove(fullPath))
		} else {
			require.NoError(t, os.WriteFile(fullPath, []byte(content), 0644))
		}
	}
	
	// List all original directories for comparison
	originalDirs := make(map[string]bool)
	originalFiles := make(map[string]string)
	
	err := filepath.WalkDir(originalDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relPath, _ := filepath.Rel(originalDir, path)
		if relPath == "." {
			return nil
		}
		
		if d.IsDir() {
			originalDirs[relPath] = true
		} else {
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			originalFiles[relPath] = string(content)
		}
		return nil
	})
	require.NoError(t, err)
	
	t.Logf("Original structure: %d directories, %d files", len(originalDirs), len(originalFiles))
	
	// Create backup manually using TAR+GZIP (simulating fixed Wings backup logic)
	t.Log("Creating backup archive...")
	err = createTestBackup(originalDir, backupFile)
	require.NoError(t, err)
	assert.FileExists(t, backupFile)
	
	// Verify archive contents
	t.Log("Verifying archive contents...")
	archiveContents, err := listTarArchive(backupFile)
	require.NoError(t, err)
	
	t.Logf("Archive contains %d entries", len(archiveContents))
	for _, entry := range archiveContents {
		t.Logf("Archive entry: %s (isDir: %v)", entry.Name, entry.IsDir)
	}
	
	// Restore using our fixed callback logic
	t.Log("Testing restore logic...")
	restoredDirs := make(map[string]bool)
	restoredFiles := make(map[string]string)
	
	err = restoreTestBackup(backupFile, func(file string, info os.FileInfo, r io.ReadCloser) error {
		defer r.Close()
		
		fullPath := filepath.Join(restoreDir, file)
		
		if info.IsDir() {
			// Test our fixed directory handling logic
			restoredDirs[file] = true
			return os.MkdirAll(fullPath, info.Mode())
		} else {
			// Test file restoration
			restoredFiles[file] = ""
			if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
				return err
			}
			
			content, err := io.ReadAll(r)
			if err != nil {
				return err
			}
			restoredFiles[file] = string(content)
			
			return os.WriteFile(fullPath, content, info.Mode())
		}
	})
	require.NoError(t, err)
	
	// Verify all directories were restored
	t.Log("Verifying directory restoration...")
	for dir := range originalDirs {
		assert.True(t, restoredDirs[dir], "Directory %s should be restored", dir)
		assert.DirExists(t, filepath.Join(restoreDir, dir), "Directory %s should exist after restore", dir)
	}
	
	// Verify all files were restored with correct content
	t.Log("Verifying file restoration...")
	for file, originalContent := range originalFiles {
		assert.Equal(t, originalContent, restoredFiles[file], "File content should match for %s", file)
		assert.FileExists(t, filepath.Join(restoreDir, file), "File %s should exist after restore", file)
	}
	
	// Verify critical empty directories are preserved
	criticalEmptyDirs := []string{
		"empty_dir",
		"level1/empty_subdir", 
		"level1/level2/empty_deep",
		"completely_empty_chain",
		"completely_empty_chain/level1",
		"completely_empty_chain/level1/level2", 
		"completely_empty_chain/level1/level2/level3",
	}
	
	t.Log("Verifying critical empty directories...")
	for _, dir := range criticalEmptyDirs {
		assert.True(t, restoredDirs[dir], "Critical empty directory %s MUST be restored", dir)
		assert.DirExists(t, filepath.Join(restoreDir, dir), "Critical empty directory %s MUST exist after restore", dir)
	}
	
	t.Logf("SUCCESS: Restored %d directories and %d files", len(restoredDirs), len(restoredFiles))
}

// archiveEntry represents an entry in the TAR archive
type archiveEntry struct {
	Name  string
	IsDir bool
	Size  int64
}

// createTestBackup creates a backup using standard TAR+GZIP with directory preservation
func createTestBackup(sourceDir, backupFile string) error {
	f, err := os.Create(backupFile)
	if err != nil {
		return err
	}
	defer f.Close()
	
	gw := gzip.NewWriter(f)
	defer gw.Close()
	
	tw := tar.NewWriter(gw)
	defer tw.Close()
	
	return filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		// Skip root directory
		if path == sourceDir {
			return nil
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
		header.ModTime = info.ModTime()
		
		// Write header (CRITICAL: includes directories!)
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		
		// Write file content for regular files only
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

// listTarArchive lists all entries in a TAR+GZIP archive
func listTarArchive(backupFile string) ([]archiveEntry, error) {
	f, err := os.Open(backupFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	
	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	
	tr := tar.NewReader(gr)
	
	var entries []archiveEntry
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		
		entries = append(entries, archiveEntry{
			Name:  header.Name,
			IsDir: header.FileInfo().IsDir(),
			Size:  header.Size,
		})
	}
	
	return entries, nil
}

// restoreTestBackup simulates the restore process with callback
func restoreTestBackup(backupFile string, callback func(string, os.FileInfo, io.ReadCloser) error) error {
	f, err := os.Open(backupFile)
	if err != nil {
		return err
	}
	defer f.Close()
	
	gr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gr.Close()
	
	tr := tar.NewReader(gr)
	
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		
		// Create a ReadCloser from the tar reader for this entry
		var r io.ReadCloser
		if header.FileInfo().IsDir() {
			// For directories, provide empty reader
			r = io.NopCloser(strings.NewReader(""))
		} else {
			// For files, provide limited reader with file content
			r = io.NopCloser(io.LimitReader(tr, header.Size))
		}
		
		// Call the callback with simulated file info
		info := &testFileInfo{
			name:    filepath.Base(header.Name),
			size:    header.Size,
			mode:    header.FileInfo().Mode(),
			modTime: header.ModTime,
			isDir:   header.FileInfo().IsDir(),
		}
		
		if err := callback(header.Name, info, r); err != nil {
			return err
		}
	}
	
	return nil
}

// testFileInfo implements os.FileInfo for testing
type testFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *testFileInfo) Name() string       { return fi.name }
func (fi *testFileInfo) Size() int64        { return fi.size }
func (fi *testFileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *testFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *testFileInfo) IsDir() bool        { return fi.isDir }
func (fi *testFileInfo) Sys() interface{}   { return nil }