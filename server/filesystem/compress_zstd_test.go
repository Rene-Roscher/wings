package filesystem

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Rene-Roscher/wings/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestZstdBinaryCompression tests ZSTD compression with binary files
func TestZstdBinaryCompression(t *testing.T) {
	// Setup config for ZSTD
	setupZstdConfig()
	
	// Use the test filesystem helper
	fs, _ := NewFs()
	tmpDir := fs.Path()

	// Use a simple binary that's guaranteed to exist
	testBinary := "/bin/echo"
	if _, err := os.Stat(testBinary); os.IsNotExist(err) {
		t.Skip("Test binary not found")
	}

	// Copy binary to test directory
	binaryName := "test-echo"
	testPath := filepath.Join(tmpDir, binaryName)
	err := copyBinaryFile(testBinary, testPath)
	require.NoError(t, err)

	// Get original checksum
	originalChecksum, err := calculateBinaryChecksum(testPath)
	require.NoError(t, err)
	t.Logf("Original checksum: %s", originalChecksum)

	// Get original permissions
	originalInfo, err := os.Stat(testPath)
	require.NoError(t, err)
	originalMode := originalInfo.Mode()
	t.Logf("Original permissions: %v", originalMode)

	// Test original binary works
	cmd := exec.Command(testPath, "hello", "zstd", "test")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err)
	t.Logf("Original output: %s", string(output))

	// Create ZSTD archive
	a := &Archive{
		Filesystem:    fs,
		BaseDirectory: "",
		Files:         []string{binaryName},
	}
	
	// Create the archive file
	archivePath := filepath.Join(tmpDir, "test-archive.tar.zst")
	f, err := os.Create(archivePath)
	require.NoError(t, err)
	
	// Stream the archive
	err = a.Stream(context.Background(), f)
	f.Close()
	require.NoError(t, err)
	
	// Verify archive was created
	archiveInfo, err := os.Stat(archivePath)
	require.NoError(t, err)
	t.Logf("Created ZSTD archive: %s (size: %d bytes)", archivePath, archiveInfo.Size())

	// Delete original binary
	err = os.Remove(testPath)
	require.NoError(t, err)

	// Verify deletion
	_, err = os.Stat(testPath)
	require.True(t, os.IsNotExist(err))

	// Extract using our DecompressFile function
	err = fs.DecompressFile(context.Background(), "", "test-archive.tar.zst")
	require.NoError(t, err)

	// Verify binary was restored
	restoredInfo, err := os.Stat(testPath)
	require.NoError(t, err)
	t.Logf("Restored permissions: %v", restoredInfo.Mode())

	// Check permissions preserved
	assert.Equal(t, originalMode, restoredInfo.Mode(), "Permissions should be preserved")

	// Check checksum
	restoredChecksum, err := calculateBinaryChecksum(testPath)
	require.NoError(t, err)
	t.Logf("Restored checksum: %s", restoredChecksum)
	assert.Equal(t, originalChecksum, restoredChecksum, "Checksum should match")

	// Most important: Test restored binary works
	cmd = exec.Command(testPath, "hello", "zstd", "test")
	restoredOutput, err := cmd.CombinedOutput()
	require.NoError(t, err, "Restored binary should execute")
	assert.Equal(t, string(output), string(restoredOutput), "Output should match")
}

// TestZstdWithSystemTarVerification verifies system tar handles ZSTD correctly
func TestZstdWithSystemTarVerification(t *testing.T) {
	// Check if tar supports zstd
	cmd := exec.Command("tar", "--help")
	output, _ := cmd.CombinedOutput()
	if !contains(string(output), "zstd") {
		t.Skip("System tar doesn't support ZSTD")
	}

	tmpDir, err := os.MkdirTemp("", "zstd-tar-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create test file
	testFile := filepath.Join(tmpDir, "test.bin")
	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	err = os.WriteFile(testFile, testData, 0644)
	require.NoError(t, err)

	// Create ZSTD archive with system tar
	archivePath := filepath.Join(tmpDir, "test.tar.zst")
	cmd = exec.Command("tar", "--zstd", "-cf", archivePath, "-C", tmpDir, "test.bin")
	err = cmd.Run()
	require.NoError(t, err)

	// Delete original
	os.Remove(testFile)

	// Extract with system tar
	cmd = exec.Command("tar", "--zstd", "-xf", archivePath, "-C", tmpDir)
	err = cmd.Run()
	require.NoError(t, err)

	// Verify content
	restoredData, err := os.ReadFile(testFile)
	require.NoError(t, err)
	assert.Equal(t, testData, restoredData, "Data should be identical")
}

func setupZstdConfig() {
	// Always set config to ensure it's initialized
	config.Set(&config.Configuration{
		AuthenticationToken: "test",
		System: config.SystemConfiguration{
			Backups: config.Backups{
				Format:           "zstd",
				CompressionLevel: "default",
			},
		},
	})
}

func copyBinaryFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	sourceInfo, err := sourceFile.Stat()
	if err != nil {
		return err
	}

	destFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, sourceInfo.Mode())
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

func calculateBinaryChecksum(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || len(s) > len(substr) && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}