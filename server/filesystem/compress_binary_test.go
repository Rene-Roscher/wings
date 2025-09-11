package filesystem

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBinaryCompressionIntegrity tests that binary files remain intact after compress/decompress
func TestBinaryCompressionIntegrity(t *testing.T) {
	// Use the test filesystem helper
	fs, _ := NewFs()
	
	tmpDir := fs.Path() // Use the filesystem's root directory

	// Test with different binary files
	testCases := []struct {
		name       string
		binaryPath string
		testCmd    []string
	}{
		{
			name:       "ls_binary",
			binaryPath: "/bin/ls",
			testCmd:    []string{"--version"},
		},
		{
			name:       "cat_binary",
			binaryPath: "/bin/cat",
			testCmd:    []string{"--version"},
		},
		{
			name:       "echo_binary",
			binaryPath: "/bin/echo",
			testCmd:    []string{"test"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Skip if binary doesn't exist
			if _, err := os.Stat(tc.binaryPath); os.IsNotExist(err) {
				t.Skipf("Binary %s not found, skipping test", tc.binaryPath)
			}

			// Copy the binary to our test directory
			binaryName := filepath.Base(tc.binaryPath)
			testBinaryPath := filepath.Join(tmpDir, binaryName)
			
			err := copyFile(tc.binaryPath, testBinaryPath)
			require.NoError(t, err)

			// Calculate checksum of original binary
			originalChecksum, err := calculateChecksum(testBinaryPath)
			require.NoError(t, err)
			t.Logf("Original binary checksum: %s", originalChecksum)

			// Get original file permissions
			originalInfo, err := os.Stat(testBinaryPath)
			require.NoError(t, err)
			originalMode := originalInfo.Mode()
			t.Logf("Original binary permissions: %v", originalMode)

			// Test that original binary works
			cmd := exec.Command(testBinaryPath, tc.testCmd...)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "Original binary should execute successfully")
			t.Logf("Original binary output: %s", string(output))

			// Create archive - CompressFiles expects a relative path within the filesystem
			archiveInfo, err := fs.CompressFiles("", []string{binaryName})
			require.NoError(t, err)
			require.NotNil(t, archiveInfo)
			
			archivePath := filepath.Join(tmpDir, archiveInfo.Name())
			t.Logf("Created archive: %s (size: %d bytes)", archivePath, archiveInfo.Size())

			// Delete the original binary
			err = os.Remove(testBinaryPath)
			require.NoError(t, err)

			// Verify binary is deleted
			_, err = os.Stat(testBinaryPath)
			require.True(t, os.IsNotExist(err), "Binary should be deleted")

			// Decompress the archive
			err = fs.DecompressFile(context.Background(), "", archiveInfo.Name())
			require.NoError(t, err)

			// Verify binary was restored
			restoredInfo, err := os.Stat(testBinaryPath)
			require.NoError(t, err, "Binary should be restored")
			
			// Check permissions
			restoredMode := restoredInfo.Mode()
			t.Logf("Restored binary permissions: %v", restoredMode)
			
			// Check if executable bit is preserved (at least for owner)
			assert.True(t, restoredMode&0100 != 0, "Binary should have executable permission")

			// Calculate checksum of restored binary
			restoredChecksum, err := calculateChecksum(testBinaryPath)
			require.NoError(t, err)
			t.Logf("Restored binary checksum: %s", restoredChecksum)

			// Verify checksums match
			assert.Equal(t, originalChecksum, restoredChecksum, "Binary content should be identical after restore")

			// Most important: Test that restored binary works
			cmd = exec.Command(testBinaryPath, tc.testCmd...)
			restoredOutput, err := cmd.CombinedOutput()
			if err != nil {
				t.Logf("Error executing restored binary: %v", err)
				t.Logf("Output: %s", string(restoredOutput))
				
				// Try to get more info about the failure
				if exitErr, ok := err.(*exec.ExitError); ok {
					t.Logf("Exit code: %d", exitErr.ExitCode())
				}
				
				// Check with ldd if it's a library issue
				lddCmd := exec.Command("ldd", testBinaryPath)
				lddOutput, _ := lddCmd.CombinedOutput()
				t.Logf("ldd output:\n%s", string(lddOutput))
			}
			require.NoError(t, err, "Restored binary should execute successfully")
			
			// Verify output is the same
			assert.Equal(t, string(output), string(restoredOutput), "Binary output should be identical")

			// Clean up
			os.Remove(archivePath)
		})
	}
}

// TestLibraryFileCompression tests compression of shared library files
func TestLibraryFileCompression(t *testing.T) {
	// Find a small shared library to test with
	testLibs := []string{
		"/lib/x86_64-linux-gnu/libc.so.6",
		"/usr/lib/x86_64-linux-gnu/libm.so.6",
		"/lib/x86_64-linux-gnu/libpthread.so.0",
	}

	var testLib string
	for _, lib := range testLibs {
		if _, err := os.Stat(lib); err == nil {
			testLib = lib
			break
		}
	}

	if testLib == "" {
		t.Skip("No test library found")
	}

	// Use test filesystem
	fs, _ := NewFs()
	tmpDir := fs.Path()

	// Copy library to test directory
	libName := filepath.Base(testLib)
	testLibPath := filepath.Join(tmpDir, libName)
	err := copyFile(testLib, testLibPath)
	require.NoError(t, err)

	// Get original checksum
	originalChecksum, err := calculateChecksum(testLibPath)
	require.NoError(t, err)
	t.Logf("Original library checksum: %s", originalChecksum)

	// Create archive
	archiveInfo, err := fs.CompressFiles("", []string{libName})
	require.NoError(t, err)

	// Delete original
	err = os.Remove(testLibPath)
	require.NoError(t, err)

	// Decompress
	err = fs.DecompressFile(context.Background(), "", archiveInfo.Name())
	require.NoError(t, err)

	// Verify checksum
	restoredChecksum, err := calculateChecksum(testLibPath)
	require.NoError(t, err)
	t.Logf("Restored library checksum: %s", restoredChecksum)

	assert.Equal(t, originalChecksum, restoredChecksum, "Library file should be identical after restore")
}

// TestCompressionWithMultipleBinaries tests compressing multiple binaries at once
func TestCompressionWithMultipleBinaries(t *testing.T) {
	fs, _ := NewFs()
	tmpDir := fs.Path()

	// Copy multiple binaries
	binaries := []string{"/bin/ls", "/bin/cat", "/bin/echo"}
	checksums := make(map[string]string)
	
	for _, bin := range binaries {
		if _, err := os.Stat(bin); os.IsNotExist(err) {
			continue
		}
		
		name := filepath.Base(bin)
		dst := filepath.Join(tmpDir, name)
		err := copyFile(bin, dst)
		require.NoError(t, err)
		
		checksum, err := calculateChecksum(dst)
		require.NoError(t, err)
		checksums[name] = checksum
	}

	if len(checksums) == 0 {
		t.Skip("No binaries found for testing")
	}

	// Create archive with all binaries
	var files []string
	for name := range checksums {
		files = append(files, name)
	}
	
	archiveInfo, err := fs.CompressFiles("", files)
	require.NoError(t, err)

	// Delete all binaries
	for name := range checksums {
		err := os.Remove(filepath.Join(tmpDir, name))
		require.NoError(t, err)
	}

	// Decompress
	err = fs.DecompressFile(context.Background(), "", archiveInfo.Name())
	require.NoError(t, err)

	// Verify all checksums
	for name, originalChecksum := range checksums {
		restoredChecksum, err := calculateChecksum(filepath.Join(tmpDir, name))
		require.NoError(t, err)
		assert.Equal(t, originalChecksum, restoredChecksum, 
			"Binary %s should be identical after restore", name)
		
		// Test execution
		cmd := exec.Command(filepath.Join(tmpDir, name), "--version")
		_, err = cmd.CombinedOutput()
		// Some binaries might not support --version, that's ok
		// The important thing is they don't segfault or have missing symbols
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				// Exit code 1 or 2 is usually "bad argument", which is fine
				// Exit code > 128 usually means signal (segfault, etc), which is bad
				assert.Less(t, exitErr.ExitCode(), 128, 
					"Binary %s crashed with signal", name)
			}
		}
	}
}

// Helper function to copy a file
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	// Get source file permissions
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

// Helper function to calculate SHA256 checksum
func calculateChecksum(path string) (string, error) {
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

// TestCompressionConsistency runs the compress/decompress cycle multiple times
func TestCompressionConsistency(t *testing.T) {
	// Use a simple binary that's guaranteed to exist
	testBinary := "/bin/echo"
	if _, err := os.Stat(testBinary); os.IsNotExist(err) {
		t.Skip("Test binary not found")
	}

	fs, _ := NewFs()
	tmpDir := fs.Path()

	// Copy binary
	binaryName := "test-echo"
	testPath := filepath.Join(tmpDir, binaryName)
	err := copyFile(testBinary, testPath)
	require.NoError(t, err)

	originalChecksum, err := calculateChecksum(testPath)
	require.NoError(t, err)

	// Run multiple compress/decompress cycles
	for i := 0; i < 5; i++ {
		t.Logf("Cycle %d", i+1)
		
		// Compress
		archiveInfo, err := fs.CompressFiles("", []string{binaryName})
		require.NoError(t, err)
		
		// Delete
		err = os.Remove(testPath)
		require.NoError(t, err)
		
		// Decompress
		err = fs.DecompressFile(context.Background(), "", archiveInfo.Name())
		require.NoError(t, err)
		
		// Verify checksum
		checksum, err := calculateChecksum(testPath)
		require.NoError(t, err)
		assert.Equal(t, originalChecksum, checksum, 
			"Checksum should remain consistent after cycle %d", i+1)
		
		// Test execution
		cmd := exec.Command(testPath, "test", fmt.Sprintf("cycle-%d", i+1))
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "Binary should execute after cycle %d", i+1)
		assert.Contains(t, string(output), fmt.Sprintf("cycle-%d", i+1))
		
		// Clean up archive for next cycle
		os.Remove(filepath.Join(tmpDir, archiveInfo.Name()))
	}
}