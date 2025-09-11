package filesystem

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/Rene-Roscher/wings/config"
)

// CreateArchiveUsingSystemTar creates a backup using the system tar command
// This bypasses all Go library issues and uses the proven system tools
func (a *Archive) CreateArchiveUsingSystemTar(ctx context.Context, dst string) error {
	if a.Filesystem == nil {
		return errors.New("filesystem: archive.Filesystem is unset")
	}

	// Determine compression flag based on file extension
	var compressionFlag string
	switch {
	case strings.HasSuffix(dst, ".tar.zst"):
		compressionFlag = "--zstd"
	case strings.HasSuffix(dst, ".tar.gz"):
		compressionFlag = "-z"
	default:
		compressionFlag = "" // No compression
	}

	// Build tar command
	args := []string{
		"-c", // Create archive
		"-f", dst, // Output file
	}
	
	if compressionFlag != "" {
		args = append(args, compressionFlag)
	}

	// Add base directory
	args = append(args, "-C", a.Filesystem.Path())
	
	// If specific files are provided, add them
	if len(a.Files) > 0 {
		for _, file := range a.Files {
			// Strip leading slash and filesystem path
			cleanFile := strings.TrimPrefix(file, a.Filesystem.Path())
			cleanFile = strings.TrimPrefix(cleanFile, "/")
			if cleanFile != "" {
				args = append(args, cleanFile)
			}
		}
	} else {
		// Archive everything in the base directory
		if a.BaseDirectory != "" {
			args = append(args, a.BaseDirectory)
		} else {
			args = append(args, ".")
		}
	}

	// Create tar command
	cmd := exec.CommandContext(ctx, "tar", args...)
	cmd.Dir = a.Filesystem.Path()
	
	// Set up ignore file if provided
	if a.Ignore != "" {
		// Write ignore patterns to exclude file
		excludeFile := filepath.Join("/tmp", fmt.Sprintf("backup-exclude-%d", os.Getpid()))
		if err := os.WriteFile(excludeFile, []byte(a.Ignore), 0600); err != nil {
			return errors.Wrap(err, "failed to write exclude file")
		}
		defer os.Remove(excludeFile)
		
		// Add exclude flag
		cmd.Args = append(cmd.Args[:2], append([]string{"--exclude-from=" + excludeFile}, cmd.Args[2:]...)...)
	}

	log.WithField("command", cmd.String()).Debug("executing system tar command")
	
	// Execute the command
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "tar command failed: %s", string(output))
	}

	return nil
}

// ExtractArchiveUsingSystemTar extracts a backup using the system tar command
func ExtractArchiveUsingSystemTar(ctx context.Context, src string, dst string) error {
	// Determine decompression flag based on file extension
	var decompressionFlag string
	switch {
	case strings.HasSuffix(src, ".tar.zst"):
		decompressionFlag = "--zstd"
	case strings.HasSuffix(src, ".tar.gz"):
		decompressionFlag = "-z"
	case strings.HasSuffix(src, ".tar.xz"):
		decompressionFlag = "-J"
	case strings.HasSuffix(src, ".tar.bz2"):
		decompressionFlag = "-j"
	default:
		decompressionFlag = "" // No decompression
	}

	// Build tar command
	args := []string{
		"-x", // Extract archive
		"-f", src, // Input file
		"-C", dst, // Extract to directory
		"--preserve-permissions", // CRITICAL: Preserve file permissions!
		"--preserve", // Preserve all attributes
	}
	
	if decompressionFlag != "" {
		args = append(args, decompressionFlag)
	}

	// Create tar command
	cmd := exec.CommandContext(ctx, "tar", args...)
	
	log.WithField("command", cmd.String()).Debug("executing system tar extract command")
	
	// Execute the command
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "tar extract failed: %s", string(output))
	}

	return nil
}

// StreamArchiveUsingSystemTar streams archive creation using system tar
func (a *Archive) StreamArchiveUsingSystemTar(ctx context.Context, w io.Writer) error {
	if a.Filesystem == nil {
		return errors.New("filesystem: archive.Filesystem is unset")
	}

	// Use zstd command for compression if needed
	var cmd *exec.Cmd
	
	// Build tar command (without compression)
	tarArgs := []string{
		"-c", // Create archive
		"-f", "-", // Output to stdout
		"-C", a.Filesystem.Path(),
	}
	
	// Add files or directory
	if len(a.Files) > 0 {
		for _, file := range a.Files {
			cleanFile := strings.TrimPrefix(file, a.Filesystem.Path())
			cleanFile = strings.TrimPrefix(cleanFile, "/")
			if cleanFile != "" {
				tarArgs = append(tarArgs, cleanFile)
			}
		}
	} else if a.BaseDirectory != "" {
		tarArgs = append(tarArgs, a.BaseDirectory)
	} else {
		tarArgs = append(tarArgs, ".")
	}

	// Check if we need compression
	if config.Get().System.Backups.Format == "zstd" {
		// Pipe tar through zstd
		tarCmd := exec.CommandContext(ctx, "tar", tarArgs...)
		tarCmd.Dir = a.Filesystem.Path()
		
		zstdCmd := exec.CommandContext(ctx, "zstd", "-c", "-T0") // -T0 uses all CPU cores
		
		// Create pipe
		pipe, err := tarCmd.StdoutPipe()
		if err != nil {
			return errors.Wrap(err, "failed to create pipe")
		}
		
		zstdCmd.Stdin = pipe
		zstdCmd.Stdout = w
		
		// Start both commands
		if err := tarCmd.Start(); err != nil {
			return errors.Wrap(err, "failed to start tar")
		}
		if err := zstdCmd.Start(); err != nil {
			return errors.Wrap(err, "failed to start zstd")
		}
		
		// Wait for both to complete
		if err := tarCmd.Wait(); err != nil {
			return errors.Wrap(err, "tar command failed")
		}
		if err := zstdCmd.Wait(); err != nil {
			return errors.Wrap(err, "zstd command failed")
		}
	} else {
		// Just tar with gzip
		tarArgs[1] = "-czf" // Add gzip compression
		cmd = exec.CommandContext(ctx, "tar", tarArgs...)
		cmd.Dir = a.Filesystem.Path()
		cmd.Stdout = w
		
		if err := cmd.Run(); err != nil {
			return errors.Wrap(err, "tar command failed")
		}
	}
	
	return nil
}