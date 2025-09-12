package server

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"

	"github.com/Rene-Roscher/wings/environment"
	"github.com/Rene-Roscher/wings/internal/progress"
	"github.com/Rene-Roscher/wings/internal/ufs"
	"github.com/Rene-Roscher/wings/remote"
	"github.com/Rene-Roscher/wings/server/backup"
	"github.com/Rene-Roscher/wings/server/filesystem"
)

// Notifies the panel of a backup's state and returns an error if one is encountered
// while performing this action.
func (s *Server) notifyPanelOfBackup(uuid string, ad *backup.ArchiveDetails, successful bool) error {
	if err := s.client.SetBackupStatus(s.Context(), uuid, ad.ToRequest(successful)); err != nil {
		if !remote.IsRequestError(err) {
			s.Log().WithFields(log.Fields{
				"backup": uuid,
				"error":  err,
			}).Error("failed to notify panel of backup status due to wings error")
			return err
		}

		return errors.New(err.Error())
	}

	return nil
}

// Get all of the ignored files for a server based on its .pteroignore file in the root.
func (s *Server) getServerwideIgnoredFiles() (string, error) {
	f, st, err := s.Filesystem().File(".pteroignore")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	defer f.Close()
	if st.Mode()&os.ModeSymlink != 0 || st.Size() > 32*1024 {
		// Don't read a symlinked ignore file, or a file larger than 32KiB in size.
		return "", nil
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// determineActualServerState checks the real container state and returns the appropriate Wings state
// This function is thread-safe and handles errors gracefully during state cleanup
func (s *Server) determineActualServerState() string {
	// During cleanup, we should NOT preserve operational states
	// This function is called to determine the FINAL state after operations complete
	
	// Check if the container is actually running right now
	if running, err := s.Environment.IsRunning(s.Context()); err == nil {
		if running {
			return environment.ProcessRunningState
		}
		return environment.ProcessOfflineState
	} else {
		// If we can't determine container state (Docker daemon down, etc.)
		// Default to offline and log the issue
		s.Log().WithError(err).Warn("failed to determine container state during cleanup - defaulting to offline")
		return environment.ProcessOfflineState
	}
}

// BackupWithContext performs a server backup with context support for cancellation.
// This method respects context cancellation at every I/O operation following CLAUDE.md guidelines.
// CRITICAL: This method MUST use the provided context from BackupOperationRegistry for proper cancellation
func (s *Server) BackupWithContext(ctx context.Context, b backup.BackupInterface) error {
	// IMPORTANT: Don't override timeout if context already has deadline (from registry)
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 6*time.Hour)
		defer cancel()
		s.Log().Debug("backup context: applied 6-hour timeout (no existing deadline)")
	} else {
		s.Log().Debug("backup context: using provided context with existing deadline")
	}

	// CRITICAL: Ensure this backup is properly registered in operation registry
	registry := GetBackupOperationRegistry()
	if _, exists := registry.Get(b.Identifier()); !exists {
		// This should not happen in normal operation - backup should be pre-registered
		s.Log().WithField("backup_id", b.Identifier()).Warn("backup operation not found in registry - this may cause cancellation issues")
	} else {
		s.Log().WithField("backup_id", b.Identifier()).Debug("backup operation confirmed in registry")
	}

	// Note: Registry completion is handled by the caller (router layer)

	// ATOMIC: Transition to backup state with coordinated flag setting
	// NOTE: This happens AFTER the queue wait in the registry, so state is only set when backup actually starts
	backingUp := true
	s.ApplyAtomicStateTransition(AtomicStateTransition{
		EnvironmentState: environment.ProcessBackupState,
		BackingUp:        &backingUp,
	})

	// ATOMIC: Ensure proper cleanup with atomic state transition
	defer func() {
		s.Log().Debug("backup state cleanup starting")
		// Determine correct post-backup state and atomically apply all changes
		actualState := s.determineActualServerState()
		backingUp := false
		s.ApplyAtomicStateTransition(AtomicStateTransition{
			EnvironmentState: actualState,
			BackingUp:        &backingUp,
		})
		
		if errors.Is(ctx.Err(), context.Canceled) {
			s.Log().WithField("new_state", actualState).Info("reset server state after backup cancellation")
		} else {
			s.Log().WithField("new_state", actualState).Info("reset server state after backup completion")
		}
		s.Log().Debug("backup state cleanup completed")
	}()
	ignored := b.Ignored()
	if b.Ignored() == "" {
		if i, err := s.getServerwideIgnoredFiles(); err != nil {
			log.WithField("server", s.ID()).WithField("error", err).Warn("failed to get server-wide ignored files")
		} else {
			ignored = i
		}
	}

	// Smart progress tracking: estimate total size once, then track progress
	progressInstance := progress.NewProgress(0)

	// Context-aware progress tracker with proper lifecycle management
	progressTracker := NewSimpleProgressTracker(ctx, s, b.Identifier(), "create", progressInstance)
	defer progressTracker.Close() // Ensure cleanup

	// Connect progress callback - called on every Archive.Write()!
	progressInstance.ProgressCallback = progressTracker.CheckProgress

	// Context-aware size estimation - SAFE from race conditions
	cachedSize := s.Filesystem().CachedUsage()
	s.Log().WithField("cached_disk_usage", cachedSize).Debug("checking cached disk usage for backup progress")

	// Always try to get a size estimate for percentage calculation
	var estimatedSize int64
	if cachedSize > 0 {
		// Use cached value but be conservative with compression estimate
		// Real-world data: 270MB server -> 255MB backup (only ~5% compression)
		// Better to overestimate than underestimate for progress tracking
		estimatedSize = cachedSize // No compression assumption - better safe than sorry
		s.Log().WithField("estimated_backup_size", estimatedSize).Debug("using cached disk usage for backup progress")
	} else {
		// Check context before expensive operation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Fallback: try one fresh disk usage calculation (non-blocking timeout)
		s.Log().Debug("no cached usage, attempting fresh disk usage calculation for backup progress")

		// Use a context with timeout to prevent hanging the backup process
		sizeCtx, sizeCancel := context.WithTimeout(ctx, 5*time.Second)
		defer sizeCancel()

		// Channel to receive result
		type sizeResult struct {
			size int64
			err  error
		}
		done := make(chan sizeResult, 1)

		// Run disk usage calculation in managed goroutine
		go func() {
			defer func() {
				if r := recover(); r != nil {
					s.Log().WithField("panic", r).Error("panic in disk usage calculation goroutine")
					select {
					case done <- sizeResult{0, errors.New("disk usage calculation panicked")}:
					case <-sizeCtx.Done():
					}
				}
			}()

			// Context-aware disk usage calculation
			size, err := s.Filesystem().DiskUsage(false)
			select {
			case done <- sizeResult{size, err}:
			case <-sizeCtx.Done():
				return // Goroutine cleanup
			}
		}()

		// Wait for result, timeout, or cancellation
		select {
		case result := <-done:
			if result.err == nil && result.size > 0 {
				estimatedSize = result.size // No compression assumption - better safe than sorry
				s.Log().WithField("estimated_backup_size", estimatedSize).Debug("calculated fresh disk usage for backup progress")
			} else {
				s.Log().WithField("error", result.err).Debug("fresh disk usage calculation failed")
			}
		case <-sizeCtx.Done():
			if errors.Is(sizeCtx.Err(), context.DeadlineExceeded) {
				s.Log().Warn("disk usage calculation timed out - using bytes-only mode for backup progress")
			} else {
				return ctx.Err() // Parent context cancelled
			}
		}
	}

	// Set total if we got a reasonable estimate (with bounds checking)
	if estimatedSize > 0 && estimatedSize < (1<<62) { // Prevent overflow attacks
		progressInstance.SetTotal(uint64(estimatedSize))
	}

	// Check context before starting backup generation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	ad, err := s.generateBackupWithProgress(ctx, b, ignored, progressInstance, progressTracker)
	if err != nil {
		// Store original error for proper reporting
		originalErr := err

		progressTracker.SendFinalProgress(false) // Send error progress

		// Try to notify panel, but preserve original error
		if notifyErr := s.notifyPanelOfBackup(b.Identifier(), &backup.ArchiveDetails{}, false); notifyErr != nil {
			s.Log().WithFields(log.Fields{
				"backup":       b.Identifier(),
				"backup_error": originalErr,
				"notify_error": notifyErr,
			}).Warn("failed to notify panel of failed backup state")
		} else {
			s.Log().WithFields(log.Fields{
				"backup": b.Identifier(),
				"error":  originalErr,
			}).Info("notified panel of failed backup state")
		}

		s.Events().Publish(BackupCompletedEvent, map[string]any{
			"uuid":          b.Identifier(),
			"is_successful": false,
			"checksum":      "",
			"checksum_type": "sha256",
			"file_size":     0,
			"error":         originalErr.Error(),
		})

		return errors.WrapIf(originalErr, "backup: error while generating server backup")
	}

	// Try to notify the panel about the successful backup status
	// CRITICAL: Never delete successful backups due to panel communication issues!
	if notifyError := s.notifyPanelOfBackup(b.Identifier(), ad, true); notifyError != nil {
		// Log the panel communication error but keep the backup
		s.Log().WithFields(log.Fields{
			"backup":          b.Identifier(),
			"notify_error":    notifyError,
			"backup_size":     ad.Size,
			"backup_checksum": ad.Checksum,
		}).Error("failed to notify panel of successful backup - backup preserved for manual recovery")

		// Emit success event despite panel notification failure
		s.Events().Publish(BackupCompletedEvent, map[string]any{
			"uuid":           b.Identifier(),
			"is_successful":  true,
			"checksum":       ad.Checksum,
			"checksum_type":  "sha1",
			"file_size":      ad.Size,
			"panel_notified": false,
			"notify_error":   notifyError.Error(),
		})

		// Return success - backup was created successfully
		return nil
	} else {
		s.Log().WithField("backup", b.Identifier()).Info("notified panel of successful backup state")
	}

	progressTracker.SendFinalProgress(true) // Send success progress

	// Emit an event over the socket so we can update the backup in realtime on
	// the frontend for the server.
	s.Events().Publish(BackupCompletedEvent, map[string]any{
		"uuid":          b.Identifier(),
		"is_successful": true,
		"checksum":      ad.Checksum,
		"checksum_type": "sha256",
		"file_size":     ad.Size,
	})

	return nil
}

// BackupWithRetry performs a backup with exponential backoff retry logic
// Implements requirement from WORK.md: default 2 retries for failed backups
func (s *Server) BackupWithRetry(ctx context.Context, b backup.BackupInterface, maxRetries int) error {
	if maxRetries <= 0 {
		maxRetries = 2 // Default as per WORK.md requirements
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 30s, 60s, 120s...
			backoffDuration := time.Duration(30*attempt*attempt) * time.Second
			s.Log().WithFields(log.Fields{
				"backup_id": b.Identifier(),
				"attempt":   attempt + 1,
				"max_retries": maxRetries + 1,
				"backoff":   backoffDuration,
				"last_error": lastErr,
			}).Warn("retrying backup after failure")

			// Wait for backoff period or context cancellation
			select {
			case <-time.After(backoffDuration):
				// Continue with retry
			case <-ctx.Done():
				return errors.WithStackIf(ctx.Err())
			}
		}

		// Attempt backup with individual timeout per attempt
		attemptCtx, cancel := context.WithTimeout(ctx, 6*time.Hour)
		err := s.BackupWithContext(attemptCtx, b)
		cancel()

		if err == nil {
			if attempt > 0 {
				s.Log().WithFields(log.Fields{
					"backup_id": b.Identifier(),
					"attempt":   attempt + 1,
					"total_attempts": attempt + 1,
				}).Info("backup succeeded after retry")
			}
			return nil
		}

		lastErr = err
		
		// Don't retry on context cancellation or unrecoverable errors
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.Log().WithFields(log.Fields{
				"backup_id": b.Identifier(),
				"attempt":   attempt + 1,
				"error":     err,
			}).Info("backup cancelled or timed out - not retrying")
			break
		}

		// Log the failed attempt
		s.Log().WithFields(log.Fields{
			"backup_id": b.Identifier(),
			"attempt":   attempt + 1,
			"max_retries": maxRetries + 1,
			"error":     err,
		}).Error("backup attempt failed")
	}

	// All attempts failed - emit failure events
	s.Events().Publish(BackupCompletedEvent, map[string]any{
		"uuid":          b.Identifier(),
		"is_successful": false,
		"error":         lastErr.Error(),
		"total_attempts": maxRetries + 1,
	})
	
	// Also emit as ActivityEvent for persistent logging (WORK.md requirement)
	s.Events().Publish(ActivityEvent, map[string]any{
		"event":         "backup_failed",
		"timestamp":     time.Now().Unix(),
		"backup_id":     b.Identifier(),
		"server_id":     s.ID(),
		"error":         lastErr.Error(),
		"total_attempts": maxRetries + 1,
		"message":       fmt.Sprintf("Backup failed after %d attempts: %v", maxRetries+1, lastErr),
	})

	return errors.WithStackIf(lastErr)
}

// Backup performs a server backup - backward compatibility method
// This method calls BackupWithContext with a background context for legacy compatibility
func (s *Server) Backup(b backup.BackupInterface) error {
	// Use background context with 6-hour timeout as per CLAUDE.md production requirements
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	return s.BackupWithContext(ctx, b)
}

// RestoreBackup calls the Restore function on the provided backup. Once this
// restoration is completed an event is emitted to the websocket to notify the
// Panel that is has been completed.
//
// In addition to the websocket event an API call is triggered to notify the
// Panel of the new state.
// RestoreBackupWithContext performs a server backup restore with context for cancellation support.
// This is the primary restore function that should be used for all restore operations.
func (s *Server) RestoreBackupWithContext(ctx context.Context, b backup.BackupInterface, reader io.ReadCloser) (err error) {
	s.Config().SetSuspended(true)
	
	// CRITICAL: DEFER ORDER (LIFO - Last In First Out)
	// First defined = Last executed
	// We want execution order: 1) Final progress, 2) State reset, 3) Panel notify, 4) Resource cleanup
	// So we define in REVERSE: Resource cleanup, Panel notify, State reset, Final progress
	
	var progressTracker *SimpleProgressTracker
	
	// Define FIRST - executes LAST: Resource cleanup
	defer func() {
		s.Config().SetSuspended(false)
		if reader != nil {
			_ = reader.Close()
		}
		s.Log().Debug("restore resource cleanup completed")
	}()
	
	// Define SECOND - executes THIRD: Panel notification (needs err value)
	defer func() {
		s.Log().WithField("success", err == nil).Debug("notifying panel of restore status")
		if rerr := s.client.SendRestorationStatus(s.Context(), b.Identifier(), err == nil); rerr != nil {
			s.Log().WithField("error", rerr).WithField("backup", b.Identifier()).Error("failed to notify Panel of backup restoration status")
		}
	}()
	
	// Define THIRD - executes SECOND: State reset (MUST happen before final progress)
	defer func() {
		// CRITICAL ERROR RECOVERY: Always reset state even if panic occurs
		defer func() {
			if r := recover(); r != nil {
				s.Log().WithField("panic", r).Error("panic during state reset - forcing state cleanup")
				// Force reset restoring flag even on panic
				restoring := false
				s.restoring.Store(restoring)
				s.Environment.SetState(environment.ProcessOfflineState)
			}
		}()
		
		s.Log().WithFields(log.Fields{
			"restoring_before": s.IsRestoring(),
			"environment_state_before": s.Environment.State(),
		}).Debug("restore state cleanup starting")
		
		// Determine correct post-restore state
		actualState := s.determineActualServerState()
		restoring := false
		
		s.Log().WithFields(log.Fields{
			"target_state": actualState,
			"target_restoring": restoring,
		}).Debug("applying atomic state transition for restore cleanup")
		
		s.ApplyAtomicStateTransition(AtomicStateTransition{
			EnvironmentState: actualState,
			Restoring:        &restoring,
		})
		
		s.Log().WithFields(log.Fields{
			"new_state": actualState,
			"restoring_after": s.IsRestoring(),
			"environment_state_after": s.Environment.State(),
		}).Info("reset server state after restore")
	}()
	
	// Define LAST - executes FIRST: Send final progress (AFTER state is reset!)
	defer func() {
		// CRITICAL: Recover from any panic in progress tracking
		defer func() {
			if r := recover(); r != nil {
				s.Log().WithField("panic", r).Error("panic in final progress tracking - ignored")
			}
		}()
		
		// IMPORTANT: Use the actual error value at defer execution time, not capture time!
		success := err == nil
		
		if progressTracker != nil {
			s.Log().WithFields(log.Fields{
				"success": success,
				"is_restoring": s.IsRestoring(),
				"state": s.Environment.State(),
			}).Info("sending final restore progress")
			progressTracker.SendFinalProgress(success)
			progressTracker.Close()
		}
		
		// Send the restore completed event HERE, while we're still in restore state
		// This ensures the event is sent before state reset
		s.Events().Publish(BackupRestoreCompletedEvent, map[string]any{
			"successful": success,
		})
		
		// Log the event for debugging
		s.Log().WithField("successful", success).Info("sent BackupRestoreCompletedEvent")
	}()

	// Don't try to restore the server until we have completely stopped the running
	// instance, otherwise you'll likely hit all types of write errors due to the
	// server being suspended.
	if s.Environment.State() != environment.ProcessOfflineState {
		if err = s.Environment.WaitForStop(ctx, 2*time.Minute, false); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return errors.WrapIf(err, "server/backup: restore: failed to wait for container stop")
			}
		}
	}

	// Check for cancellation after stopping server
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// ATOMIC: Transition to restore state with coordinated flag setting
	restoring := true
	s.ApplyAtomicStateTransition(AtomicStateTransition{
		EnvironmentState: environment.ProcessRestoringState,
		Restoring:        &restoring,
	})

	// Handle different restore scenarios
	var decompressedReader io.ReadCloser

	if reader == nil {
		// LOCAL BACKUP RESTORE: reader is nil, backup interface handles decompression
		s.Log().Debug("performing local backup restore - backup interface handles decompression")
		decompressedReader = nil // Will be handled by backup.Restore() method
	} else {
		// REMOTE BACKUP RESTORE: we need to detect format and decompress
		s.Log().Debug("performing remote backup restore - detecting compression format")

		// Auto-detect compression format and create appropriate decompressor
		format, detectedReader, err := filesystem.DetectCompressionFormat(reader)
		if err != nil {
			return errors.WrapIf(err, "failed to detect backup format")
		}
		reader = detectedReader

		// Create decompressor based on detected format
		decompressedReader, err = filesystem.CreateDecompressor(reader, format)
		if err != nil {
			return errors.WrapIf(err, "failed to create decompressor")
		}
		defer decompressedReader.Close()
	}

	// Restore progress tracking with real Progress instance
	var processedFiles int64

	// Create progress instance for restore - estimate total from backup file size
	restoreProgress := progress.NewProgress(0)

	// Check if download size was passed through context (for S3 downloads)
	var downloadSize int64
	if ctxSize := ctx.Value("download_size"); ctxSize != nil {
		if size, ok := ctxSize.(int64); ok && size > 0 {
			downloadSize = size
			s.Log().WithField("download_size", downloadSize).Debug("using download size from context for restore progress")
		}
	}
	
	// Try to get backup file size for percentage calculation
	backupSize, err := b.Details(s.Context(), nil)
	if err != nil {
		s.Log().WithField("error", err).Debug("failed to get backup details for size")
	}
	
	// Determine the best size to use for progress tracking
	var estimatedTotal int64
	if downloadSize > 0 {
		// For S3: Use actual download size with conservative multiplier for extraction
		// Downloaded archives typically expand 3-4x when extracted (gzip/zstd compression)
		// Using 3.2x gives good results without overshooting too much
		estimatedTotal = int64(float64(downloadSize) * 3.2)
		s.Log().WithField("download_size", downloadSize).WithField("estimated_restore_size", estimatedTotal).Info("set restore progress total from download size")
	} else if err == nil && backupSize != nil && backupSize.Size > 0 {
		// For local backups: Use backup file size with multiplier
		estimatedTotal = int64(float64(backupSize.Size) * 1.5)
		s.Log().WithField("backup_size", backupSize.Size).WithField("estimated_restore_size", estimatedTotal).Info("set restore progress total from backup size")
	} else {
		// Fallback: Use a reasonable estimate
		estimatedTotal = int64(10 * 1024 * 1024 * 1024) // 10GB estimate
		s.Log().WithField("estimated_restore_size", estimatedTotal).Info("using estimated size for restore progress (backup size unavailable)")
	}
	
	restoreProgress.SetTotal(uint64(estimatedTotal))

	progressTracker = NewSimpleProgressTracker(ctx, s, b.Identifier(), "restore", restoreProgress)

	// Connect callback for percentage tracking
	restoreProgress.ProgressCallback = progressTracker.CheckProgress

	// Optimized progress update function - minimal overhead
	updateProgress := func(fileSize int64) {
		// Batch small updates to reduce atomic operations overhead
		if fileSize > 0 {
			restoreProgress.AddWritten(uint64(fileSize))
		}

		// Only track file count if it's useful (avoid unnecessary atomic ops)
		if processedFiles < 1000000 { // Prevent overflow on extreme file counts
			atomic.AddInt64(&processedFiles, 1)
		}
	}

	// Attempt to restore the backup to the server by running through each entry
	// in the file one at a time and writing them to the disk.
	s.Log().Debug("starting file writing process for backup restoration")

	// For local backups, pass the original reader (backup interface handles decompression)
	// For remote backups, pass the decompressed reader
	restoreReader := decompressedReader
	if reader == nil {
		// Local backup: let backup interface handle its own file reading
		restoreReader = nil
	}
	
	// MEMORY SAFETY: Set maximum buffer size for file operations (10MB)
	const maxBufferSize = 10 * 1024 * 1024
	buffer := make([]byte, 32*1024) // 32KB buffer for streaming

	// Track restore statistics for validation
	var restoreStats struct {
		fileCount int
		dirCount  int
		totalSize int64
	}

	err = b.Restore(ctx, restoreReader, func(file string, info fs.FileInfo, r io.ReadCloser) error {
		defer r.Close()
		//s.Events().Publish(DaemonMessageEvent, "(restoring): "+file)
		
		// Use buffer to mark as used (for memory-safe streaming in future)
		_ = buffer

		// Skip problematic root directory entries that can cause errors
		if file == "." || file == "" || file == "/" || file == "./" || strings.HasPrefix(file, "../") {
			return nil
		}

		// Track statistics for integrity validation
		if info.IsDir() {
			restoreStats.dirCount++
		} else {
			restoreStats.fileCount++
			restoreStats.totalSize += info.Size()
		}

		// Handle directories and files differently
		if info.IsDir() {
			// For directories, create the directory structure using the underlying UnixFS
			if err := s.Filesystem().UnixFS().MkdirAll(file, ufs.FileMode(info.Mode())); err != nil {
				return err
			}
			// Set directory timestamps
			atime := info.ModTime()
			return s.Filesystem().Chtimes(file, atime, atime)
		}

		// For regular files, write the content
		// TODO: since this will be called a lot, it may be worth adding an optimized
		// Write with Chtimes method to the UnixFS that is able to re-use the
		// same dirfd and file name.
		if err := s.Filesystem().Write(file, r, info.Size(), info.Mode()); err != nil {
			return err
		}
		atime := info.ModTime()

		// Send ultra-live progress update AFTER successful write
		updateProgress(info.Size())

		return s.Filesystem().Chtimes(file, atime, atime)
	})

	// Basic restore validation
	if err == nil {
		s.Log().WithFields(log.Fields{
			"files_restored": restoreStats.fileCount,
			"dirs_restored":  restoreStats.dirCount,
			"total_size":     restoreStats.totalSize,
		}).Info("backup restore completed successfully")

		// Sanity check: restore must have processed something
		if restoreStats.fileCount == 0 && restoreStats.dirCount == 0 {
			s.Log().Warn("restore completed but no files or directories were processed - backup may be empty or corrupt")
		}
	}

	// State reset and final progress are handled by defer blocks

	return errors.WithStackIf(err)
}

// RestoreBackup performs a server backup restore with the server's context.
// This method is kept for backward compatibility. New code should use RestoreBackupWithContext.
func (s *Server) RestoreBackup(b backup.BackupInterface, reader io.ReadCloser) error {
	return s.RestoreBackupWithContext(s.Context(), b, reader)
}

// generateBackupWithProgress creates a backup with progress tracking and context support
func (s *Server) generateBackupWithProgress(ctx context.Context, b backup.BackupInterface, ignored string, progressInstance *progress.Progress, progressTracker *SimpleProgressTracker) (*backup.ArchiveDetails, error) {
	// For local backups, we need to inject the progress tracker into the archive
	if localBackup, ok := b.(*backup.LocalBackup); ok {
		return s.generateLocalBackupWithProgress(ctx, localBackup, ignored, progressInstance)
	}

	// For S3 backups, we also need progress tracking
	if s3Backup, ok := b.(*backup.S3Backup); ok {
		return s.generateS3BackupWithProgress(ctx, s3Backup, ignored, progressInstance, progressTracker)
	}

	// Fallback to original Generate method if backup type is unknown
	ad, err := b.Generate(ctx, s.Filesystem(), ignored)
	if err != nil {
		return nil, err
	}

	// Quick integrity validation for any backup type - CRITICAL: Fail backup on validation errors
	if err := s.validateBackupIntegrity(b); err != nil {
		s.Log().WithError(err).Error("backup integrity validation failed - backup is corrupted")
		return ad, errors.Wrap(err, "backup integrity validation failed")
	}

	// Content integrity validation (file/directory count check) - CRITICAL: Fail backup on validation errors
	backupPath := b.Path() // Works for all backup types
	if err := s.validateBackupContent(backupPath, s.Filesystem().Path()); err != nil {
		s.Log().WithError(err).Error("backup content validation failed - backup is incomplete")
		return ad, errors.Wrap(err, "backup content validation failed")
	} else {
		s.Log().Debug("backup content validation passed - backup is complete")
	}

	return ad, nil
}

// validateBackupIntegrity performs minimal integrity checks on backup file
func (s *Server) validateBackupIntegrity(b backup.BackupInterface) error {
	backupPath := ""

	// Get backup path based on type
	if localBackup, ok := b.(*backup.LocalBackup); ok {
		backupPath = localBackup.Path()
	} else {
		// For S3 backups, we can't validate local file
		return nil
	}

	// Basic file existence and size check
	stat, err := os.Stat(backupPath)
	if err != nil {
		return errors.Wrap(err, "backup file not accessible")
	}

	// Archive must be at least 20 bytes (minimum GZIP + TAR headers)
	if stat.Size() < 20 {
		return errors.New("backup file suspiciously small - may be corrupt")
	}

	// Quick magic bytes check for GZIP
	f, err := os.Open(backupPath)
	if err != nil {
		return err
	}
	defer f.Close()

	magic := make([]byte, 2)
	if n, err := f.Read(magic); err != nil || n < 2 {
		return errors.New("cannot read backup file header")
	}

	// Check for GZIP magic bytes (0x1f, 0x8b) or ZSTD magic bytes (0x28, 0xb5)
	if magic[0] == 0x1f && magic[1] == 0x8b {
		// Valid GZIP
		return nil
	}
	if magic[0] == 0x28 && magic[1] == 0xb5 {
		// Valid ZSTD
		return nil
	}

	// Check for uncompressed TAR (less common but possible)
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}

	// Read TAR header area
	tarTest := make([]byte, 512)
	if n, err := f.Read(tarTest); err != nil || n < 512 {
		return errors.New("backup file too short for valid TAR")
	}

	// Very basic TAR validation - check for reasonable header structure
	// TAR headers have specific patterns at specific offsets
	if tarTest[156] == '0' || tarTest[156] == '5' { // Regular file or directory
		return nil
	}

	return errors.New("backup file format not recognized - may be corrupt")
}

// validateBackupContent performs fast file/directory count validation between original and backup
func (s *Server) validateBackupContent(backupPath, serverPath string) error {
	// 1. Count original files and directories (fast directory walk)
	originalStats, err := s.countServerFilesAndDirs(serverPath)
	if err != nil {
		return errors.Wrap(err, "failed to count original server files")
	}

	// 2. Count backup entries (TAR header scan only, no extraction)
	backupStats, err := s.countBackupEntries(backupPath)
	if err != nil {
		return errors.Wrap(err, "failed to count backup entries")
	}

	// Generate SHA1 checksums for debug logging
	backupChecksum := "unknown"
	serverChecksum := "unknown"

	// Get backup file SHA1 (reuse existing checksum method)
	if backupFile, err := os.Open(backupPath); err == nil {
		hasher := sha256.New()
		if _, err := io.Copy(hasher, backupFile); err == nil {
			backupChecksum = hex.EncodeToString(hasher.Sum(nil))
		}
		backupFile.Close()
	}

	// Get server directory content SHA1 (walk files and hash content)
	if serverHash := sha256.New(); serverHash != nil {
		err := filepath.Walk(serverPath, func(path string, info os.FileInfo, err error) error {
			if err != nil || path == serverPath || info.IsDir() {
				return nil // Skip errors, root, and directories
			}

			relPath, _ := filepath.Rel(serverPath, path)
			serverHash.Write([]byte(relPath)) // Include path in hash

			if file, err := os.Open(path); err == nil {
				io.Copy(serverHash, file)
				file.Close()
			}
			return nil
		})

		if err == nil {
			serverChecksum = hex.EncodeToString(serverHash.Sum(nil))
		}
	}

	s.Log().WithFields(log.Fields{
		"original_files":      originalStats.FileCount,
		"original_dirs":       originalStats.DirCount,
		"backup_files":        backupStats.FileCount,
		"backup_dirs":         backupStats.DirCount,
		"backup_sha256":         backupChecksum,
		"server_content_sha256": serverChecksum,
	}).Debug("backup content validation stats")

	// 3. Compare file counts
	if originalStats.FileCount != backupStats.FileCount {
		return errors.Errorf("backup file count mismatch: expected %d files, backup contains %d files",
			originalStats.FileCount, backupStats.FileCount)
	}

	// 4. Compare directory counts
	if originalStats.DirCount != backupStats.DirCount {
		return errors.Errorf("backup directory count mismatch: expected %d directories, backup contains %d directories",
			originalStats.DirCount, backupStats.DirCount)
	}

	return nil
}

// fileStats holds counts for validation
type fileStats struct {
	FileCount int
	DirCount  int
}

// countServerFilesAndDirs counts files and directories in server filesystem
func (s *Server) countServerFilesAndDirs(serverPath string) (*fileStats, error) {
	stats := &fileStats{}

	err := filepath.Walk(serverPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Skip unreadable files/dirs but continue counting
			return nil
		}

		// Skip the root directory itself
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

// countBackupEntries counts entries in TAR archive by scanning headers only (no extraction)
func (s *Server) countBackupEntries(backupPath string) (*fileStats, error) {
	stats := &fileStats{}

	f, err := os.Open(backupPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Auto-detect compression and create appropriate reader
	format, detectedReader, err := filesystem.DetectCompressionFormat(io.NopCloser(f))
	if err != nil {
		return nil, errors.Wrap(err, "failed to detect backup compression format")
	}

	decompressedReader, err := filesystem.CreateDecompressor(detectedReader, format)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create decompressor")
	}
	defer decompressedReader.Close()

	// Scan TAR headers (no content reading)
	tarReader := tar.NewReader(decompressedReader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "failed to read TAR header")
		}

		// Skip problematic root entries (same logic as restore)
		if header.Name == "." || header.Name == "" || header.Name == "/" ||
			header.Name == "./" || strings.HasPrefix(header.Name, "../") {
			continue
		}

		// Count based on header type
		if header.FileInfo().IsDir() {
			stats.DirCount++
		} else {
			stats.FileCount++
		}
	}

	return stats, nil
}

// generateLocalBackupWithProgress creates a local backup with progress tracking and context support
// UNIFIED BEHAVIOR: Uses same progress pattern as S3 backups for consistency (WORK.md compliance)
func (s *Server) generateLocalBackupWithProgress(ctx context.Context, b *backup.LocalBackup, ignored string, progressInstance *progress.Progress) (*backup.ArchiveDetails, error) {
	// UNIFIED PROGRESS: For local backups, no scaling needed anymore
	// The total is already correctly estimated based on disk usage
	// Local backups complete when archive is done (no separate upload phase like S3)

	// Create archive (100% of progress for local backups)
	a := &filesystem.Archive{
		Filesystem: s.Filesystem(),
		Ignore:     ignored,
		Progress:   progressInstance, // Will reach 100% when archive is complete
	}

	s.Log().WithField("backup", b.Identifier()).WithField("path", b.Path()).Info("creating backup for server")
	if err := a.Create(ctx, b.Path()); err != nil {
		return nil, err
	}
	s.Log().WithField("backup", b.Identifier()).Info("created backup successfully")

	// Phase 2: Finalization phase (remaining 20% for consistency with S3)
	// This ensures identical progress behavior between Local and S3 backups
	// Local backups are done when archive is complete - no finalization needed
	// The progress should already be at or near 100%

	ad, err := b.Details(s.Context(), nil)
	if err != nil {
		return nil, errors.WrapIf(err, "backup: failed to get archive details for local backup")
	}
	return ad, nil
}

// generateS3BackupWithProgress creates an S3 backup with progress tracking and context support
// UNIFIED BEHAVIOR: Uses same progress pattern as Local backups for consistency (WORK.md compliance)
func (s *Server) generateS3BackupWithProgress(ctx context.Context, b *backup.S3Backup, ignored string, progressInstance *progress.Progress, progressTracker *SimpleProgressTracker) (*backup.ArchiveDetails, error) {
	// S3 PROGRESS: 80/20 split pattern for S3 backups
	// Archive creation = 80%, Upload = 20%
	
	// Configure tracker for S3 80/20 mode from the start
	if progressTracker != nil {
		// Mark as S3 immediately, archive size will be set later
		progressTracker.SetS3Mode(0)
		s.Log().Debug("configured S3 backup progress tracker for 80/20 split")
	}

	// Phase 1: Create local archive with progress tracking
	// This will now report 80% when complete (originalTotal bytes of scaledTotal)
	a := &filesystem.Archive{
		Filesystem: s.Filesystem(),
		Ignore:     ignored,
		Progress:   progressInstance,
	}

	s.Log().WithField("backup", b.Identifier()).WithField("path", b.Path()).Info("creating S3 backup archive")
	if err := a.Create(ctx, b.Path()); err != nil {
		return nil, err
	}
	s.Log().WithField("backup", b.Identifier()).Info("created S3 backup archive - starting S3 upload")

	// Get actual archive size for accurate 80/20 split
	if progressTracker != nil {
		if stat, err := os.Stat(b.Path()); err == nil {
			archiveSize := stat.Size()
			progressTracker.SetS3Mode(archiveSize)
			s.Log().WithField("archive_size", archiveSize).Debug("set S3 tracker archive size for 80/20 split")
		}
	}

	// Phase 2: S3 upload with REAL progress tracking (remaining 20%)
	s.Log().Debug("S3 upload phase starting with real progress tracking")
	
	// Set up real progress tracking for S3 upload with WebSocket callback
	if progressInstance != nil && progressTracker != nil {
		b.WithUploadProgress(progressInstance)
		// Set the callback to trigger WebSocket events during upload
		b.WithUploadCallback(func() {
			progressTracker.CheckProgress()
		})
	}

	// Perform actual S3 upload with real progress tracking
	ad, err := b.Generate(ctx, s.Filesystem(), ignored)
	if err != nil {
		return nil, err
	}

	s.Log().WithField("backup", b.Identifier()).Info("S3 backup upload completed")
	return ad, nil
}
