package server

import (
	"context"
	"io"
	"io/fs"
	"os"
	"sync/atomic"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/docker/docker/errdefs"

	"github.com/Rene-Roscher/wings/environment"
	"github.com/Rene-Roscher/wings/internal/progress"
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
func (s *Server) determineActualServerState() string {
	// The most reliable way: check if the container is actually running right now
	if running, err := s.Environment.IsRunning(s.Context()); err == nil && running {
		return environment.ProcessRunningState
	}
	
	return environment.ProcessOfflineState
}

// BackupWithContext performs a server backup with context support for cancellation.
// This method respects context cancellation at every I/O operation following CLAUDE.md guidelines.
func (s *Server) BackupWithContext(ctx context.Context, b backup.BackupInterface) error {
	// Set reasonable timeout if none provided - 6 hours for backup as per CLAUDE.md
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 6*time.Hour)
		defer cancel()
	}
	// Check for context cancellation before proceeding
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Set backup state to show in frontend via WebSocket
	s.Environment.SetState(environment.ProcessBackupState)
	
	// Restore actual current state when backup is done
	defer func() {
		// Determine what the server state SHOULD be right now by checking actual container state
		actualState := s.determineActualServerState()
		s.Environment.SetState(actualState)
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
	
	// Simple progress tracker without goroutines
	progressTracker := &SimpleProgressTracker{
		server:     s,
		backupID:   b.Identifier(),
		backupType: "create",
		progress:   progressInstance,
	}
	
	// Connect progress callback - called on every Archive.Write()!
	progressInstance.ProgressCallback = progressTracker.CheckProgress

	// SYNCHRONOUS size estimation - must happen BEFORE backup starts to avoid race condition
	cachedSize := s.Filesystem().CachedUsage()
	s.Log().WithField("cached_disk_usage", cachedSize).Debug("checking cached disk usage for backup progress")
	
	// Always try to get a size estimate for percentage calculation
	var estimatedSize int64
	if cachedSize > 0 {
		// Use cached value (instantaneous) with compression estimate
		estimatedSize = cachedSize / 2 // tar.gz compression ~50%
		s.Log().WithField("estimated_backup_size", estimatedSize).Debug("using cached disk usage for backup progress")
	} else {
		// Fallback: try one fresh disk usage calculation (non-blocking timeout)
		s.Log().Debug("no cached usage, attempting fresh disk usage calculation for backup progress")
		
		// Use a context with timeout to prevent hanging the backup process
		ctx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer cancel()
		
		// Channel to receive result
		done := make(chan struct {
			size int64
			err  error
		}, 1)
		
		// Run disk usage calculation in goroutine with timeout
		go func() {
			defer func() {
				if r := recover(); r != nil {
					s.Log().WithField("panic", r).Error("panic in disk usage calculation goroutine")
					done <- struct {
						size int64
						err  error
					}{0, errors.New("disk usage calculation panicked")}
				}
			}()
			size, err := s.Filesystem().DiskUsage(false) // Fresh calculation
			done <- struct {
				size int64
				err  error
			}{size, err}
		}()
		
		// Wait for result or timeout
		select {
		case result := <-done:
			if result.err == nil && result.size > 0 {
				estimatedSize = result.size / 2 // tar.gz compression ~50%  
				s.Log().WithField("estimated_backup_size", estimatedSize).Debug("calculated fresh disk usage for backup progress")
			} else {
				s.Log().WithField("error", result.err).Debug("fresh disk usage calculation failed")
			}
		case <-ctx.Done():
			s.Log().Warn("disk usage calculation timed out - using bytes-only mode for backup progress")
		}
	}
	
	// Set total if we got a reasonable estimate
	if estimatedSize > 0 {
		progressInstance.SetTotal(uint64(estimatedSize))
	}
	ad, err := s.generateBackupWithProgress(ctx, b, ignored, progressInstance, progressTracker)
	if err != nil {
		progressTracker.SendFinalProgress(false) // Send error progress
		if err := s.notifyPanelOfBackup(b.Identifier(), &backup.ArchiveDetails{}, false); err != nil {
			s.Log().WithFields(log.Fields{
				"backup": b.Identifier(),
				"error":  err,
			}).Warn("failed to notify panel of failed backup state")
		} else {
			s.Log().WithField("backup", b.Identifier()).Info("notified panel of failed backup state")
		}

		s.Events().Publish(BackupCompletedEvent+":"+b.Identifier(), map[string]any{
			"uuid":          b.Identifier(),
			"is_successful": false,
			"checksum":      "",
			"checksum_type": "sha1",
			"file_size":     0,
		})

		return errors.WrapIf(err, "backup: error while generating server backup")
	}

	// Try to notify the panel about the status of this backup. If for some reason this request
	// fails, delete the archive from the daemon and return that error up the chain to the caller.
	if notifyError := s.notifyPanelOfBackup(b.Identifier(), ad, true); notifyError != nil {
		_ = b.Remove()

		s.Log().WithField("error", notifyError).Info("failed to notify panel of successful backup state")
		return err
	} else {
		s.Log().WithField("backup", b.Identifier()).Info("notified panel of successful backup state")
	}

	progressTracker.SendFinalProgress(true) // Send success progress

	// Emit an event over the socket so we can update the backup in realtime on
	// the frontend for the server.
	s.Events().Publish(BackupCompletedEvent+":"+b.Identifier(), map[string]any{
		"uuid":          b.Identifier(),
		"is_successful": true,
		"checksum":      ad.Checksum,
		"checksum_type": "sha1",
		"file_size":     ad.Size,
	})

	return nil
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
func (s *Server) RestoreBackup(b backup.BackupInterface, reader io.ReadCloser) (err error) {
	s.Config().SetSuspended(true)
	// Local backups will not pass a reader through to this function, so check first
	// to make sure it is a valid reader before trying to close it.
	defer func() {
		s.Config().SetSuspended(false)
		// After restore, server should always be offline (restore requires server stop)
		s.Environment.SetState(environment.ProcessOfflineState)
		if reader != nil {
			_ = reader.Close()
		}
	}()
	// Send an API call to the Panel as soon as this function is done running so that
	// the Panel is informed of the restoration status of this backup.
	defer func() {
		if rerr := s.client.SendRestorationStatus(s.Context(), b.Identifier(), err == nil); rerr != nil {
			s.Log().WithField("error", rerr).WithField("backup", b.Identifier()).Error("failed to notify Panel of backup restoration status")
		}
	}()

	// Don't try to restore the server until we have completely stopped the running
	// instance, otherwise you'll likely hit all types of write errors due to the
	// server being suspended.
	if s.Environment.State() != environment.ProcessOfflineState {
		if err = s.Environment.WaitForStop(s.Context(), 2*time.Minute, false); err != nil {
			if !errdefs.IsNotFound(err) {
				return errors.WrapIf(err, "server/backup: restore: failed to wait for container stop")
			}
		}
	}

	// NOW set restore state after server is guaranteed to be stopped
	s.Environment.SetState(environment.ProcessRestoringState)

	// Restore progress tracking with real Progress instance
	var processedFiles int64
	
	// Create progress instance for restore - estimate total from backup file size
	restoreProgress := progress.NewProgress(0)
	
	// Try to get backup file size for percentage calculation
	if backupSize, err := b.Details(s.Context(), nil); err == nil && backupSize.Size > 0 {
		// Estimate uncompressed size (tar.gz expansion ~2x)
		estimatedTotal := backupSize.Size * 2 
		restoreProgress.SetTotal(uint64(estimatedTotal))
		s.Log().WithField("backup_size", backupSize.Size).WithField("estimated_restore_size", estimatedTotal).Debug("set restore progress total")
	}
	
	progressTracker := &SimpleProgressTracker{
		server:     s,
		backupID:   b.Identifier(),
		backupType: "restore",
		progress:   restoreProgress, // Real progress instance!
	}
	
	// Connect callback for percentage tracking
	restoreProgress.ProgressCallback = progressTracker.CheckProgress

	updateProgress := func(fileSize int64) {
		// Update progress efficiently without memory allocation
		if fileSize > 0 {
			restoreProgress.AddWritten(uint64(fileSize))
		}
		
		// Track file count (mainly for debugging/logging)
		atomic.AddInt64(&processedFiles, 1)
	}

	// Attempt to restore the backup to the server by running through each entry
	// in the file one at a time and writing them to the disk.
	s.Log().Debug("starting file writing process for backup restoration")
	err = b.Restore(s.Context(), reader, func(file string, info fs.FileInfo, r io.ReadCloser) error {
		defer r.Close()
		s.Events().Publish(DaemonMessageEvent, "(restoring): "+file)

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

	// Send final progress update
	progressTracker.SendFinalProgress(err == nil)

	return errors.WithStackIf(err)
}

// generateBackupWithProgress creates a backup with progress tracking and context support
func (s *Server) generateBackupWithProgress(ctx context.Context, b backup.BackupInterface, ignored string, progressInstance *progress.Progress, _ *SimpleProgressTracker) (*backup.ArchiveDetails, error) {
	// For local backups, we need to inject the progress tracker into the archive
	if localBackup, ok := b.(*backup.LocalBackup); ok {
		return s.generateLocalBackupWithProgress(ctx, localBackup, ignored, progressInstance)
	}

	// For S3 backups, we also need progress tracking
	if s3Backup, ok := b.(*backup.S3Backup); ok {
		return s.generateS3BackupWithProgress(ctx, s3Backup, ignored, progressInstance)
	}

	// Fallback to original Generate method if backup type is unknown
	return b.Generate(ctx, s.Filesystem(), ignored)
}

// generateLocalBackupWithProgress creates a local backup with progress tracking and context support  
func (s *Server) generateLocalBackupWithProgress(ctx context.Context, b *backup.LocalBackup, ignored string, progressInstance *progress.Progress) (*backup.ArchiveDetails, error) {
	a := &filesystem.Archive{
		Filesystem: s.Filesystem(),
		Ignore:     ignored,
		Progress:   progressInstance, // Inject progress tracker
	}

	s.Log().WithField("backup", b.Identifier()).WithField("path", b.Path()).Info("creating backup for server")
	if err := a.Create(ctx, b.Path()); err != nil {
		return nil, err
	}
	s.Log().WithField("backup", b.Identifier()).Info("created backup successfully")

	ad, err := b.Details(s.Context(), nil)
	if err != nil {
		return nil, errors.WrapIf(err, "backup: failed to get archive details for local backup")
	}
	return ad, nil
}

// generateS3BackupWithProgress creates an S3 backup with progress tracking and context support
func (s *Server) generateS3BackupWithProgress(ctx context.Context, b *backup.S3Backup, ignored string, _ *progress.Progress) (*backup.ArchiveDetails, error) {
	// Work WITH the source: S3Backup.Generate already handles everything correctly
	// Avoid double-creation by letting the original S3 flow work unmodified
	// 
	// Future improvement: Extend backup package to support progress callbacks natively
	// For now: Accept that S3 progress tracking is limited, but backup works correctly
	return b.Generate(ctx, s.Filesystem(), ignored)
}
