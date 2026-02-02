package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apex/log"
	"github.com/Rene-Roscher/wings/internal/progress"
)

// SimpleProgressTracker - ultra-lightweight progress tracking with ZERO overhead
type SimpleProgressTracker struct {
	server       *Server
	backupID     string
	backupType   string
	progress     *progress.Progress
	lastSent     int64 // Last percentage sent
	lastTime     int64 // Last time sent (nanoseconds)
	lastBytes    int64 // Last bytes value sent (for detecting changes at 100%)
	isS3         bool  // Whether this is an S3 backup (needs 80/20 split)
	archiveSize  int64 // Size of archive (for S3 80/20 calculation)
	
	// Context-aware goroutine management
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// BackupProgressUpdate represents the data sent over WebSocket
type BackupProgressUpdate struct {
	BackupID     string `json:"backup_id"`
	Type         string `json:"type"`
	Percentage   int    `json:"percentage"`
	BytesWritten int64  `json:"bytes_written,omitempty"`
	BytesTotal   int64  `json:"bytes_total,omitempty"`
}

// CheckProgress - called on every Archive.Write() - ULTRA FAST!
func (spt *SimpleProgressTracker) CheckProgress() {
	if spt.progress == nil {
		return
	}

	// Ultra-fast time-based throttling - minimize syscalls
	now := time.Now().UnixNano()
	lastTime := atomic.LoadInt64(&spt.lastTime)

	// Only load values if we might send an update (performance!)
	written := int64(spt.progress.Written())
	total := int64(spt.progress.Total())

	var percentage int
	var shouldSend bool
	lastSent := atomic.LoadInt64(&spt.lastSent)

	// SINGLE THROTTLING CHECK: Only send events maximum every 250ms to prevent WebSocket flooding
	const throttleIntervalNanos = 250_000_000 // 250ms in nanoseconds  
	shouldSendByTime := (now - lastTime) >= throttleIntervalNanos

	// Check if this is initial progress (0%)
	isInitialProgress := lastTime == 0 && lastSent == 0

	if total > 0 {
		// Standard percentage calculation first
		rawPercentage := int((written * 100) / total)
		
		// S3 SPECIAL CASE: Scale to 80% during archive, then 80-100% during upload
		if spt.isS3 {
			if spt.archiveSize == 0 {
				// Archive phase: scale to 0-80%
				// Now that total is the full size, written should reach approximately total
				if written >= total {
					percentage = 80 // Cap at 80% when archive is done
				} else {
					// Scale 0 to total => 0 to 80%
					percentage = int((written * 80) / total)
				}
			} else {
				// Upload phase: written goes from total to total+archiveSize
				// Scale this to 80-100%
				if written <= total {
					percentage = 80 // Still at 80% if upload hasn't started
				} else {
					// Upload progress: how much of the archive have we uploaded?
					uploadBytes := written - total
					if uploadBytes >= spt.archiveSize {
						percentage = 100 // Upload complete
					} else {
						// Scale upload progress (0 to archiveSize) to (80% to 100%)
						uploadPercent := int((uploadBytes * 20) / spt.archiveSize)
						percentage = 80 + uploadPercent
					}
				}
			}
		} else {
			// Standard percentage for non-S3
			percentage = rawPercentage
		}
		percentage = min(100, percentage)
		
		// Send on percentage increase AND time throttle (OR initial)
		percentageChanged := percentage > int(lastSent)
		// Also send if bytes changed (for updates within same percentage)
		lastBytesVal := atomic.LoadInt64(&spt.lastBytes)
		bytesChanged := int64(written) != lastBytesVal
		shouldSend = ((percentageChanged || bytesChanged) && shouldSendByTime) || isInitialProgress
		if shouldSend {
			atomic.StoreInt64(&spt.lastSent, int64(percentage))
			atomic.StoreInt64(&spt.lastBytes, int64(written))
		}
	} else {
		// Byte mode - show progress in 1MB chunks with time throttling  
		percentage = 0 // Use 0% for unknown total instead of -1
		lastMB := lastSent
		currentMB := written / (1024 * 1024) // 1MB chunks
		dataChanged := currentMB > lastMB || written > 0 // Include any progress
		shouldSend = (dataChanged && shouldSendByTime) || isInitialProgress
		if shouldSend {
			atomic.StoreInt64(&spt.lastSent, currentMB)
		}
	}

	// ALWAYS send initial progress (0%) and final progress (100%) - but only ONCE!
	isFinalProgress := total > 0 && percentage >= 100 && atomic.LoadInt64(&spt.lastSent) < 100
	
	if shouldSend || isFinalProgress {
		atomic.StoreInt64(&spt.lastTime, now)
		if percentage >= 0 {
			atomic.StoreInt64(&spt.lastSent, int64(percentage))
		}

		// Check context before sending
		if spt.ctx != nil {
			select {
			case <-spt.ctx.Done():
				return // Context cancelled, skip event
			default:
			}
		}
		
		// CRITICAL FIX: Send events SYNCHRONOUSLY during normal progress
		// Only the FINAL events truly need to be async to avoid blocking restore completion
		// Regular progress events are fast enough to send inline
		update := BackupProgressUpdate{
			BackupID:     spt.backupID,
			Type:         spt.backupType,
			Percentage:   percentage,
			BytesWritten: written,
			BytesTotal:   total,
		}

		// For FINAL progress, we still use async to not block the restore completion
		// But for normal progress, send synchronously to avoid goroutine accumulation
		if isFinalProgress {
			// Only spawn goroutine for final event to avoid blocking restore
			spt.wg.Add(1)
			go func() {
				defer spt.wg.Done()
				defer func() {
					if r := recover(); r != nil {
						return
					}
				}()
				
				// Final check before send
				if spt.ctx != nil {
					select {
					case <-spt.ctx.Done():
						return
					default:
					}
				}
				
				spt.server.Events().Publish(BackupProgressEvent, update)
				spt.server.Log().WithField("backup_id", spt.backupID).
					WithField("percentage", percentage).
					WithField("bytes_written", written).
					WithField("bytes_total", total).
					Info("sent FINAL backup progress event")
			}()
		} else {
			// Send normal progress events synchronously - they're fast!
			spt.server.Events().Publish(BackupProgressEvent, update)
			
			// Log initial event for debugging
			if isInitialProgress {
				spt.server.Log().WithField("backup_id", spt.backupID).
					WithField("percentage", percentage).
					WithField("bytes_total", total).
					Debug("sent INITIAL backup progress event")
			}
		}
	}
}

// NewSimpleProgressTracker creates a progress tracker with proper lifecycle management
func NewSimpleProgressTracker(ctx context.Context, server *Server, backupID, backupType string, progress *progress.Progress) *SimpleProgressTracker {
	progCtx, cancel := context.WithCancel(ctx)
	return &SimpleProgressTracker{
		server:     server,
		backupID:   backupID,
		backupType: backupType,
		progress:   progress,
		ctx:        progCtx,
		cancel:     cancel,
		isS3:       false, // Will be set via SetS3Mode if needed
	}
}

// SetS3Mode configures the tracker for S3 80/20 split
func (spt *SimpleProgressTracker) SetS3Mode(archiveSize int64) {
	spt.isS3 = true
	spt.archiveSize = archiveSize
}

// Close cleans up all goroutines and resources
func (spt *SimpleProgressTracker) Close() {
	// Cancel context first to signal all goroutines to stop
	if spt.cancel != nil {
		spt.cancel()
		spt.cancel = nil // Prevent double-cancel
	}
	
	// Wait for any pending CheckProgress goroutines with SHORT timeout
	// These are fire-and-forget event sends, we don't need to wait long
	done := make(chan struct{})
	go func() {
		spt.wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		// All goroutines finished cleanly
	case <-time.After(100 * time.Millisecond):
		// Very short timeout - these are just event sends
		// If they're not done in 100ms, they're stuck and we move on
		// This prevents blocking the entire restore operation
		if spt.server != nil {
			spt.server.Log().Debug("progress tracker closed with pending events")
		}
	}
}

// SendFinalProgress - call when backup completes
func (spt *SimpleProgressTracker) SendFinalProgress(success bool) {
	percentage := 100
	if !success {
		percentage = -1 // Error indicator
	}

	var written, total int64
	if spt.progress != nil {
		written = int64(spt.progress.Written())
		total = int64(spt.progress.Total())
	}

	spt.server.Log().WithFields(log.Fields{
		"backup_id":     spt.backupID,
		"backup_type":   spt.backupType,
		"success":       success,
		"percentage":    percentage,
		"written":       written,
		"total":         total,
		"is_restoring":  spt.server.IsRestoring(),
		"server_state":  spt.server.Environment.State(),
	}).Info("SENDING FINAL PROGRESS EVENT")

	update := BackupProgressUpdate{
		BackupID:     spt.backupID,
		Type:         spt.backupType,
		Percentage:   percentage,
		BytesWritten: written,
		BytesTotal:   total,
	}

	// Send final progress SYNCHRONOUSLY - no goroutine!
	// This is the FINAL event, we don't need async here
	if spt.ctx != nil {
		select {
		case <-spt.ctx.Done():
			spt.server.Log().Warn("context cancelled, skipping final progress event")
			return
		default:
		}
	}
	
	// Send the final event directly - this is fast enough
	spt.server.Events().Publish(BackupProgressEvent, update)

	// Update lastSent to reflect final progress
	atomic.StoreInt64(&spt.lastSent, int64(percentage))

	// NO MORE GOROUTINES HERE! The caller will handle Close()
}
