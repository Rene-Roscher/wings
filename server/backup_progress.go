package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rene-Roscher/wings/internal/progress"
)

// SimpleProgressTracker - ultra-lightweight progress tracking with ZERO overhead
type SimpleProgressTracker struct {
	server     *Server
	backupID   string
	backupType string
	progress   *progress.Progress
	lastSent   int64 // Last percentage sent
	lastTime   int64 // Last time sent (nanoseconds)
	
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
		// Percentage mode - responsive but throttled
		percentage = min(100, int((written*100)/total))
		// Send on percentage increase AND time throttle (OR initial)
		percentageChanged := percentage > int(lastSent)
		shouldSend = (percentageChanged && shouldSendByTime) || isInitialProgress
		if shouldSend {
			atomic.StoreInt64(&spt.lastSent, int64(percentage))
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

		// Context-aware async send with proper lifecycle management
		if spt.ctx != nil {
			select {
			case <-spt.ctx.Done():
				return // Don't spawn goroutine if context is cancelled
			default:
			}
		}
		
		spt.wg.Add(1)
		go func(p int, w, t int64, isFinal, isInitial bool) {
			defer spt.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					return // Minimal recovery overhead
				}
			}()
			
			// Check context before expensive operations
			if spt.ctx != nil {
				select {
				case <-spt.ctx.Done():
					return
				default:
				}
			}

			update := BackupProgressUpdate{
				BackupID:     spt.backupID,
				Type:         spt.backupType,
				Percentage:   p,
				BytesWritten: w,
				BytesTotal:   t,
			}

			spt.server.Events().Publish(BackupProgressEvent, update)
			
			// Enhanced debugging for critical events
			if isFinal {
				spt.server.Log().WithField("backup_id", spt.backupID).
					WithField("percentage", p).
					WithField("bytes_written", w).
					WithField("bytes_total", t).
					Info("sent FINAL backup progress event")
			} else if isInitial {
				spt.server.Log().WithField("backup_id", spt.backupID).
					WithField("percentage", p).
					WithField("bytes_total", t).
					Debug("sent INITIAL backup progress event")
			}
		}(percentage, written, total, isFinalProgress, isInitialProgress)
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
	}
}

// Close cleans up all goroutines and resources
func (spt *SimpleProgressTracker) Close() {
	if spt.cancel != nil {
		spt.cancel()
		spt.cancel = nil // Prevent double-cancel
	}
	spt.wg.Wait() // Wait for all goroutines to finish
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

	update := BackupProgressUpdate{
		BackupID:     spt.backupID,
		Type:         spt.backupType,
		Percentage:   percentage,
		BytesWritten: written,
		BytesTotal:   total,
	}

	// Send final progress with context awareness
	if spt.ctx != nil {
		select {
		case <-spt.ctx.Done():
			return // Don't send if context is cancelled
		default:
		}
	}
	
	spt.wg.Add(1)
	go func() {
		defer spt.wg.Done()
		defer func() {
			recover() // Silent recovery - progress failures must never break backups
		}()
		
		// Final context check
		if spt.ctx != nil {
			select {
			case <-spt.ctx.Done():
				return
			default:
			}
		}
		
		spt.server.Events().Publish(BackupProgressEvent, update)
	}()
	
	// Close after final progress with managed goroutine
	spt.wg.Add(1)
	go func() {
		defer spt.wg.Done()
		defer func() {
			recover() // Silent recovery
		}()
		
		// Use context-aware sleep instead of time.Sleep
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		
		select {
		case <-timer.C:
			spt.Close()
		case <-spt.ctx.Done():
			spt.Close() // Still close even if context cancelled
			return
		}
	}()
}
