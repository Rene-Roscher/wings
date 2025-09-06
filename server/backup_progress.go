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

	// Smart throttling: 300ms for balance between responsiveness and performance  
	if (now - lastTime) < 300*1000000 { // 300ms = good balance
		return
	}

	// Only load values if we might send an update (performance!)
	written := int64(spt.progress.Written())
	total := int64(spt.progress.Total())

	var percentage int
	var shouldSend bool
	lastSent := atomic.LoadInt64(&spt.lastSent)

	if total > 0 {
		// Percentage mode - very responsive
		percentage = min(100, int((written*100)/total))
		// Send on ANY percentage increase (1%, 2%, 3%... super live!)
		shouldSend = percentage > int(lastSent)
		if shouldSend {
			atomic.StoreInt64(&spt.lastSent, int64(percentage))
		}
	} else {
		// Byte mode - show every 512KB for max liveness without spam
		percentage = -1
		lastKB := lastSent
		currentKB := written / (512 * 1024) // 512KB chunks = very live
		shouldSend = currentKB > lastKB
		if shouldSend {
			atomic.StoreInt64(&spt.lastSent, currentKB)
		}
	}

	if shouldSend {
		atomic.StoreInt64(&spt.lastTime, now)

		// Context-aware async send with proper lifecycle management
		if spt.ctx != nil {
			select {
			case <-spt.ctx.Done():
				return // Don't spawn goroutine if context is cancelled
			default:
			}
		}
		
		spt.wg.Add(1)
		go func(p int, w, t int64) {
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
		}(percentage, written, total)
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
	
	// Close after final progress
	go func() {
		time.Sleep(100 * time.Millisecond) // Allow final progress to send
		spt.Close()
	}()
}
