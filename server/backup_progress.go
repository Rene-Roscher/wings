package server

import (
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

	// Ultra-fast check: only proceed if enough time passed (no expensive operations)
	now := time.Now().UnixNano()
	lastTime := atomic.LoadInt64(&spt.lastTime)
	
	// Smart throttling: 200ms for super-live feel, but not spam
	if (now - lastTime) < 200*1000000 { // 200ms = super live
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
		
		// Ultra-fast async send (no defer overhead)
		go func(p int, w, t int64) {
			// Minimal recovery overhead
			if r := recover(); r != nil {
				return
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
	
	// Send final progress (async to never break backup process)
	go func() {
		defer func() {
			recover() // Silent recovery - progress failures must never break backups
		}()
		spt.server.Events().Publish(BackupProgressEvent, update)
	}()
}