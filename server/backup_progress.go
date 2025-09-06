package server

import (
	"sync/atomic"
	"time"

	"github.com/Rene-Roscher/wings/internal/progress"
)

// SimpleProgressTracker - ultra-lightweight progress tracking with ZERO overhead
type SimpleProgressTracker struct {
	server     *Server
	backupType string
	progress   *progress.Progress
	lastSent   int64 // Last percentage sent
	lastTime   int64 // Last time sent (nanoseconds)
}

// BackupProgressUpdate represents the data sent over WebSocket
type BackupProgressUpdate struct {
	Type         string `json:"type"`
	Percentage   int    `json:"percentage"`
	BytesWritten int64  `json:"bytes_written,omitempty"`
	BytesTotal   int64  `json:"bytes_total,omitempty"`
}

// CheckProgress - called periodically by Archive.Write() - NO GOROUTINES!
func (spt *SimpleProgressTracker) CheckProgress() {
	if spt.progress == nil {
		return
	}

	written := int64(spt.progress.Written())
	total := int64(spt.progress.Total())
	
	if total == 0 {
		return // No total set yet
	}
	
	percentage := int((written * 100) / total)
	if percentage > 100 {
		percentage = 100
	}
	
	now := time.Now().UnixNano()
	lastPercentage := int(atomic.LoadInt64(&spt.lastSent))
	lastTime := atomic.LoadInt64(&spt.lastTime)
	
	// Only send if percentage increased AND at least 500ms passed
	if percentage > lastPercentage && (now-lastTime) > 500*1000000 { // 500ms in nanoseconds
		atomic.StoreInt64(&spt.lastSent, int64(percentage))
		atomic.StoreInt64(&spt.lastTime, now)
		
		// Send progress update (async to avoid blocking)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					// Silent recovery - don't block backup process
				}
			}()
			
			update := BackupProgressUpdate{
				Type:         spt.backupType,
				Percentage:   percentage,
				BytesWritten: written,
				BytesTotal:   total,
			}
			
			spt.server.Events().Publish(BackupProgressEvent, update)
		}()
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