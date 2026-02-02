package backup

import (
	"io"
	"sync/atomic"
	"time"

	"github.com/apex/log"
)

// DownloadProgressReader wraps an io.ReadCloser to track download progress
type DownloadProgressReader struct {
	reader      io.ReadCloser
	totalSize   int64
	downloaded  atomic.Int64
	lastReport  atomic.Int64
	onProgress  func(downloaded, total int64)
	logger      *log.Entry
	backupID    string
}

// NewDownloadProgressReader creates a new progress-tracking reader
func NewDownloadProgressReader(reader io.ReadCloser, totalSize int64, backupID string, onProgress func(downloaded, total int64)) *DownloadProgressReader {
	return &DownloadProgressReader{
		reader:     reader,
		totalSize:  totalSize,
		onProgress: onProgress,
		logger:     log.WithFields(log.Fields{"backup_id": backupID, "component": "download_progress"}),
		backupID:   backupID,
	}
}

// Read implements io.Reader with progress tracking
func (r *DownloadProgressReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if n > 0 {
		// Update downloaded bytes
		current := r.downloaded.Add(int64(n))
		
		// Report progress at most once per 100ms to avoid spam
		now := time.Now().UnixNano()
		lastReport := r.lastReport.Load()
		if now-lastReport > 100*int64(time.Millisecond) {
			if r.lastReport.CompareAndSwap(lastReport, now) {
				if r.onProgress != nil {
					r.onProgress(current, r.totalSize)
				}
				
				// Log progress every 10%
				if r.totalSize > 0 {
					percentage := (current * 100) / r.totalSize
					if percentage%10 == 0 {
						r.logger.WithFields(log.Fields{
							"downloaded_mb": current / (1024 * 1024),
							"total_mb":      r.totalSize / (1024 * 1024),
							"percentage":    percentage,
						}).Info("S3 download progress")
					}
				}
			}
		}
	}
	
	// Log completion
	if err == io.EOF && r.totalSize > 0 {
		r.logger.WithFields(log.Fields{
			"downloaded_mb": r.downloaded.Load() / (1024 * 1024),
			"total_mb":      r.totalSize / (1024 * 1024),
		}).Info("S3 download completed")
	}
	
	return n, err
}

// Close implements io.Closer
func (r *DownloadProgressReader) Close() error {
	return r.reader.Close()
}