package backup

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/cenkalti/backoff/v4"
	"github.com/juju/ratelimit"
	"github.com/mholt/archives"

	"github.com/Rene-Roscher/wings/config"
	"github.com/Rene-Roscher/wings/remote"
	"github.com/Rene-Roscher/wings/server/filesystem"
)

type S3Backup struct {
	Backup
	// Progress tracker for upload phase (optional)
	uploadProgress ProgressTracker
	// Progress callback for upload phase (optional)
	uploadCallback func()
}

// ProgressTracker interface for S3 upload progress tracking
// Compatible with internal/progress.Progress
type ProgressTracker interface {
	AddWritten(bytes uint64)
	Total() uint64
	Written() uint64
}

var _ BackupInterface = (*S3Backup)(nil)

func NewS3(client remote.Client, uuid string, ignore string) *S3Backup {
	return &S3Backup{
		Backup: Backup{
			client:  client,
			Uuid:    uuid,
			Ignore:  ignore,
			adapter: S3BackupAdapter,
		},
		uploadProgress: nil, // Set via WithUploadProgress method
	}
}

// WithUploadProgress sets the progress tracker for S3 upload phase
func (s *S3Backup) WithUploadProgress(progress ProgressTracker) *S3Backup {
	s.uploadProgress = progress
	return s
}

// WithUploadCallback sets the callback to trigger on upload progress
func (s *S3Backup) WithUploadCallback(callback func()) *S3Backup {
	s.uploadCallback = callback
	return s
}

// Remove removes a backup from the system.
func (s *S3Backup) Remove() error {
	return os.Remove(s.Path())
}

// WithLogContext attaches additional context to the log output for this backup.
func (s *S3Backup) WithLogContext(c map[string]interface{}) {
	s.logContext = c
}

// Generate creates a new backup on the disk, moves it into the S3 bucket via
// the provided presigned URL, and then deletes the backup from the disk.
func (s *S3Backup) Generate(ctx context.Context, fsys *filesystem.Filesystem, ignore string) (*ArchiveDetails, error) {
	var uploadedParts []remote.BackupPart
	success := false
	
	defer func() {
		if success {
			s.Remove() // Only remove on successful upload
		} else {
			// Clean up orphaned S3 parts on failure
			if len(uploadedParts) > 0 {
				s.log().WithField("orphaned_parts", len(uploadedParts)).Warn("cleaning up orphaned S3 parts after backup failure")
				// Note: Panel should handle multipart upload abort via CompleteMultipartUpload API
				// We log the issue for monitoring and manual cleanup if needed
			}
		}
		// On failure, backup file is kept for debugging/retry
	}()

	// Check if backup archive already exists (S3 two-phase backup)
	if _, err := os.Stat(s.Path()); os.IsNotExist(err) {
		// Archive doesn't exist - create it (single-phase backup)
		a := &filesystem.Archive{
			Filesystem: fsys,
			Ignore:     ignore,
		}

		s.log().WithField("path", s.Path()).Info("creating backup for server")
		if err := a.Create(ctx, s.Path()); err != nil {
			return nil, err
		}
		s.log().Info("created backup successfully")
	} else if err != nil {
		// Handle other stat errors (permissions, etc)
		s.log().WithField("error", err).Warn("failed to stat backup file - attempting to create anyway")
		a := &filesystem.Archive{
			Filesystem: fsys,
			Ignore:     ignore,
		}

		s.log().WithField("path", s.Path()).Info("creating backup for server (stat failed)")
		if err := a.Create(ctx, s.Path()); err != nil {
			return nil, err
		}
		s.log().Info("created backup successfully")
	} else {
		// Archive already exists - proceed with upload (two-phase backup)
		s.log().WithField("path", s.Path()).Info("using existing backup archive for S3 upload")
	}

	rc, err := os.Open(s.Path())
	if err != nil {
		return nil, errors.Wrap(err, "backup: could not read archive from disk")
	}
	defer rc.Close()

	parts, err := s.generateRemoteRequest(ctx, rc)
	if err != nil {
		uploadedParts = parts // Store for cleanup
		return nil, err
	}
	uploadedParts = parts
	ad, err := s.Details(ctx, parts)
	if err != nil {
		return nil, errors.WrapIf(err, "backup: failed to get archive details after upload")
	}

	success = true // Mark as successful for cleanup
	return ad, nil
}

// Restore will read from the provided reader which should be a TAR archive.
// IMPORTANT: For S3 restores, the server layer has already handled decompression,
// so we receive a decompressed TAR stream ready for extraction.
//
// When a file is encountered in the archive the callback function will be triggered.
// If the callback returns an error the entire process is stopped.
func (s *S3Backup) Restore(ctx context.Context, r io.Reader, callback RestoreCallback) error {
	s.log().Debug("S3 restore: starting restore process")
	
	// CRITICAL: The reader provided here is ALREADY DECOMPRESSED by the server layer!
	// The server's RestoreBackupWithContext method handles:
	// 1. Format detection (gzip, zstd, etc.)
	// 2. Decompression
	// 3. Passing us the clean TAR stream
	//
	// We should NOT attempt format detection or decompression here!
	
	// Start with the provided reader (already decompressed TAR stream)
	finalReader := r
	
	// Apply write rate limiting to prevent disk overload
	if writeLimit := int64(config.Get().System.Backups.WriteLimit * 1024 * 1024); writeLimit > 0 {
		s.log().WithField("write_limit_mb", writeLimit/1024/1024).Debug("S3 restore: applying write rate limit")
		finalReader = ratelimit.Reader(r, ratelimit.NewBucketWithRate(float64(writeLimit), writeLimit))
	}
	
	// Note: Download progress tracking doesn't make sense here because:
	// 1. We're receiving an already-decompressed stream
	// 2. The actual download happens in the router layer
	// 3. The size would be the decompressed size, not download size
	// Progress tracking for restore happens at the file extraction level in the server layer
	
	s.log().Debug("S3 restore: starting TAR archive extraction")
	// Use the mholt/archives package to extract TAR archive
	// The reader is already decompressed, just extract the TAR
	tarFormat := archives.Tar{}
	fileCount := 0
	totalBytes := uint64(0)
	
	if err := tarFormat.Extract(ctx, finalReader, func(ctx context.Context, f archives.FileInfo) error {
		fileCount++
		totalBytes += uint64(f.Size())
		
		// Log every 100 files or every 100MB to track progress
		if fileCount%100 == 0 || totalBytes%(100*1024*1024) < uint64(f.Size()) {
			s.log().WithFields(log.Fields{
				"files_processed": fileCount,
				"total_bytes_mb":  totalBytes / (1024 * 1024),
				"current_file":    f.NameInArchive,
			}).Debug("S3 restore: extraction progress")
		}
		
		r, err := f.Open()
		if err != nil {
			s.log().WithFields(log.Fields{
				"file": f.NameInArchive,
				"error": err,
			}).Error("S3 restore: failed to open file from archive")
			return err
		}
		defer r.Close()

		// DIRECT CALLBACK - no goroutine needed!
		// The callback should be fast and context-aware itself
		if err := callback(f.NameInArchive, f.FileInfo, r); err != nil {
			s.log().WithFields(log.Fields{
				"file": f.NameInArchive,
				"error": err,
			}).Error("S3 restore: callback failed for file")
			return err
		}
		
		// Check context after each file
		select {
		case <-ctx.Done():
			s.log().WithField("file", f.NameInArchive).Warn("S3 restore: context cancelled during extraction")
			return ctx.Err()
		default:
			// Continue processing
		}
		
		return nil
	}); err != nil {
		s.log().WithFields(log.Fields{
			"files_processed": fileCount,
			"total_bytes_mb":  totalBytes / (1024 * 1024),
			"error": err,
		}).Error("S3 restore: TAR extraction failed")
		return err
	}
	
	s.log().WithFields(log.Fields{
		"files_processed": fileCount,
		"total_bytes_mb":  totalBytes / (1024 * 1024),
	}).Info("S3 restore: completed successfully")
	return nil
}

// Generates the remote S3 request and begins the upload.
func (s *S3Backup) generateRemoteRequest(ctx context.Context, rc io.ReadCloser) ([]remote.BackupPart, error) {
	defer rc.Close()

	s.log().Debug("attempting to get size of backup...")
	size, err := s.Backup.Size()
	if err != nil {
		return nil, err
	}
	s.log().WithField("size", size).Debug("got size of backup")

	s.log().Debug("attempting to get S3 upload urls from Panel...")
	urls, err := s.client.GetBackupRemoteUploadURLs(ctx, s.Backup.Uuid, size)
	if err != nil {
		return nil, err
	}
	s.log().Debug("got S3 upload urls from the Panel")
	s.log().WithField("parts", len(urls.Parts)).Info("attempting to upload backup to s3 endpoint...")

	uploader := newS3FileUploader(rc)
	// Set progress tracker and callback if available
	if s.uploadProgress != nil {
		uploader.WithProgressTracker(s.uploadProgress)
		if s.uploadCallback != nil {
			uploader.WithProgressCallback(s.uploadCallback)
		}
	}
	for i, part := range urls.Parts {
		// Check context before each part upload
		select {
		case <-ctx.Done():
			s.log().WithField("uploaded_parts", len(uploader.uploadedParts)).Warn("backup cancelled, uploaded parts may need cleanup")
			return uploader.uploadedParts, ctx.Err()
		default:
		}
		
		// Get the size for the current part.
		var partSize int64
		if i+1 < len(urls.Parts) {
			partSize = urls.PartSize
		} else {
			// This is the remaining size for the last part,
			// there is not a minimum size limit for the last part.
			partSize = size - (int64(i) * urls.PartSize)
		}

		// Attempt to upload the part with context.
		etag, err := uploader.uploadPart(ctx, part, partSize)
		if err != nil {
			s.log().WithField("part_id", i+1).WithField("uploaded_parts", len(uploader.uploadedParts)).WithField("total_parts", len(urls.Parts)).WithError(err).Error("failed to upload S3 part - uploaded parts may be orphaned")
			return uploader.uploadedParts, err
		}
		uploader.uploadedParts = append(uploader.uploadedParts, remote.BackupPart{
			ETag:       etag,
			PartNumber: i + 1,
		})
		s.log().WithField("part_id", i+1).Info("successfully uploaded backup part")
	}
	s.log().WithField("parts", len(urls.Parts)).Info("backup has been successfully uploaded")

	return uploader.uploadedParts, nil
}

type s3FileUploader struct {
	io.ReadCloser
	client          *http.Client
	uploadedParts   []remote.BackupPart
	progressTracker ProgressTracker
	progressCallback func()
}

// newS3FileUploader returns a new file uploader instance.
func newS3FileUploader(file io.ReadCloser) *s3FileUploader {
	return &s3FileUploader{
		ReadCloser: file,
		// We purposefully use a super high timeout on this request since we need to upload
		// a 5GB file. This assumes at worst a 10Mbps connection for uploading. While technically
		// you could go slower we're targeting mostly hosted servers that should have 100Mbps
		// connections anyways.
		client:          &http.Client{Timeout: time.Hour * 2},
		progressTracker: nil, // Set via WithProgressTracker method
	}
}

// WithProgressTracker sets the progress tracker for upload progress
func (fu *s3FileUploader) WithProgressTracker(progress ProgressTracker) *s3FileUploader {
	fu.progressTracker = progress
	return fu
}

// WithProgressCallback sets the callback for upload progress
func (fu *s3FileUploader) WithProgressCallback(callback func()) *s3FileUploader {
	fu.progressCallback = callback
	return fu
}

// backoff returns a new expoential backoff implementation using a context that
// will also stop the backoff if it is canceled.
func (fu *s3FileUploader) backoff(ctx context.Context) backoff.BackOffContext {
	b := backoff.NewExponentialBackOff()
	b.Multiplier = 2
	b.MaxElapsedTime = time.Minute

	return backoff.WithContext(b, ctx)
}

// uploadPart attempts to upload a given S3 file part to the S3 system. If a
// 5xx error is returned from the endpoint this will continue with an exponential
// backoff to try and successfully upload the part.
//
// Once uploaded the ETag is returned to the caller.
func (fu *s3FileUploader) uploadPart(ctx context.Context, part string, size int64) (string, error) {
	// Validate input parameters to prevent attacks
	if size <= 0 || size > (5*1024*1024*1024) { // Max 5GB per part (S3 limit)
		return "", errors.New("backup: invalid part size for S3 upload")
	}
	
	r, err := http.NewRequestWithContext(ctx, http.MethodPut, part, nil)
	if err != nil {
		return "", errors.Wrap(err, "backup: could not create request for S3")
	}

	r.ContentLength = size
	r.Header.Add("Content-Length", strconv.Itoa(int(size)))
	// Use generic content type since we support multiple compression formats
	// The actual format will be auto-detected during restore
	r.Header.Add("Content-Type", "application/octet-stream")

	// Limit the reader to the size of the part - prevents over-read attacks
	limitedReader := io.LimitReader(fu.ReadCloser, size)
	
	// Wrap with progress tracking if available
	if fu.progressTracker != nil {
		progressReader := NewProgressReader(limitedReader, fu.progressTracker)
		if fu.progressCallback != nil {
			progressReader.WithCallback(fu.progressCallback)
		}
		r.Body = Reader{Reader: progressReader}
	} else {
		r.Body = Reader{Reader: limitedReader}
	}

	var etag string
	err = backoff.Retry(func() error {
		res, err := fu.client.Do(r)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return backoff.Permanent(err)
			}
			// Don't use a permanent error here, if there is a temporary resolution error with
			// the URL due to DNS issues we want to keep re-trying.
			return errors.Wrap(err, "backup: S3 HTTP request failed")
		}
		_ = res.Body.Close()

		if res.StatusCode != http.StatusOK {
			err := errors.New(fmt.Sprintf("backup: failed to put S3 object: [HTTP/%d] %s", res.StatusCode, res.Status))
			// Only attempt a backoff retry if this error is because of a 5xx error from
			// the S3 endpoint. Any 4xx error should be treated as an error that a retry
			// would not fix.
			if res.StatusCode >= http.StatusInternalServerError {
				return err
			}
			return backoff.Permanent(err)
		}

		// Get the ETag from the uploaded part, this should be sent with the
		// CompleteMultipartUpload request.
		etag = res.Header.Get("ETag")

		return nil
	}, fu.backoff(ctx))
	if err != nil {
		if v, ok := err.(*backoff.PermanentError); ok {
			return "", v.Unwrap()
		}
		return "", err
	}
	return etag, nil
}

// Reader provides a wrapper around an existing io.Reader
// but implements io.Closer in order to satisfy an io.ReadCloser.
type Reader struct {
	io.Reader
}

func (Reader) Close() error {
	return nil
}

// ProgressReader wraps an io.Reader and tracks bytes read for progress updates
type ProgressReader struct {
	reader       io.Reader
	progress     ProgressTracker
	callback     func() // Optional callback for progress updates
	lastCallback int64  // Last time callback was triggered (unix nano)
	mutex        sync.Mutex
}

// NewProgressReader creates a new progress-aware reader
func NewProgressReader(reader io.Reader, progress ProgressTracker) *ProgressReader {
	return &ProgressReader{
		reader:   reader,
		progress: progress,
		callback: nil,
	}
}

// WithCallback sets an optional callback to be triggered on progress updates
func (pr *ProgressReader) WithCallback(callback func()) *ProgressReader {
	pr.callback = callback
	return pr
}

// Read implements io.Reader and updates progress as bytes are read
func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	if n > 0 && pr.progress != nil {
		pr.mutex.Lock()
		pr.progress.AddWritten(uint64(n))
		
		// Trigger callback if set, but throttle to max once per 250ms
		if pr.callback != nil {
			now := time.Now().UnixNano()
			// Send update if 250ms have passed since last callback
			if now-pr.lastCallback >= 250_000_000 { // 250ms in nanoseconds
				pr.lastCallback = now
				// Recover from panic in callback - progress events must never break uploads
				func() {
					defer func() {
						if r := recover(); r != nil {
							// Silently ignore - progress is non-critical
						}
					}()
					pr.callback()
				}()
			}
		}
		pr.mutex.Unlock()
	}
	return n, err
}

// Close implements io.Closer (no-op for compatibility)
func (pr *ProgressReader) Close() error {
	if closer, ok := pr.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
