package router

import (
	"context"
	"io"
	"net/http"
	"os"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/gin-gonic/gin"

	"github.com/Rene-Roscher/wings/environment"
	"github.com/Rene-Roscher/wings/router/middleware"
	"github.com/Rene-Roscher/wings/server"
	"github.com/Rene-Roscher/wings/server/backup"
)

// isValidBackupContentType is now replaced by backup.IsValidBackupContentType
// which uses the extensible CompressionRegistry for better format support

// postServerBackup performs a backup against a given server instance using the
// provided backup adapter.
func postServerBackup(c *gin.Context) {
	s := middleware.ExtractServer(c)
	client := middleware.ExtractApiClient(c)
	logger := middleware.ExtractLogger(c)

	// RACE CONDITION PROTECTION: Prevent concurrent operations
	if s.IsBackingUp() {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": "A backup operation is already running for this server",
		})
		return
	}
	if s.IsRestoring() {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": "A restore operation is already running for this server",
		})
		return
	}
	if s.IsTransferring() {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": "A transfer operation is already running for this server",
		})
		return
	}
	var data struct {
		Adapter backup.AdapterType `json:"adapter"`
		Uuid    string             `json:"uuid"`
		Ignore  string             `json:"ignore"`
	}
	if err := c.BindJSON(&data); err != nil {
		return
	}

	var adapter backup.BackupInterface
	switch data.Adapter {
	case backup.LocalBackupAdapter:
		adapter = backup.NewLocal(client, data.Uuid, data.Ignore)
	case backup.S3BackupAdapter:
		adapter = backup.NewS3(client, data.Uuid, data.Ignore)
	default:
		middleware.CaptureAndAbort(c, errors.New("router/backups: provided adapter is not valid: "+string(data.Adapter)))
		return
	}

	// Attach the server ID and the request ID to the adapter log context for easier
	// parsing in the logs.
	adapter.WithLogContext(map[string]any{
		"server":     s.ID(),
		"request_id": c.GetString("request_id"),
	})

	// Note: SetBackingUp is now handled atomically within the backup function

	go func(b backup.BackupInterface, s *server.Server, logger *log.Entry) {
		// Ensure backup state is always reset, even on panic
		defer func() {
			if r := recover(); r != nil {
				logger.WithField("panic", r).Error("backup operation panicked")
				// Only reset backup flag on panic - normal completion is handled in Backup()
				s.SetBackingUp(false)
			}
		}()
		// ATOMIC REGISTRATION: Register and get accurate queue status
		registry := server.GetBackupOperationRegistry()
		logger.Info("registering backup operation in queue system")
		
		// ATOMIC: Register operation and get queue status atomically
		_, ctx, cancel, err, wasQueued := registry.Register(s.Context(), data.Uuid, s.ID(), server.OperationTypeBackup)
		if err != nil {
			logger.WithError(err).Error("failed to register backup operation")
			s.Events().Publish(server.DaemonMessageEvent, "Failed to register backup: " + err.Error())
			return
		}
		
		// ACCURATE STATE MANAGEMENT: Set state based on actual queue experience
		if wasQueued {
			s.Environment.SetState(environment.ProcessBackupQueuedState)
			s.Events().Publish(server.DaemonMessageEvent, "Backup was queued and slot acquired - starting backup process...")
		} else {
			s.Events().Publish(server.DaemonMessageEvent, "Backup slot available - starting backup process immediately...")
		}
		// Defer cleanup - will run AFTER backup completes
		defer func() {
			logger.Debug("backup goroutine cleanup starting")
			registry.Complete(data.Uuid)
			cancel() // Cancel AFTER marking complete
			logger.Debug("backup goroutine cleanup completed")
		}()

		// Add timeout if not already set
		ctx, timeoutCancel := context.WithTimeout(ctx, 6*time.Hour)
		defer timeoutCancel()

		if err := s.BackupWithRetry(ctx, b, 2); err != nil {
			logger.WithField("error", errors.WithStackIf(err)).Error("router: failed to generate server backup after retries")
			
			// Send failure event to ensure frontend gets notified
			s.Events().Publish(server.BackupCompletedEvent, map[string]any{
				"uuid":          data.Uuid,
				"is_successful": false,
				"error":         err.Error(),
			})
		} else {
			logger.Info("backup completed successfully")
		}
	}(adapter, s, logger)

	c.Status(http.StatusAccepted)
}

// postServerRestoreBackup handles restoring a backup for a server by downloading
// or finding the given backup on the system and then unpacking the archive into
// the server's data directory. If the TruncateDirectory field is provided and
// is true all of the files will be deleted for the server.
//
// This endpoint will block until the backup is fully restored allowing for a
// spinner to be displayed in the Panel UI effectively.
//
// TODO: stop the server if it is running
func postServerRestoreBackup(c *gin.Context) {
	s := middleware.ExtractServer(c)
	client := middleware.ExtractApiClient(c)
	logger := middleware.ExtractLogger(c)

	// RACE CONDITION PROTECTION: Prevent concurrent operations
	if s.IsBackingUp() {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": "A backup operation is already running for this server",
		})
		return
	}
	if s.IsRestoring() {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": "A restore operation is already running for this server",
		})
		return
	}
	if s.IsTransferring() {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": "A transfer operation is already running for this server",
		})
		return
	}

	var data struct {
		Adapter           backup.AdapterType `binding:"required,oneof=wings s3" json:"adapter"`
		TruncateDirectory bool               `json:"truncate_directory"`
		// A UUID is always required for this endpoint, however the download URL
		// is only present when the given adapter type is s3.
		DownloadUrl string `json:"download_url"`
	}
	if err := c.BindJSON(&data); err != nil {
		return
	}
	if data.Adapter == backup.S3BackupAdapter && data.DownloadUrl == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "The download_url field is required when the backup adapter is set to S3."})
		return
	}

	// State management is now handled atomically within the restore function
	// to prevent race conditions with the operation registry

	logger.Info("processing server backup restore request")
	if data.TruncateDirectory {
		logger.Info("received \"truncate_directory\" flag in request: deleting server files")
		if err := s.Filesystem().TruncateRootDirectory(); err != nil {
			middleware.CaptureAndAbort(c, err)
			return
		}
	}

	// Now that we've cleaned up the data directory if necessary, grab the backup file
	// and attempt to restore it into the server directory.
	if data.Adapter == backup.LocalBackupAdapter {
		b, _, err := backup.LocateLocal(client, c.Param("backup"))
		if err != nil {
			middleware.CaptureAndAbort(c, err)
			return
		}
		go func(s *server.Server, b backup.BackupInterface, logger *log.Entry) {
			// ATOMIC REGISTRATION: Register and get accurate queue status
			registry := server.GetBackupOperationRegistry()
			logger.Info("registering local restore operation in queue system")
			
			// ATOMIC: Register operation and get queue status atomically
			_, ctx, cancel, err, wasQueued := registry.Register(s.Context(), c.Param("backup"), s.ID(), server.OperationTypeRestore)
			if err != nil {
				logger.WithError(err).Error("failed to register restore operation")
				s.Events().Publish(server.DaemonMessageEvent, "Failed to register restore: " + err.Error())
				return
			}
			
			// ACCURATE STATE MANAGEMENT: Set state based on actual queue experience
			if wasQueued {
				s.Environment.SetState(environment.ProcessRestoreQueuedState)
				s.Events().Publish(server.DaemonMessageEvent, "Restore was queued and slot acquired - starting restore process...")
			} else {
				s.Events().Publish(server.DaemonMessageEvent, "Restore slot available - starting restore process immediately...")
			}
			defer func() {
				if r := recover(); r != nil {
					logger.WithField("panic", r).Error("restore operation panicked")
				}
				logger.Debug("local restore goroutine cleanup starting")
				registry.Complete(c.Param("backup"))
				cancel() // Cancel AFTER marking complete
				logger.Debug("local restore goroutine cleanup completed")
				// Note: SetRestoring is now handled atomically within the restore function
			}()

			// Add 4-hour timeout for restore operations
			ctx, timeoutCancel := context.WithTimeout(ctx, 4*time.Hour)
			defer timeoutCancel()

			logger.Info("starting restoration process for server backup using local driver")
			if err := s.RestoreBackupWithContext(ctx, b, nil); err != nil {
				logger.WithField("error", err).Error("failed to restore local backup to server")
				s.Events().Publish(server.DaemonMessageEvent, "Failed server restoration from local backup: " + err.Error())
				// BackupRestoreCompletedEvent is now sent by RestoreBackupWithContext
			} else {
				logger.WithFields(log.Fields{
					"is_restoring": s.IsRestoring(),
					"server_state": s.Environment.State(),
				}).Info("Local restore completed successfully")
				
				s.Events().Publish(server.DaemonMessageEvent, "Completed server restoration from local backup.")
				// BackupRestoreCompletedEvent is now sent by RestoreBackupWithContext
				logger.Info("completed server restoration from local backup")
			}
		}(s, b, logger)
		// State cleanup handled atomically by restore operation
		c.Status(http.StatusAccepted)
		return
	}

	// Since this is not a local backup we need to stream the archive and then
	// parse over the contents as we go in order to restore it to the server.
	httpClient := &http.Client{
		Timeout: time.Hour * 2, // 2 hour timeout for large backup downloads
		Transport: &http.Transport{
			MaxIdleConns:        10,
			IdleConnTimeout:     30 * time.Second,
			DisableKeepAlives:   false,
			DisableCompression:  true, // Backup files are already compressed
		},
	}
	logger.WithField("download_url", data.DownloadUrl).Info("downloading backup from remote location...")
	// Use proper timeout to prevent indefinite hangs during backup downloads.
	// 2 hour timeout should be sufficient for most backup file sizes while preventing
	// resource exhaustion from stuck connections.
	req, err := http.NewRequestWithContext(s.Context(), http.MethodGet, data.DownloadUrl, nil)
	if err != nil {
		logger.WithField("error", err).Error("failed to create HTTP request for backup download")
		middleware.CaptureAndAbort(c, err)
		return
	}
	
	logger.Debug("executing HTTP request for backup download")
	downloadStart := time.Now()
	res, err := httpClient.Do(req)
	if err != nil {
		logger.WithFields(log.Fields{
			"error": err,
			"duration_ms": time.Since(downloadStart).Milliseconds(),
		}).Error("HTTP request failed for backup download")
		middleware.CaptureAndAbort(c, err)
		return
	}
	
	logger.WithFields(log.Fields{
		"status_code": res.StatusCode,
		"content_length": res.ContentLength,
		"content_type": res.Header.Get("Content-Type"),
		"duration_ms": time.Since(downloadStart).Milliseconds(),
	}).Info("received HTTP response for backup download")
	
	// CRITICAL: Ensure response body is always closed in error paths before goroutine takes ownership
	var goroutineStarted bool
	defer func() {
		// Only close if goroutine hasn't taken ownership of the response
		if !goroutineStarted && res != nil && res.Body != nil {
			if err := res.Body.Close(); err != nil {
				logger.WithError(err).Warn("failed to close HTTP response body in error path")
			}
		}
	}()
	
	// Validate content types for supported backup formats using extensible compression registry
	contentType := res.Header.Get("Content-Type")
	if contentType == "" {
		// Accept empty content type (some S3 providers don't set it)
	} else if !backup.IsValidBackupContentType(contentType) {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "The provided backup link has an unsupported content type. \"" + contentType + "\" is not a supported backup format (gzip, zstd, or tar).",
		})
		return
	}
	
	// Mark that goroutine will take ownership of the response
	goroutineStarted = true

	go func(s *server.Server, uuid string, logger *log.Entry) {
		// CRITICAL: Always close response body to prevent resource leak
		defer res.Body.Close()
		
		// ATOMIC REGISTRATION: Register and get accurate queue status
		registry := server.GetBackupOperationRegistry()
		logger.Info("registering S3 restore operation in queue system")
		
		// ATOMIC: Register operation and get queue status atomically
		_, ctx, cancel, err, wasQueued := registry.Register(s.Context(), uuid, s.ID(), server.OperationTypeRestore)
		if err != nil {
			logger.WithError(err).Error("failed to register S3 restore operation")
			s.Events().Publish(server.DaemonMessageEvent, "Failed to register S3 restore: " + err.Error())
			return
		}
		
		// ACCURATE STATE MANAGEMENT: Set state based on actual queue experience
		if wasQueued {
			s.Environment.SetState(environment.ProcessRestoreQueuedState)
			s.Events().Publish(server.DaemonMessageEvent, "S3 restore was queued and slot acquired - starting restore process...")
		} else {
			s.Events().Publish(server.DaemonMessageEvent, "S3 restore slot available - starting restore process immediately...")
		}
		defer func() {
			if r := recover(); r != nil {
				logger.WithField("panic", r).Error("S3 restore operation panicked")
			}
			logger.Debug("S3 restore goroutine cleanup starting")
			registry.Complete(uuid)
			cancel() // Cancel AFTER marking complete
			logger.Debug("S3 restore goroutine cleanup completed")
			// Note: SetRestoring is now handled atomically within the restore function
		}()

		// Add 4-hour timeout for restore operations
		ctx, timeoutCancel := context.WithTimeout(ctx, 4*time.Hour)
		defer timeoutCancel()

		logger.WithField("content_length", res.ContentLength).Info("starting restoration process for server backup using S3 driver")
		
		// Create S3 backup instance
		s3Backup := backup.NewS3(client, uuid, "")
		
		// Wrap response body with download progress tracking if we know the size
		var downloadReader io.ReadCloser = res.Body
		if res.ContentLength > 0 {
			logger.WithField("size_mb", res.ContentLength/(1024*1024)).Info("S3 backup download size known, adding download progress tracking")
			
			// Progress callback for download tracking
			onProgress := func(downloaded, total int64) {
				// Calculate download percentage (0-80% for download, 80-100% for extraction)
				percentage := 0
				if total > 0 {
					percentage = int((downloaded * 80) / total)
				}
				
				// Log progress (WebSocket events are handled by server layer)
				if percentage%10 == 0 {
					logger.WithFields(log.Fields{
						"downloaded_percentage": percentage,
						"downloaded_mb": downloaded / (1024 * 1024),
						"total_mb": total / (1024 * 1024),
					}).Debug("S3 download progress")
				}
			}
			
			downloadReader = backup.NewDownloadProgressReader(res.Body, res.ContentLength, uuid, onProgress)
		}
		
		if err := s.RestoreBackupWithContext(ctx, s3Backup, downloadReader); err != nil {
			logger.WithField("error", errors.WithStack(err)).Error("failed to restore remote S3 backup to server")
			s.Events().Publish(server.DaemonMessageEvent, "Failed server restoration from S3 backup: " + err.Error())
			// BackupRestoreCompletedEvent is now sent by RestoreBackupWithContext
		} else {
			logger.WithFields(log.Fields{
				"is_restoring": s.IsRestoring(),
				"server_state": s.Environment.State(),
			}).Info("S3 restore completed successfully")
			
			s.Events().Publish(server.DaemonMessageEvent, "Completed server restoration from S3 backup.")
			// BackupRestoreCompletedEvent is now sent by RestoreBackupWithContext
			logger.Info("completed server restoration from S3 backup")
		}
	}(s, c.Param("backup"), logger)

	// State cleanup handled atomically by restore operation
	c.Status(http.StatusAccepted)
}

// deleteServerBackup deletes a backup file of a server. This now supports both Local and S3 backups
// for consistent behavior (WORK.md compliance). If the backup is not found on the machine just return a 404 error.
func deleteServerBackup(c *gin.Context) {
	client := middleware.ExtractApiClient(c)
	backupID := c.Param("backup")
	
	// UNIFIED BEHAVIOR: Try to locate and delete backup regardless of type (Local or S3)
	// This ensures consistent deletion behavior between storage types (WORK.md requirement)
	
	// First try to locate as Local backup
	if localBackup, _, err := backup.LocateLocal(client, backupID); err == nil {
		// Found as Local backup - delete it
		if err := localBackup.Remove(); err != nil && !errors.Is(err, os.ErrNotExist) {
			middleware.CaptureAndAbort(c, err)
			return
		}
		c.Status(http.StatusNoContent)
		return
	}
	
	// If not found as Local backup, check if it's an S3 backup file that exists locally
	// S3 backups may leave local files behind after failed uploads or for debugging
	s3Backup := backup.NewS3(client, backupID, "")
	if _, err := os.Stat(s3Backup.Path()); err == nil {
		// Found S3 backup file locally - delete it
		if err := s3Backup.Remove(); err != nil && !errors.Is(err, os.ErrNotExist) {
			middleware.CaptureAndAbort(c, err)
			return
		}
		c.Status(http.StatusNoContent)
		return
	}
	
	// If neither Local nor S3 backup file found, return 404
	c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
		"error": "The requested backup was not found on this server.",
	})
}

// cancelServerBackup cancels a running backup operation for a server.
// This endpoint allows clients to cancel backup operations that are currently in progress.
func cancelServerBackup(c *gin.Context) {
	s := middleware.ExtractServer(c)
	logger := middleware.ExtractLogger(c)

	backupID := c.Param("backup")
	if backupID == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "Backup ID is required",
		})
		return
	}

	registry := server.GetBackupOperationRegistry()

	// Get the operation to verify it belongs to this server
	operation, exists := registry.Get(backupID)
	if !exists {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "Backup operation not found or already completed",
		})
		return
	}

	// Verify the operation belongs to this server
	if operation.ServerID != s.ID() {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
			"error": "Backup operation does not belong to this server",
		})
		return
	}

	// Cancel the operation
	if err := registry.Cancel(backupID); err != nil {
		logger.WithField("backup_id", backupID).WithError(err).Error("failed to cancel backup operation")
		middleware.CaptureAndAbort(c, err)
		return
	}

	logger.WithFields(log.Fields{
		"backup_id": backupID,
		"server":    s.ID(),
		"type":      operation.Type,
	}).Info("backup operation cancelled via API")

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Backup operation cancelled successfully",
	})
}

// getServerBackupOperations returns all currently running backup operations for a server.
// This endpoint allows clients to see what backup/restore operations are currently active.
func getServerBackupOperations(c *gin.Context) {
	s := middleware.ExtractServer(c)
	registry := server.GetBackupOperationRegistry()

	operations := registry.List(s.ID())

	// Convert operations to JSON-safe format
	type OperationResponse struct {
		ID        string               `json:"id"`
		BackupID  string               `json:"backup_id"`
		Type      server.OperationType `json:"type"`
		StartTime int64                `json:"start_time"`
	}

	var response []OperationResponse
	for _, op := range operations {
		opResponse := OperationResponse{
			ID:        op.ID,
			BackupID:  op.BackupID,
			Type:      op.Type,
			StartTime: op.StartTime,
		}

		response = append(response, opResponse)
	}

	c.JSON(http.StatusOK, gin.H{
		"operations": response,
		"count":      len(response),
	})
}
