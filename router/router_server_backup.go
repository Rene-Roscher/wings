package router

import (
	"context"
	"net/http"
	"os"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/gin-gonic/gin"

	"github.com/Rene-Roscher/wings/router/middleware"
	"github.com/Rene-Roscher/wings/server"
	"github.com/Rene-Roscher/wings/server/backup"
)

// postServerBackup performs a backup against a given server instance using the
// provided backup adapter.
func postServerBackup(c *gin.Context) {
	s := middleware.ExtractServer(c)
	client := middleware.ExtractApiClient(c)
	logger := middleware.ExtractLogger(c)
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

	go func(b backup.BackupInterface, s *server.Server, logger *log.Entry) {
		// Register operation for cancellation support
		registry := server.GetBackupOperationRegistry()
		_, ctx, cancel := registry.Register(data.Uuid, s.ID(), server.OperationTypeBackup)
		defer func() {
			cancel()
			registry.Complete(data.Uuid)
		}()
		
		// Add timeout if not already set
		ctx, timeoutCancel := context.WithTimeout(ctx, 6*time.Hour)
		defer timeoutCancel()
		
		if err := s.BackupWithContext(ctx, b); err != nil {
			logger.WithField("error", errors.WithStackIf(err)).Error("router: failed to generate server backup")
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

	s.SetRestoring(true)
	hasError := true
	defer func() {
		if hasError {
			s.SetRestoring(false)
		}
	}()

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
			defer s.SetRestoring(false) // Ensure restoring state is always reset
			logger.Info("starting restoration process for server backup using local driver")
			if err := s.RestoreBackup(b, nil); err != nil {
				logger.WithField("error", err).Error("failed to restore local backup to server")
			}
			s.Events().Publish(server.DaemonMessageEvent, "Completed server restoration from local backup.")
			s.Events().Publish(server.BackupRestoreCompletedEvent, "")
			logger.Info("completed server restoration from local backup")
		}(s, b, logger)
		hasError = false
		c.Status(http.StatusAccepted)
		return
	}

	// Since this is not a local backup we need to stream the archive and then
	// parse over the contents as we go in order to restore it to the server.
	httpClient := http.Client{
		Timeout: time.Hour * 2, // 2 hour timeout for large backup downloads
	}
	logger.Info("downloading backup from remote location...")
	// Use proper timeout to prevent indefinite hangs during backup downloads.
	// 2 hour timeout should be sufficient for most backup file sizes while preventing
	// resource exhaustion from stuck connections.
	req, err := http.NewRequestWithContext(s.Context(), http.MethodGet, data.DownloadUrl, nil)
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	res, err := httpClient.Do(req)
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	// Don't allow content types that we know are going to give us problems.
	if res.Header.Get("Content-Type") == "" || !strings.Contains("application/x-gzip application/gzip", res.Header.Get("Content-Type")) {
		_ = res.Body.Close()
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
			"error": "The provided backup link is not a supported content type. \"" + res.Header.Get("Content-Type") + "\" is not application/x-gzip.",
		})
		return
	}

	go func(s *server.Server, uuid string, logger *log.Entry) {
		defer s.SetRestoring(false) // Ensure restoring state is always reset
		logger.Info("starting restoration process for server backup using S3 driver")
		if err := s.RestoreBackup(backup.NewS3(client, uuid, ""), res.Body); err != nil {
			logger.WithField("error", errors.WithStack(err)).Error("failed to restore remote S3 backup to server")
		}
		s.Events().Publish(server.DaemonMessageEvent, "Completed server restoration from S3 backup.")
		s.Events().Publish(server.BackupRestoreCompletedEvent, "")
		logger.Info("completed server restoration from S3 backup")
	}(s, c.Param("backup"), logger)

	hasError = false
	c.Status(http.StatusAccepted)
}

// deleteServerBackup deletes a local backup of a server. If the backup is not
// found on the machine just return a 404 error. The service calling this
// endpoint can make its own decisions as to how it wants to handle that
// response.
func deleteServerBackup(c *gin.Context) {
	b, _, err := backup.LocateLocal(middleware.ExtractApiClient(c), c.Param("backup"))
	if err != nil {
		// Just return from the function at this point if the backup was not located.
		if errors.Is(err, os.ErrNotExist) {
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
				"error": "The requested backup was not found on this server.",
			})
			return
		}
		middleware.CaptureAndAbort(c, err)
		return
	}
	// I'm not entirely sure how likely this is to happen, however if we did manage to
	// locate the backup previously and it is now missing when we go to delete, just
	// treat it as having been successful, rather than returning a 404.
	if err := b.Remove(); err != nil && !errors.Is(err, os.ErrNotExist) {
		middleware.CaptureAndAbort(c, err)
		return
	}
	c.Status(http.StatusNoContent)
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
		ID        string                `json:"id"`
		BackupID  string                `json:"backup_id"`
		Type      server.OperationType  `json:"type"`
		StartTime int64                 `json:"start_time"`
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
