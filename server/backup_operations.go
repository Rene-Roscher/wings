package server

import (
	"context"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/google/uuid"
)

// OperationType represents the type of backup operation
type OperationType string

const (
	// OperationTypeBackup represents a backup creation operation
	OperationTypeBackup OperationType = "backup"
	// OperationTypeRestore represents a backup restoration operation
	OperationTypeRestore OperationType = "restore"
)

// BackupOperation represents a running backup or restore operation
type BackupOperation struct {
	// ID is the unique identifier for this operation
	ID string `json:"id"`
	// BackupID is the backup UUID this operation is for
	BackupID string `json:"backup_id"`
	// ServerID is the server UUID this operation is for
	ServerID string `json:"server_id"`
	// Type indicates if this is a backup or restore operation
	Type OperationType `json:"type"`
	// Context is the cancellable context for this operation
	Context context.Context `json:"-"`
	// Cancel is the cancellation function
	Cancel context.CancelFunc `json:"-"`
	// StartTime is when the operation started (Unix timestamp)
	StartTime int64 `json:"start_time"`
}

// BackupOperationRegistry tracks running backup and restore operations
// allowing them to be cancelled via API calls
type BackupOperationRegistry struct {
	mu         sync.RWMutex
	operations map[string]*BackupOperation
	logger     *log.Entry
}

// NewBackupOperationRegistry creates a new operation registry
func NewBackupOperationRegistry() *BackupOperationRegistry {
	return &BackupOperationRegistry{
		operations: make(map[string]*BackupOperation),
		logger:     log.WithField("component", "backup_registry"),
	}
}

// Register registers a new backup operation for tracking and cancellation
func (r *BackupOperationRegistry) Register(backupID, serverID string, opType OperationType) (*BackupOperation, context.Context, context.CancelFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()

	operationID := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())

	operation := &BackupOperation{
		ID:        operationID,
		BackupID:  backupID,
		ServerID:  serverID,
		Type:      opType,
		Context:   ctx,
		Cancel:    cancel,
		StartTime: time.Now().Unix(),
	}

	// Check if operation already exists (shouldn't happen with proper state management)
	if existing, exists := r.operations[backupID]; exists {
		r.logger.WithFields(log.Fields{
			"backup_id":    backupID,
			"existing_id":  existing.ID,
			"new_id":       operationID,
			"type":         opType,
		}).Warn("backup operation already exists, replacing")
	}

	r.operations[backupID] = operation

	r.logger.WithFields(log.Fields{
		"operation_id": operationID,
		"backup_id":    backupID,
		"server_id":    serverID,
		"type":         opType,
	}).Info("registered backup operation")

	return operation, ctx, cancel
}

// Cancel cancels a backup operation by backup ID
func (r *BackupOperationRegistry) Cancel(backupID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	operation, exists := r.operations[backupID]
	if !exists {
		return errors.New("backup operation not found or already completed")
	}

	r.logger.WithFields(log.Fields{
		"operation_id": operation.ID,
		"backup_id":    backupID,
		"server_id":    operation.ServerID,
		"type":         operation.Type,
	}).Info("cancelling backup operation")

	// Cancel the context
	operation.Cancel()

	// Remove from registry
	delete(r.operations, backupID)

	return nil
}

// Get retrieves a backup operation by backup ID
func (r *BackupOperationRegistry) Get(backupID string) (*BackupOperation, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	operation, exists := r.operations[backupID]
	return operation, exists
}

// List returns all currently running operations for a server
func (r *BackupOperationRegistry) List(serverID string) []*BackupOperation {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var operations []*BackupOperation
	for _, op := range r.operations {
		if op.ServerID == serverID {
			operations = append(operations, op)
		}
	}

	return operations
}

// Complete removes a completed operation from the registry
func (r *BackupOperationRegistry) Complete(backupID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if operation, exists := r.operations[backupID]; exists {
		r.logger.WithFields(log.Fields{
			"operation_id": operation.ID,
			"backup_id":    backupID,
			"server_id":    operation.ServerID,
			"type":         operation.Type,
		}).Info("backup operation completed")

		delete(r.operations, backupID)
	}
}

// Count returns the total number of running operations
func (r *BackupOperationRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.operations)
}

// CountForServer returns the number of running operations for a specific server
func (r *BackupOperationRegistry) CountForServer(serverID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, op := range r.operations {
		if op.ServerID == serverID {
			count++
		}
	}

	return count
}

// CleanupStaleOperations removes operations that have been running longer than maxDuration
func (r *BackupOperationRegistry) CleanupStaleOperations(maxDuration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().Unix()

	for backupID, operation := range r.operations {
		if now-operation.StartTime > int64(maxDuration.Seconds()) {
			r.logger.WithFields(log.Fields{
				"operation_id": operation.ID,
				"backup_id":    backupID,
				"server_id":    operation.ServerID,
				"type":         operation.Type,
				"duration":     time.Duration(now-operation.StartTime) * time.Second,
			}).Warn("cleaning up stale backup operation")

			// Cancel the stale operation
			operation.Cancel()
			delete(r.operations, backupID)
		}
	}
}

// Global backup operation registry instance
var backupOperationRegistry = NewBackupOperationRegistry()

// GetBackupOperationRegistry returns the global backup operation registry
func GetBackupOperationRegistry() *BackupOperationRegistry {
	return backupOperationRegistry
}

// StartBackupOperationCleanup starts a background goroutine that periodically cleans up stale operations
func StartBackupOperationCleanup(ctx context.Context) {
	ticker := time.NewTicker(time.Minute * 5) // Check every 5 minutes
	defer ticker.Stop()

	log.Info("starting backup operation cleanup goroutine")

	for {
		select {
		case <-ticker.C:
			// Clean up operations older than 8 hours (backup timeout is 6h, restore is 4h)
			backupOperationRegistry.CleanupStaleOperations(time.Hour * 8)
		case <-ctx.Done():
			log.Info("stopping backup operation cleanup goroutine")
			return
		}
	}
}
