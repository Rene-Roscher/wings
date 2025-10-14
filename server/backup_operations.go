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
	// CRITICAL FIX: Store semaphore token to prevent leaks
	// This channel MUST be used to return the token when operation completes
	semaphoreToken chan struct{} `json:"-"`
}

// BackupOperationRegistry tracks running backup and restore operations
// allowing them to be cancelled via API calls with concurrency limits and queuing
type BackupOperationRegistry struct {
	mu         sync.RWMutex
	operations map[string]*BackupOperation
	logger     *log.Entry
	// CRITICAL: Operation limits to prevent resource exhaustion with queuing support
	maxConcurrentBackups  int
	maxConcurrentRestores int
	// QUEUING: Semaphores to handle waiting instead of immediate rejection  
	backupSemaphore  chan struct{}
	restoreSemaphore chan struct{}
}

// NewBackupOperationRegistry creates a new operation registry with resource limits and queuing
func NewBackupOperationRegistry() *BackupOperationRegistry {
	maxBackups := 8
	maxRestores := 8
	
	return &BackupOperationRegistry{
		operations: make(map[string]*BackupOperation),
		logger:     log.WithField("component", "backup_registry"),
		// UPDATED: Higher limits as requested - 8 concurrent backups and 8 restores
		maxConcurrentBackups:  maxBackups,
		maxConcurrentRestores: maxRestores,
		// QUEUING: Semaphore channels for controlled concurrency with waiting
		backupSemaphore:  make(chan struct{}, maxBackups),
		restoreSemaphore: make(chan struct{}, maxRestores),
	}
}

// Register registers a new backup operation for tracking and cancellation with queuing
// CRITICAL: Now accepts parent context and uses semaphore queuing instead of immediate rejection
// Returns: operation, context, cancelFunc, error, wasQueued
func (r *BackupOperationRegistry) Register(parentCtx context.Context, backupID, serverID string, opType OperationType) (*BackupOperation, context.Context, context.CancelFunc, error, bool) {
	// QUEUING: Acquire semaphore slot - will wait if limit reached
	var semaphore chan struct{}
	switch opType {
	case OperationTypeBackup:
		semaphore = r.backupSemaphore
	case OperationTypeRestore:
		semaphore = r.restoreSemaphore
	default:
		return nil, nil, nil, errors.New("invalid operation type"), false
	}

	// ATOMIC: Check if we need to wait (for accurate state reporting)
	needsQueue := len(semaphore) >= cap(semaphore)
	
	// WAIT in queue until slot available or context cancelled
	select {
	case semaphore <- struct{}{}: // Successfully acquired slot
		r.logger.WithFields(log.Fields{
			"backup_id": backupID,
			"type":      opType,
			"was_queued": needsQueue,
		}).Debug("acquired operation slot from queue")
	case <-parentCtx.Done():
		return nil, nil, nil, errors.Wrap(parentCtx.Err(), "cancelled while waiting in operation queue"), needsQueue
	}

	// Now proceed with registration
	r.mu.Lock()
	defer r.mu.Unlock()

	operationID := uuid.New().String()
	// CRITICAL: Use parent context to ensure cancellation propagation
	ctx, cancel := context.WithCancel(parentCtx)

	operation := &BackupOperation{
		ID:             operationID,
		BackupID:       backupID,
		ServerID:       serverID,
		Type:           opType,
		Context:        ctx,
		Cancel:         cancel,
		StartTime:      time.Now().Unix(),
		semaphoreToken: semaphore, // CRITICAL FIX: Store token reference
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
		"total_ops":    len(r.operations),
	}).Info("registered backup operation")

	return operation, ctx, cancel, nil, needsQueue
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

	// CRITICAL FIX: Release semaphore token BLOCKING (prevents leaks)
	// This MUST be done before deleting the operation
	if operation.semaphoreToken != nil {
		<-operation.semaphoreToken // BLOCKING receive to return token
		r.logger.Debug("released semaphore slot after cancellation")
	}

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

// Complete removes a completed operation from the registry and releases semaphore slot
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

		// CRITICAL FIX: Release semaphore token BLOCKING (prevents leaks)
		if operation.semaphoreToken != nil {
			<-operation.semaphoreToken // BLOCKING receive to return token
			r.logger.Debug("released semaphore slot after completion")
		}

		delete(r.operations, backupID)
	}
}

// Count returns the total number of running operations
func (r *BackupOperationRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.operations)
}

// GetQueueStatus returns detailed queue status for monitoring
func (r *BackupOperationRegistry) GetQueueStatus() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	backupCount := 0
	restoreCount := 0
	for _, op := range r.operations {
		switch op.Type {
		case OperationTypeBackup:
			backupCount++
		case OperationTypeRestore:
			restoreCount++
		}
	}
	
	return map[string]any{
		"backups": map[string]any{
			"active":    backupCount,
			"max":       r.maxConcurrentBackups,
			"available": r.maxConcurrentBackups - backupCount,
			"queue_length": len(r.backupSemaphore),
		},
		"restores": map[string]any{
			"active":    restoreCount,
			"max":       r.maxConcurrentRestores,
			"available": r.maxConcurrentRestores - restoreCount,
			"queue_length": len(r.restoreSemaphore),
		},
		"total_operations": len(r.operations),
	}
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

			// CRITICAL FIX: Release semaphore token BLOCKING (prevents leaks)
			if operation.semaphoreToken != nil {
				<-operation.semaphoreToken // BLOCKING receive to return token
				r.logger.Debug("released semaphore slot during cleanup")
			}

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

// CancelAllForServer cancels all running backup operations for a specific server
// This is used during server deletion to ensure proper cleanup
func (r *BackupOperationRegistry) CancelAllForServer(serverID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var cancelledOps []string
	
	for backupID, operation := range r.operations {
		if operation.ServerID == serverID {
			r.logger.WithFields(log.Fields{
				"operation_id": operation.ID,
				"backup_id":    backupID,
				"server_id":    serverID,
				"type":         operation.Type,
			}).Info("cancelling backup operation for server deletion")

			// Cancel the context
			operation.Cancel()
			
			// CRITICAL FIX: Release semaphore token BLOCKING (prevents leaks)
			if operation.semaphoreToken != nil {
				<-operation.semaphoreToken // BLOCKING receive to return token
				r.logger.Debug("released semaphore slot for server deletion")
			}
			
			// Remove from registry
			delete(r.operations, backupID)
			cancelledOps = append(cancelledOps, backupID)
		}
	}

	if len(cancelledOps) > 0 {
		r.logger.WithFields(log.Fields{
			"server_id":        serverID,
			"cancelled_count":  len(cancelledOps),
			"cancelled_ops":    cancelledOps,
		}).Info("cancelled all backup operations for server deletion")
	}

	return nil
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
