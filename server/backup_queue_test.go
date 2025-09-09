package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackupOperationRegistryBasics tests the core queue functionality
func TestBackupOperationRegistryBasics(t *testing.T) {
	registry := NewBackupOperationRegistry()
	ctx := context.Background()

	// Test 1: Basic registration
	op, opCtx, cancel, err, wasQueued := registry.Register(ctx, "backup1", "server1", OperationTypeBackup)
	require.NoError(t, err)
	assert.False(t, wasQueued, "First registration should not be queued")
	assert.NotNil(t, op)
	assert.NotNil(t, opCtx)
	assert.NotNil(t, cancel)

	// Test 2: Operation retrieval
	retrieved, exists := registry.Get("backup1")
	assert.True(t, exists)
	assert.Equal(t, op.ID, retrieved.ID)

	// Test 3: Operation completion
	registry.Complete("backup1")
	_, exists = registry.Get("backup1")
	assert.False(t, exists, "Operation should be removed after completion")

	cancel() // Cleanup
}

// TestBackupQueueConcurrencyLimits tests the 8 concurrent backup limit
func TestBackupQueueConcurrencyLimits(t *testing.T) {
	registry := NewBackupOperationRegistry()
	ctx := context.Background()

	var operations []*BackupOperation
	var cancels []context.CancelFunc

	// Fill up all 8 backup slots
	for i := 0; i < 8; i++ {
		op, _, cancel, err, wasQueued := registry.Register(ctx, 
			"backup"+string(rune(i+'0')), "server1", OperationTypeBackup)
		require.NoError(t, err)
		assert.False(t, wasQueued, "First 8 operations should not be queued")
		
		operations = append(operations, op)
		cancels = append(cancels, cancel)
	}

	// Test queue status
	status := registry.GetQueueStatus()
	backupStatus := status["backups"].(map[string]any)
	assert.Equal(t, 8, backupStatus["active"])
	assert.Equal(t, 0, backupStatus["available"])

	// Test 9th operation should wait (we'll test this with a timeout)
	waitCtx, waitCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer waitCancel()

	_, _, _, err, wasQueued := registry.Register(waitCtx, "backup9", "server1", OperationTypeBackup)
	assert.Error(t, err, "9th operation should timeout while waiting")
	assert.True(t, wasQueued, "9th operation should be detected as queued")

	// Cleanup: Complete one operation and verify slot becomes available
	registry.Complete("backup0")
	
	// Now 9th operation should succeed (it won't queue since slot is available)
	_, _, cancel9, err, wasQueued := registry.Register(ctx, "backup9", "server1", OperationTypeBackup)
	require.NoError(t, err)
	assert.False(t, wasQueued, "Operation should not be queued since slot was available")

	// Cleanup all
	for _, cancel := range cancels[1:] { // Skip cancelled[0] as we already completed it
		cancel()
	}
	cancel9()
	registry.Complete("backup9")
}

// TestBackupRestoreSeparateLimits tests that backup and restore have separate 8-slot limits
func TestBackupRestoreSeparateLimits(t *testing.T) {
	registry := NewBackupOperationRegistry()
	ctx := context.Background()

	var backupCancels []context.CancelFunc
	var restoreCancels []context.CancelFunc

	// Fill up all 8 backup slots
	for i := 0; i < 8; i++ {
		_, _, cancel, err, wasQueued := registry.Register(ctx, 
			"backup"+string(rune(i+'0')), "server1", OperationTypeBackup)
		require.NoError(t, err)
		assert.False(t, wasQueued)
		backupCancels = append(backupCancels, cancel)
	}

	// Fill up all 8 restore slots (should not be affected by backup slots)
	for i := 0; i < 8; i++ {
		_, _, cancel, err, wasQueued := registry.Register(ctx, 
			"restore"+string(rune(i+'0')), "server1", OperationTypeRestore)
		require.NoError(t, err)
		assert.False(t, wasQueued, "Restore slots should be independent of backup slots")
		restoreCancels = append(restoreCancels, cancel)
	}

	// Verify status shows both types are at capacity
	status := registry.GetQueueStatus()
	backupStatus := status["backups"].(map[string]any)
	restoreStatus := status["restores"].(map[string]any)
	
	assert.Equal(t, 8, backupStatus["active"])
	assert.Equal(t, 0, backupStatus["available"])
	assert.Equal(t, 8, restoreStatus["active"])
	assert.Equal(t, 0, restoreStatus["available"])
	assert.Equal(t, 16, status["total_operations"]) // 8 backup + 8 restore

	// Cleanup
	for _, cancel := range backupCancels {
		cancel()
	}
	for _, cancel := range restoreCancels {
		cancel()
	}
}

// TestBackupOperationCancellation tests operation cancellation and cleanup
func TestBackupOperationCancellation(t *testing.T) {
	registry := NewBackupOperationRegistry()
	ctx := context.Background()

	// Register operation
	_, opCtx, cancel, err, _ := registry.Register(ctx, "backup1", "server1", OperationTypeBackup)
	require.NoError(t, err)

	// Verify operation exists
	_, exists := registry.Get("backup1")
	assert.True(t, exists)

	// Cancel operation
	err = registry.Cancel("backup1")
	require.NoError(t, err)

	// Verify operation was removed
	_, exists = registry.Get("backup1")
	assert.False(t, exists, "Operation should be removed after cancellation")

	// Verify context was cancelled
	select {
	case <-opCtx.Done():
		// Good, context was cancelled
	case <-time.After(100 * time.Millisecond):
		t.Error("Operation context should have been cancelled")
	}

	cancel() // Cleanup
}

// TestServerDeletionCleanup tests cleanup when server is deleted
func TestServerDeletionCleanup(t *testing.T) {
	registry := NewBackupOperationRegistry()
	ctx := context.Background()

	var cancels []context.CancelFunc

	// Create operations for multiple servers
	_, _, cancel1, err, _ := registry.Register(ctx, "backup1", "server1", OperationTypeBackup)
	require.NoError(t, err)
	cancels = append(cancels, cancel1)

	_, _, cancel2, err, _ := registry.Register(ctx, "backup2", "server1", OperationTypeBackup)
	require.NoError(t, err)
	cancels = append(cancels, cancel2)

	_, _, cancel3, err, _ := registry.Register(ctx, "backup3", "server2", OperationTypeBackup)
	require.NoError(t, err)
	cancels = append(cancels, cancel3)

	// Verify all operations exist
	assert.Equal(t, 3, registry.Count())
	assert.Equal(t, 2, registry.CountForServer("server1"))
	assert.Equal(t, 1, registry.CountForServer("server2"))

	// Cancel all operations for server1
	err = registry.CancelAllForServer("server1")
	require.NoError(t, err)

	// Verify only server1 operations were cancelled
	assert.Equal(t, 1, registry.Count(), "Only server2 operation should remain")
	assert.Equal(t, 0, registry.CountForServer("server1"))
	assert.Equal(t, 1, registry.CountForServer("server2"))

	// Cleanup remaining
	cancel3()
}

// TestConcurrentAccess tests thread safety of the registry
func TestConcurrentAccess(t *testing.T) {
	registry := NewBackupOperationRegistry()
	ctx := context.Background()

	const numGoroutines = 10
	const operationsPerGoroutine = 5

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*operationsPerGoroutine)

	// Launch multiple goroutines registering operations concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			
			for j := 0; j < operationsPerGoroutine; j++ {
				backupID := "backup_" + string(rune(goroutineID+'0')) + "_" + string(rune(j+'0'))
				serverID := "server" + string(rune(goroutineID+'0'))
				
				_, _, cancel, err, _ := registry.Register(ctx, backupID, serverID, OperationTypeBackup)
				if err != nil {
					errors <- err
					return
				}
				
				// Immediately complete to free up slots
				registry.Complete(backupID)
				if cancel != nil {
					cancel()
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		t.Errorf("Concurrent access error: %v", err)
	}

	// Verify registry is clean
	assert.Equal(t, 0, registry.Count(), "Registry should be empty after all operations completed")
}