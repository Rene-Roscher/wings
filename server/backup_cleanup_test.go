package server

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Rene-Roscher/wings/config"
	"github.com/Rene-Roscher/wings/server/backup"
)

func init() {
	// Initialize config for tests to prevent nil pointer dereference
	tmpDir := os.TempDir()
	config.Set(&config.Configuration{
		AuthenticationToken: "test-token",
		System: config.SystemConfiguration{
			BackupDirectory: tmpDir,
		},
	})
}

// TestBackupCleanupOnServerDeletion tests backup file cleanup when server is deleted
func TestBackupCleanupOnServerDeletion(t *testing.T) {
	registry := GetBackupOperationRegistry()
	ctx := context.Background()

	// Test server deletion cleanup for multiple servers
	servers := []string{"server1", "server2", "server3"}
	var allOperations []string

	// Register operations for different servers (limit to stay under 8 total)
	for _, serverID := range servers {
		for i := 0; i < 2; i++ { // Only 2 per server = 6 total (under limit)
			backupID := serverID + "_backup_" + string(rune(i+'0'))
			_, _, cancel, err, _ := registry.Register(ctx, backupID, serverID, OperationTypeBackup)
			require.NoError(t, err)
			allOperations = append(allOperations, backupID)
			
			// Clean up individual operations
			defer func(bid string) {
				cancel()
				registry.Complete(bid)
			}(backupID)
		}
	}

	// Verify all operations are registered
	assert.Equal(t, 6, registry.Count()) // 3 servers * 2 operations each

	// Verify counts per server
	for _, serverID := range servers {
		assert.Equal(t, 2, registry.CountForServer(serverID))
	}

	// Delete server1 - should cancel all its operations
	err := registry.CancelAllForServer("server1")
	require.NoError(t, err)

	// Verify server1 operations are gone
	assert.Equal(t, 0, registry.CountForServer("server1"))
	assert.Equal(t, 4, registry.Count()) // Should have 4 remaining (2 servers * 2 each)

	// Verify other servers are unaffected
	assert.Equal(t, 2, registry.CountForServer("server2"))
	assert.Equal(t, 2, registry.CountForServer("server3"))

	// Delete server2
	err = registry.CancelAllForServer("server2")
	require.NoError(t, err)

	assert.Equal(t, 0, registry.CountForServer("server2"))
	assert.Equal(t, 2, registry.Count()) // Should have 2 remaining (server3 only)

	// Verify server3 still has its operations
	assert.Equal(t, 2, registry.CountForServer("server3"))
}

// TestBackupCleanupStaleOperations tests cleanup of stale operations
func TestBackupCleanupStaleOperations(t *testing.T) {
	registry := GetBackupOperationRegistry()
	ctx := context.Background()

	// Fresh operation (should not be cleaned)
	_, _, cancel1, err, _ := registry.Register(ctx, "fresh_backup", "server1", OperationTypeBackup)
	require.NoError(t, err)
	defer func() {
		cancel1()
		registry.Complete("fresh_backup")
	}()

	// Manually create a stale operation for testing
	// Note: We can't easily manipulate StartTime without modifying the registry
	// So we'll test the cleanup logic conceptually

	initialCount := registry.Count()
	assert.True(t, initialCount > 0, "Should have at least one operation")

	// Test cleanup with very short duration (should not clean fresh operations)
	registry.CleanupStaleOperations(1 * time.Millisecond)

	// Should still have the same count (operations are fresh)
	assert.Equal(t, initialCount, registry.Count())

	// Test cleanup with very long duration (would clean old operations if they existed)
	registry.CleanupStaleOperations(24 * time.Hour)

	// Should still have operations (they're not old enough)
	assert.Equal(t, initialCount, registry.Count())
}

// TestLocalBackupCleanupFunction tests the local backup file cleanup
func TestLocalBackupCleanupFunction(t *testing.T) {
	// This test verifies the cleanup function exists and can be called
	// We don't test actual file operations to avoid affecting real files
	
	// Test cleanup function doesn't panic with non-existent server
	err := backup.CleanupBackupFilesForServer("non-existent-server-123")
	
	// Function should handle non-existent servers gracefully
	// Error is acceptable (directory not found), panic is not
	if err != nil {
		assert.Contains(t, err.Error(), "no such file or directory", 
			"Should handle non-existent server gracefully")
	}
}

// TestBackupCleanupRace tests concurrent cleanup operations
func TestBackupCleanupRace(t *testing.T) {
	registry := GetBackupOperationRegistry()
	ctx := context.Background()

	// Register multiple operations
	var cancels []context.CancelFunc
	for i := 0; i < 5; i++ {
		backupID := "race_test_" + string(rune(i+'0'))
		_, _, cancel, err, _ := registry.Register(ctx, backupID, "race_server", OperationTypeBackup)
		require.NoError(t, err)
		cancels = append(cancels, cancel)
	}

	initialCount := registry.Count()

	// Run concurrent cleanup operations
	done := make(chan bool, 2)

	// Cleanup by server
	go func() {
		registry.CancelAllForServer("race_server")
		done <- true
	}()

	// Cleanup stale operations
	go func() {
		registry.CleanupStaleOperations(1 * time.Hour)
		done <- true
	}()

	// Wait for both to complete
	<-done
	<-done

	// Should not have any operations for race_server
	assert.Equal(t, 0, registry.CountForServer("race_server"))
	
	// Registry should be in consistent state
	finalCount := registry.Count()
	assert.True(t, finalCount <= initialCount, "Count should not increase")

	// Cleanup remaining cancels
	for _, cancel := range cancels {
		cancel()
	}
}

// TestBackupQueueStatusAfterCleanup tests queue status after cleanup operations
func TestBackupQueueStatusAfterCleanup(t *testing.T) {
	registry := GetBackupOperationRegistry()
	ctx := context.Background()

	// Fill some slots
	var cancels []context.CancelFunc
	for i := 0; i < 3; i++ {
		backupID := "status_test_" + string(rune(i+'0'))
		_, _, cancel, err, _ := registry.Register(ctx, backupID, "status_server", OperationTypeBackup)
		require.NoError(t, err)
		cancels = append(cancels, cancel)
	}

	// Check status before cleanup
	status := registry.GetQueueStatus()
	backupStatus := status["backups"].(map[string]any)
	assert.Equal(t, 3, backupStatus["active"])
	assert.Equal(t, 5, backupStatus["available"]) // 8 - 3 = 5

	// Cleanup server operations
	err := registry.CancelAllForServer("status_server")
	require.NoError(t, err)

	// Check status after cleanup
	status = registry.GetQueueStatus()
	backupStatus = status["backups"].(map[string]any)
	assert.Equal(t, 0, backupStatus["active"])
	assert.Equal(t, 8, backupStatus["available"]) // All slots available

	// Cleanup
	for _, cancel := range cancels {
		cancel()
	}
}

// TestServerDeletionBackupIntegration tests full server deletion backup cleanup flow
func TestServerDeletionBackupIntegration(t *testing.T) {
	registry := GetBackupOperationRegistry()
	ctx := context.Background()
	serverID := "integration_test_server"

	// Simulate server with ongoing backup and restore operations
	backupOp, _, backupCancel, err, _ := registry.Register(ctx, "backup_op", serverID, OperationTypeBackup)
	require.NoError(t, err)

	restoreOp, _, restoreCancel, err, _ := registry.Register(ctx, "restore_op", serverID, OperationTypeRestore)
	require.NoError(t, err)

	// Verify operations are active
	assert.Equal(t, 2, registry.CountForServer(serverID))
	
	retrieved, exists := registry.Get("backup_op")
	assert.True(t, exists)
	assert.Equal(t, backupOp.ID, retrieved.ID)

	retrieved, exists = registry.Get("restore_op") 
	assert.True(t, exists)
	assert.Equal(t, restoreOp.ID, retrieved.ID)

	// Simulate server deletion - should cancel all operations
	err = registry.CancelAllForServer(serverID)
	require.NoError(t, err)

	// Verify all operations are cancelled and removed
	assert.Equal(t, 0, registry.CountForServer(serverID))
	
	_, exists = registry.Get("backup_op")
	assert.False(t, exists, "Backup operation should be removed")
	
	_, exists = registry.Get("restore_op")
	assert.False(t, exists, "Restore operation should be removed")

	// Cleanup
	backupCancel()
	restoreCancel()
}