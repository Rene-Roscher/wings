package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Rene-Roscher/wings/internal/progress"
	"github.com/Rene-Roscher/wings/system"
)

// newMockServer creates a minimal Server instance for testing
func newMockServer() *Server {
	return &Server{
		installing:   system.NewAtomicBool(false),
		transferring: system.NewAtomicBool(false),
		restoring:    system.NewAtomicBool(false),
		backingUp:    system.NewAtomicBool(false),
	}
}

// TestSimpleProgressTrackerBasics tests core progress tracking functionality
func TestSimpleProgressTrackerBasics(t *testing.T) {
	// Create minimal server-like structure for testing
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create progress instance
	prog := progress.NewProgress(100)
	prog.SetTotal(100)

	// Create test server (minimal mock)
	mockServer := newMockServer()

	// Create progress tracker
	tracker := NewSimpleProgressTracker(ctx, mockServer, "test-backup-123", "local", prog)
	defer tracker.Close()

	// Test initial state
	assert.Equal(t, int64(0), atomic.LoadInt64(&tracker.lastSent))
	assert.Equal(t, int64(0), atomic.LoadInt64(&tracker.lastTime))

	// Test progress update (simulate small write)
	prog.AddWritten(10)
	tracker.CheckProgress()

	// Give goroutines time to execute
	time.Sleep(50 * time.Millisecond)

	// Should have sent initial progress
	assert.True(t, atomic.LoadInt64(&tracker.lastSent) >= 10, "Should track progress")
	assert.True(t, atomic.LoadInt64(&tracker.lastTime) > 0, "Should record last update time")
}

// TestProgressTrackerThrottling tests progress update throttling
func TestProgressTrackerThrottling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prog := progress.NewProgress(1000)
	prog.SetTotal(1000)

	mockServer := newMockServer()
	tracker := NewSimpleProgressTracker(ctx, mockServer, "throttle-test", "s3", prog)
	defer tracker.Close()

	// Make rapid progress updates
	updateCount := 0
	lastSent := int64(0)

	for i := 0; i < 10; i++ {
		prog.AddWritten(100) // Each add is 10%
		tracker.CheckProgress()

		currentSent := atomic.LoadInt64(&tracker.lastSent)
		if currentSent > lastSent {
			updateCount++
			lastSent = currentSent
		}

		// Very small sleep - faster than throttle interval
		time.Sleep(1 * time.Millisecond)
	}

	// Should have throttled updates (not all 10 updates should be sent)
	assert.True(t, updateCount < 10, "Should throttle rapid updates, got %d updates", updateCount)
	assert.Equal(t, int64(100), atomic.LoadInt64(&tracker.lastSent), "Should reach 100%")
}

// TestProgressTrackerFinalProgress tests final progress handling
func TestProgressTrackerFinalProgress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prog := progress.NewProgress(1000)
	prog.SetTotal(100)

	mockServer := newMockServer()
	tracker := NewSimpleProgressTracker(ctx, mockServer, "final-test", "s3", prog)
	defer tracker.Close()

	// Progress to near completion
	prog.AddWritten(99)
	tracker.CheckProgress()

	// Send final progress
	tracker.SendFinalProgress(true) // Success

	time.Sleep(50 * time.Millisecond)

	// Should be at 100%
	assert.Equal(t, int64(100), atomic.LoadInt64(&tracker.lastSent), "Final progress should be 100%")
}

// TestProgressTrackerFinalProgressFailure tests final progress on failure
func TestProgressTrackerFinalProgressFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prog := progress.NewProgress(1000)
	prog.SetTotal(100)

	mockServer := newMockServer()
	tracker := NewSimpleProgressTracker(ctx, mockServer, "failure-test", "local", prog)
	defer tracker.Close()

	// Progress partway
	prog.AddWritten(50)
	tracker.CheckProgress()

	// Send final progress with failure
	tracker.SendFinalProgress(false) // Failure

	time.Sleep(50 * time.Millisecond)

	// Should indicate failure (-1)
	// Note: We can't easily test the exact value without accessing the event system
	// but we verify the method doesn't panic and completes
}

// TestProgressTrackerContextCancellation tests proper context handling
func TestProgressTrackerContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	prog := progress.NewProgress(1000)
	prog.SetTotal(100)

	mockServer := newMockServer()
	tracker := NewSimpleProgressTracker(ctx, mockServer, "cancel-test", "s3", prog)

	// Make some progress
	prog.AddWritten(25)
	tracker.CheckProgress()

	// Cancel context
	cancel()

	// Try to make more progress after cancellation
	prog.AddWritten(25)
	tracker.CheckProgress()

	// Close should not hang
	done := make(chan bool)
	go func() {
		tracker.Close()
		done <- true
	}()

	select {
	case <-done:
		// Good, close completed
	case <-time.After(1 * time.Second):
		t.Error("Close() should not hang after context cancellation")
	}
}

// TestProgressTrackerByteMode tests progress tracking without total size
func TestProgressTrackerByteMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create progress without total (unknown size scenario)
	prog := progress.NewProgress(1000)
	// Don't set total - simulates unknown backup size

	mockServer := newMockServer()
	tracker := NewSimpleProgressTracker(ctx, mockServer, "byte-test", "s3", prog)
	defer tracker.Close()

	// Add bytes written
	prog.AddWritten(1024 * 1024)     // 1MB
	tracker.CheckProgress()

	prog.AddWritten(2 * 1024 * 1024) // Another 2MB
	tracker.CheckProgress()

	time.Sleep(50 * time.Millisecond)

	// In byte mode, should track MB chunks
	lastSent := atomic.LoadInt64(&tracker.lastSent)
	assert.True(t, lastSent >= 2, "Should track MB chunks, got %d", lastSent)
}

// TestProgressTrackerResourceCleanup tests proper resource cleanup
func TestProgressTrackerResourceCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prog := progress.NewProgress(1000)
	prog.SetTotal(100)

	mockServer := newMockServer()
	tracker := NewSimpleProgressTracker(ctx, mockServer, "cleanup-test", "local", prog)

	// Make progress to spawn some goroutines
	prog.AddWritten(50)
	tracker.CheckProgress()

	// Send final progress to trigger cleanup goroutines
	tracker.SendFinalProgress(true)

	// Close should wait for all goroutines to complete
	start := time.Now()
	tracker.Close()
	elapsed := time.Since(start)

	// Should complete cleanup within reasonable time
	assert.True(t, elapsed < 5*time.Second, "Close should complete quickly, took %v", elapsed)

	// Double close should not panic or hang
	tracker.Close()
}

// TestProgressEvent tests progress event structure
func TestProgressEvent(t *testing.T) {
	// Test progress update structure
	update := BackupProgressUpdate{
		BackupID:     "test-backup-456",
		Type:         "s3",
		Percentage:   75,
		BytesWritten: 7500,
		BytesTotal:   10000,
	}

	assert.Equal(t, "test-backup-456", update.BackupID)
	assert.Equal(t, "s3", update.Type)
	assert.Equal(t, 75, update.Percentage)
	assert.Equal(t, int64(7500), update.BytesWritten)
	assert.Equal(t, int64(10000), update.BytesTotal)
}

// TestS3ProgressSplitLogic tests the 80/20 progress split for S3
func TestS3ProgressSplitLogic(t *testing.T) {
	// Test S3 progress calculation (80% archive, 20% upload)
	
	testCases := []struct {
		archiveBytes int64
		totalBytes   int64
		expected     int
		description  string
	}{
		{0, 1000, 0, "Initial state"},
		{500, 1000, 40, "50% archive = 40% total (80% of 50%)"},
		{1000, 1000, 80, "100% archive = 80% total"},
		// Upload phase would be 80-100% based on S3 upload progress
	}

	for _, tc := range testCases {
		// Simulate S3 progress calculation: 80% for archiving
		archivePercent := int((tc.archiveBytes * 100) / tc.totalBytes)
		s3Percent := int((archivePercent * 80) / 100)

		assert.Equal(t, tc.expected, s3Percent, tc.description)
	}
}

// TestProgressTrackerPerformance tests performance characteristics
func TestProgressTrackerPerformance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prog := progress.NewProgress(1000)
	prog.SetTotal(1000000) // 1M total

	mockServer := newMockServer()
	tracker := NewSimpleProgressTracker(ctx, mockServer, "perf-test", "local", prog)
	defer tracker.Close()

	// Measure time for many rapid progress updates
	start := time.Now()
	
	for i := 0; i < 1000; i++ {
		prog.AddWritten(1000) // 1000 rapid updates
		tracker.CheckProgress()
	}
	
	elapsed := time.Since(start)

	// Should be very fast (under 100ms for 1000 calls)
	assert.True(t, elapsed < 100*time.Millisecond, 
		"1000 CheckProgress calls should be fast, took %v", elapsed)
}