# ZSTD Compression Implementation

## Overview

This implementation adds support for ZSTD compression in Pterodactyl Wings backup system while maintaining 100% backward compatibility with existing GZIP backups.

## Changes Made

### 1. Configuration Support
- Added `format` field to `config.yaml` under `system.backups`
- Supported values: `"gzip"` (default), `"zstd"`, `"none"`
- Maintains backward compatibility - defaults to `"gzip"`

### 2. Archive Creation (`server/filesystem/archive.go`)
- Refactored compression logic into pluggable system
- Added `createCompressor()` method that chooses format based on config
- Added `createZstdWriter()` with adaptive threading (2-4 threads max)
- Added `createGzipWriter()` with existing logic preserved
- Proper compression level mapping from config

### 3. Archive Restoration (`server/filesystem/archive_restore.go`)
- Added automatic format detection via magic bytes
- ZSTD magic: `0x28B52FFD`
- GZIP magic: `0x1F8B`
- Graceful fallback to GZIP for unknown formats

### 4. Backup Integration (`server/backup.go`)
- Updated `RestoreBackup()` to auto-detect compression format
- Seamless decompression without API changes
- Full backward compatibility with existing backups

### 5. S3 Backup Fixes (`server/backup/backup_s3.go`)
- Fixed critical bug: S3 backups no longer self-delete on failure
- Fixed context cancellation bug in `generateRemoteRequest()`
- Proper success/failure handling

### 6. Path Generation (`server/backup/backup.go`)
- Updated `Path()` method to generate appropriate file extensions:
  - ZSTD: `.tar.zst`
  - GZIP: `.tar.gz` 
  - None: `.tar`

## Performance Benefits

### ZSTD vs GZIP Comparison:
- **Compression Speed**: 3-5x faster than GZIP
- **Decompression Speed**: 2-3x faster than GZIP  
- **Compression Ratio**: 10-20% better than GZIP
- **Memory Usage**: Lower with `LowerEncoderMem` option
- **Threading**: 2-4 threads (adaptive based on CPU count)

## Configuration

```yaml
system:
  backups:
    # Existing options remain unchanged
    write_limit: 0
    compression_level: "best_speed"
    
    # NEW: Compression format
    format: "zstd"  # Options: "gzip", "zstd", "none"
```

## Backward Compatibility Guarantees

### 1. Existing Backups
- All existing `.tar.gz` backups remain fully restorable
- Auto-detection handles format seamlessly
- No database migrations required
- No panel changes required

### 2. API Compatibility  
- All backup APIs remain identical
- JSON responses unchanged
- WebSocket events unchanged
- File structure unchanged

### 3. Gradual Migration
- Default remains `"gzip"` for safety
- Can be enabled per-installation basis
- Old and new backups can coexist
- Instant rollback by changing config

## Testing

### Unit Tests
- Format detection tests (`archive_test.go`)
- GZIP decompression tests
- ZSTD decompression tests
- All existing filesystem tests still pass

### Integration Tests
- Tested with real backup/restore cycles
- Verified S3 upload compatibility
- Confirmed file extension handling

## Deployment Strategy

### Phase 1: Deploy (Week 1)
```yaml
format: "gzip"  # No change in behavior
```

### Phase 2: Enable ZSTD (Week 2-3)  
```yaml
format: "zstd"  # New backups use ZSTD
```

### Phase 3: Monitor & Scale (Week 4+)
- Monitor performance metrics
- Validate backup integrity  
- Scale to all installations

## Monitoring

### Key Metrics to Track:
- Backup creation time (expect 50-70% reduction)
- Backup file sizes (expect 10-20% reduction)
- Memory usage during backups
- CPU utilization (should be similar with threading)
- S3 upload times (faster due to smaller files)

### Success Criteria:
- Zero backup failures
- Faster backup/restore times
- Smaller storage usage
- 100% restore success rate

## Rollback Plan

If issues arise, instant rollback:

```yaml
format: "gzip"  # Back to original behavior
```

- No code changes needed
- All new backups will use GZIP
- Existing ZSTD backups remain restorable
- Zero downtime rollback

## Technical Notes

### Thread Management
- Maximum 4 threads for ZSTD compression
- Adaptive scaling: 2 threads (1-4 CPUs), 3 threads (5-8 CPUs), 4 threads (9+ CPUs)
- Memory-efficient encoding with `LowerEncoderMem`

### File Extensions
- Extensions now reflect actual compression format
- Backward compatibility maintained for existing files
- Future-proof for additional formats

### Error Handling
- Compression failures fall back to GZIP
- Decompression auto-detects format
- Graceful handling of corrupted files

## Future Enhancements

### Possible Additions:
- Dictionary compression for game servers
- LZ4 support for ultra-fast compression
- Backup format conversion tools
- Compression benchmarking tools

This implementation provides significant performance improvements while maintaining production stability and backward compatibility.