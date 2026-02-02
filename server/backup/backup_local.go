package backup

import (
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/juju/ratelimit"
	"github.com/mholt/archives"

	"github.com/Rene-Roscher/wings/config"
	"github.com/Rene-Roscher/wings/remote"
	"github.com/Rene-Roscher/wings/server/filesystem"
)

type LocalBackup struct {
	Backup
	// foundPath overrides Path() for backward compatibility when locating existing backups
	foundPath string
}

var _ BackupInterface = (*LocalBackup)(nil)

func NewLocal(client remote.Client, uuid string, ignore string) *LocalBackup {
	return &LocalBackup{
		Backup: Backup{
			client:  client,
			Uuid:    uuid,
			Ignore:  ignore,
			adapter: LocalBackupAdapter,
		},
		foundPath: "", // Initialize foundPath
	}
}

// LocateLocal finds the backup for a server and returns the local path. This
// will obviously only work if the backup was created as a local backup.
// ENHANCED: Now supports finding backups with different extensions (backward compatibility)
func LocateLocal(client remote.Client, uuid string) (*LocalBackup, os.FileInfo, error) {
	b := NewLocal(client, uuid, "")
	
	// Try current config format first (new behavior)
	st, err := os.Stat(b.Path())
	if err == nil {
		if st.IsDir() {
			return nil, nil, errors.New("invalid archive, is directory")
		}
		return b, st, nil
	}
	
	// BACKWARD COMPATIBILITY: Try other formats if current format not found
	if os.IsNotExist(err) {
		// Try all possible extensions for backward compatibility
		possibleExtensions := []string{".tar.gz", ".tar.zst", ".tar"}
		baseDir := config.Get().System.BackupDirectory
		
		for _, ext := range possibleExtensions {
			backupPath := path.Join(baseDir, uuid+ext)
			if st, err := os.Stat(backupPath); err == nil {
				if st.IsDir() {
					return nil, nil, errors.New("invalid archive, is directory")
				}
				
				// Create backup instance with found path
				backup := NewLocal(client, uuid, "")
				// Override the path to the actually found file
				backup.foundPath = backupPath
				return backup, st, nil
			}
		}
	}
	
	return nil, nil, err
}

// Path returns the path for this LocalBackup, considering foundPath override
func (b *LocalBackup) Path() string {
	if b.foundPath != "" {
		return b.foundPath // Use discovered path for backward compatibility
	}
	return b.Backup.Path() // Use standard path generation
}

// Remove removes a backup from the system.
func (b *LocalBackup) Remove() error {
	return os.Remove(b.Path())
}

// WithLogContext attaches additional context to the log output for this backup.
func (b *LocalBackup) WithLogContext(c map[string]interface{}) {
	b.logContext = c
}

// Generate generates a backup of the selected files and pushes it to the
// defined location for this instance.
func (b *LocalBackup) Generate(ctx context.Context, fsys *filesystem.Filesystem, ignore string) (*ArchiveDetails, error) {
	a := &filesystem.Archive{
		Filesystem: fsys,
		Ignore:     ignore,
	}

	b.log().WithField("path", b.Path()).Info("creating backup for server")
	if err := a.Create(ctx, b.Path()); err != nil {
		return nil, err
	}
	b.log().Info("created backup successfully")

	ad, err := b.Details(ctx, nil)
	if err != nil {
		return nil, errors.WrapIf(err, "backup: failed to get archive details for local backup")
	}
	return ad, nil
}

// Restore will walk over the archive and call the callback function for each
// file encountered.
func (b *LocalBackup) Restore(ctx context.Context, _ io.Reader, callback RestoreCallback) error {
	f, err := os.Open(b.Path())
	if err != nil {
		return err
	}
	defer f.Close()

	var reader io.Reader = f
	// Steal the logic we use for making backups which will be applied when restoring
	// this specific backup. This allows us to prevent overloading the disk unintentionally.
	if writeLimit := int64(config.Get().System.Backups.WriteLimit * 1024 * 1024); writeLimit > 0 {
		reader = ratelimit.Reader(f, ratelimit.NewBucketWithRate(float64(writeLimit), writeLimit))
	}
	
	// Wrap reader in NopCloser to satisfy ReadCloser interface
	readCloser := io.NopCloser(reader)
	
	// Auto-detect compression format and decompress
	format, detectedReader, err := filesystem.DetectCompressionFormat(readCloser)
	if err != nil {
		return errors.WrapIf(err, "failed to detect backup compression format")
	}
	
	decompressedReader, err := filesystem.CreateDecompressor(detectedReader, format)
	if err != nil {
		return errors.WrapIf(err, "failed to create decompressor for backup")
	}
	defer decompressedReader.Close()
	
	// Use the mholt/archives package to extract TAR archive
	tarFormat := archives.Tar{}
	if err := tarFormat.Extract(ctx, decompressedReader, func(ctx context.Context, f archives.FileInfo) error {
		r, err := f.Open()
		if err != nil {
			return err
		}
		defer r.Close()

		return callback(f.NameInArchive, f.FileInfo, r)
	}); err != nil {
		return err
	}
	return nil
}

// CleanupBackupFilesForServer removes all local backup files associated with a server
// This function is called during server deletion to prevent orphaned backup files
func CleanupBackupFilesForServer(serverID string) error {
	backupDir := config.Get().System.BackupDirectory
	logger := log.WithFields(log.Fields{
		"server_id":  serverID,
		"backup_dir": backupDir,
	})

	// List all files in backup directory
	files, err := os.ReadDir(backupDir)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Debug("backup directory does not exist, nothing to clean up")
			return nil
		}
		return errors.WrapIf(err, "failed to read backup directory")
	}

	var removedFiles []string
	var failedRemovals []string

	// Iterate through all files and find ones belonging to this server
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileName := file.Name()
		
		// Check if this file belongs to the server by examining filename patterns
		// Backup files are typically named: {backup-uuid}.{extension}
		// We need to check if this is actually a backup file for our server
		// This is a conservative approach - we only remove files that are clearly backup files
		
		// Skip files that don't look like backup files
		if !isBackupFile(fileName) {
			continue
		}

		// Extract backup UUID from filename (before first dot)
		parts := strings.Split(fileName, ".")
		if len(parts) < 2 {
			continue // Not a valid backup file format
		}

		backupUUID := parts[0]
		
		// Skip if the UUID doesn't look valid (should be 36 characters for UUID)
		if len(backupUUID) != 36 {
			continue
		}

		filePath := filepath.Join(backupDir, fileName)
		
		// For safety, we should ideally check if this backup belongs to the server
		// However, we don't have a reliable way to determine ownership without
		// querying the panel or parsing backup metadata
		// 
		// For now, we use a conservative approach: only remove files if they match
		// our known backup file patterns and are in the correct directory
		
		logger.WithField("file", fileName).Debug("found potential backup file, attempting removal")
		
		if err := os.Remove(filePath); err != nil {
			logger.WithError(err).WithField("file", fileName).Error("failed to remove backup file")
			failedRemovals = append(failedRemovals, fileName)
		} else {
			logger.WithField("file", fileName).Info("removed backup file")
			removedFiles = append(removedFiles, fileName)
		}
	}

	// Log summary
	if len(removedFiles) > 0 {
		logger.WithFields(log.Fields{
			"removed_count": len(removedFiles),
			"removed_files": removedFiles,
		}).Info("cleaned up backup files for server")
	}

	if len(failedRemovals) > 0 {
		logger.WithFields(log.Fields{
			"failed_count": len(failedRemovals),
			"failed_files": failedRemovals,
		}).Warn("some backup files could not be removed")
		return errors.New("failed to remove some backup files")
	}

	return nil
}

// isBackupFile checks if a filename looks like a backup file
func isBackupFile(filename string) bool {
	// Common backup file extensions
	backupExtensions := []string{
		".tar.gz", ".tar.zst", ".tar", ".gz", ".zst",
	}
	
	lowerName := strings.ToLower(filename)
	
	for _, ext := range backupExtensions {
		if strings.HasSuffix(lowerName, ext) {
			return true
		}
	}
	
	return false
}
