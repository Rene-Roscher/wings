package backup

import (
	"context"
	"io"
	"os"
	"path"

	"emperror.dev/errors"
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
	if err := format.Extract(ctx, reader, func(ctx context.Context, f archives.FileInfo) error {
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
