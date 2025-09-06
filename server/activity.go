package server

import (
	"context"
	"time"

	"emperror.dev/errors"

	"github.com/Rene-Roscher/wings/internal/database"
	"github.com/Rene-Roscher/wings/internal/models"
)

const ActivityPowerPrefix = "server:power."

const (
	ActivityConsoleCommand      = models.Event("server:console.command")
	ActivitySftpWrite           = models.Event("server:sftp.write")
	ActivitySftpCreate          = models.Event("server:sftp.create")
	ActivitySftpCreateDirectory = models.Event("server:sftp.create-directory")
	ActivitySftpRename          = models.Event("server:sftp.rename")
	ActivitySftpDelete          = models.Event("server:sftp.delete")
	ActivityFileUploaded        = models.Event("server:file.uploaded")
	ActivityFileDownloaded      = models.Event("server:file.downloaded")
	ActivityFileCompressed      = models.Event("server:file.compressed")
	ActivityFileDecompressed    = models.Event("server:file.decompressed")
	ActivityFileChmod           = models.Event("server:file.chmod")
)

// RequestActivity is a wrapper around a LoggedEvent that is able to track additional request
// specific metadata including the specific user and IP address associated with all subsequent
// events. The internal logged event structure can be extracted by calling RequestEvent.Event().
type RequestActivity struct {
	server string
	user   string
	ip     string
}

// Event returns the underlying logged event from the RequestEvent instance and sets the
// specific event and metadata on it.
func (ra RequestActivity) Event(event models.Event, metadata models.ActivityMeta) *models.Activity {
	a := models.Activity{Server: ra.server, IP: ra.ip, Event: event, Metadata: metadata}

	return a.SetUser(ra.user)
}

// SetUser clones the RequestActivity struct and sets a new user value on the copy
// before returning it.
func (ra RequestActivity) SetUser(u string) RequestActivity {
	c := ra
	c.user = u
	return c
}

func (s *Server) NewRequestActivity(user string, ip string) RequestActivity {
	return RequestActivity{server: s.ID(), user: user, ip: ip}
}

// SaveActivity saves an activity entry to the database in a background routine. If an error is
// encountered it is logged but not returned to the caller.
func (s *Server) SaveActivity(a RequestActivity, event models.Event, metadata models.ActivityMeta) {
	ctx, cancel := context.WithTimeout(s.Context(), time.Second*3)
	go func() {
		defer cancel()
		if tx := database.Instance().WithContext(ctx).Create(a.Event(event, metadata)); tx.Error != nil {
			s.Log().WithField("error", errors.WithStack(tx.Error)).
				WithField("event", event).
				Error("activity: failed to save event")
		}
	}()

	// Publish activity as event over WebSocket (async to avoid blocking)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.Log().WithField("error", r).WithField("event", event).Error("activity: failed to publish WebSocket event")
			}
		}()
		s.Events().Publish(ActivityEvent, map[string]any{
			"event":    string(event),
			"user":     a.user,
			"metadata": metadata,
		})
	}()
}
