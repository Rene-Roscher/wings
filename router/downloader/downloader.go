package downloader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/google/uuid"

	"github.com/Rene-Roscher/wings/server"
)

var client *http.Client

func init() {
	dialer := &net.Dialer{
		LocalAddr: nil,
	}

	trnspt := http.DefaultTransport.(*http.Transport).Clone()
	trnspt.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		ipStr, _, err := net.SplitHostPort(c.RemoteAddr().String())
		if err != nil {
			return c, errors.WithStack(err)
		}
		ip := net.ParseIP(ipStr)
		if ip == nil {
			return c, errors.WithStack(ErrInvalidIPAddress)
		}
		if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() {
			return c, errors.WithStack(ErrInternalResolution)
		}
		for _, block := range internalRanges {
			if !block.Contains(ip) {
				continue
			}
			return c, errors.WithStack(ErrInternalResolution)
		}
		return c, nil
	}

	client = &http.Client{
		Timeout: time.Hour * 12,

		Transport: trnspt,

		// Disallow any redirect on an HTTP call. This is a security requirement: do not modify
		// this logic without first ensuring that the new target location IS NOT within the current
		// instance's local network.
		//
		// This specific error response just causes the client to not follow the redirect and
		// returns the actual redirect response to the caller. Not perfect, but simple and most
		// people won't be using URLs that redirect anyways hopefully?
		//
		// We'll re-evaluate this down the road if needed.
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

var instance = &Downloader{
	// Tracks all the active downloads.
	downloadCache: make(map[string]*Download),
	// Tracks all the downloads active for a given server instance. This is
	// primarily used to make things quicker and keep the code a little more
	// legible throughout here.
	serverCache: make(map[string][]string),
}

// Internal IP ranges that should be blocked if the resource requested resolves within.
var internalRanges = []*net.IPNet{
	mustParseCIDR("127.0.0.1/8"),
	mustParseCIDR("10.0.0.0/8"),
	mustParseCIDR("172.16.0.0/12"),
	mustParseCIDR("192.168.0.0/16"),
	mustParseCIDR("169.254.0.0/16"),
	mustParseCIDR("::1/128"),
	mustParseCIDR("fe80::/10"),
	mustParseCIDR("fc00::/7"),
}

const (
	ErrInternalResolution = errors.Sentinel("downloader: destination resolves to internal network location")
	ErrInvalidIPAddress   = errors.Sentinel("downloader: invalid IP address")
	ErrDownloadFailed     = errors.Sentinel("downloader: download request failed")
	ErrInvalidFilename    = errors.Sentinel("downloader: invalid or unsafe filename")
	ErrFileTooLarge       = errors.Sentinel("downloader: file exceeds maximum allowed size")
)

// Maximum download size: 15GB (configurable if needed)
const maxDownloadSize = 15 * 1024 * 1024 * 1024

type Counter struct {
	total   int
	onWrite func(total int)
}

func (c *Counter) Write(p []byte) (int, error) {
	n := len(p)
	c.total += n
	c.onWrite(c.total)
	return n, nil
}

// DownloadProgressUpdate represents the data sent over WebSocket for download progress
type DownloadProgressUpdate struct {
	Identifier   string `json:"identifier"`
	Filename     string `json:"filename"`
	Directory    string `json:"directory"`
	URL          string `json:"url"`
	Percentage   int    `json:"percentage"`
	BytesWritten int64  `json:"bytes_written"`
	BytesTotal   int64  `json:"bytes_total"`
	Status       string `json:"status"` // "downloading", "completed", "failed"
}

type DownloadRequest struct {
	Directory string
	URL       *url.URL
	FileName  string
	UseHeader bool
}

type Download struct {
	Identifier string
	path       string
	mu         sync.RWMutex
	req        DownloadRequest
	server     *server.Server
	progress   float64
	cancelFunc *context.CancelFunc
	// WebSocket progress tracking (only enabled for background downloads)
	sendEvents     bool
	lastEventTime  int64 // Unix nano for throttling
	lastPercentage int   // Last percentage sent
}

// New starts a new tracked download which allows for cancellation later on by calling
// the Downloader.Cancel function.
func New(s *server.Server, r DownloadRequest) *Download {
	dl := Download{
		Identifier: uuid.Must(uuid.NewRandom()).String(),
		req:        r,
		server:     s,
	}
	instance.track(&dl)
	return &dl
}

// EnableEvents enables WebSocket progress events for background downloads
// Should only be called for foreground=false downloads
func (dl *Download) EnableEvents() {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	dl.sendEvents = true
}

// ByServer returns all the tracked downloads for a given server instance.
func ByServer(sid string) []*Download {
	instance.mu.Lock()
	defer instance.mu.Unlock()
	var downloads []*Download
	if v, ok := instance.serverCache[sid]; ok {
		for _, id := range v {
			if dl, ok := instance.downloadCache[id]; ok {
				downloads = append(downloads, dl)
			}
		}
	}
	return downloads
}

// ByID returns a single Download matching a given identifier. If no download is found
// the second argument in the response will be false.
func ByID(dlid string) *Download {
	return instance.find(dlid)
}

//goland:noinspection GoVetCopyLock
func (dl Download) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Identifier string
		Progress   float64
	}{
		Identifier: dl.Identifier,
		Progress:   dl.Progress(),
	})
}

// sanitizeFilename prevents path traversal attacks by cleaning and validating filenames
// SECURITY: This function is CRITICAL for preventing RCE via path traversal
func sanitizeFilename(filename string) (string, error) {
	if filename == "" {
		return "", ErrInvalidFilename
	}

	// Use filepath.Base to remove any directory components (prevents ../ attacks)
	clean := filepath.Base(filename)

	// Additional validation: Base() alone is not enough for edge cases
	// Check for dangerous patterns that might bypass Base()
	if clean == "." || clean == ".." {
		return "", errors.WithStack(ErrInvalidFilename)
	}

	// Reject absolute paths (should already be handled by Base, but defense in depth)
	if filepath.IsAbs(filename) {
		return "", errors.WithStack(ErrInvalidFilename)
	}

	// Reject filenames that still contain path separators after Base()
	// This catches edge cases on different operating systems
	if strings.ContainsAny(clean, "/\\") {
		return "", errors.WithStack(ErrInvalidFilename)
	}

	// Reject hidden files and system files (optional, but good security practice)
	if strings.HasPrefix(clean, ".") {
		return "", errors.WithStack(ErrInvalidFilename)
	}

	// Validate length (prevent extremely long filenames)
	if len(clean) > 255 {
		return "", errors.WithStack(ErrInvalidFilename)
	}

	// Whitelist approach: only allow alphanumeric, dash, underscore, and single dot
	// This prevents special characters that might be exploited
	for i, c := range clean {
		valid := (c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.'

		if !valid {
			return "", errors.Wrap(ErrInvalidFilename, fmt.Sprintf("invalid character at position %d: %c", i, c))
		}
	}

	// Prevent multiple dots in a row (could be used for obfuscation)
	if strings.Contains(clean, "..") {
		return "", errors.WithStack(ErrInvalidFilename)
	}

	return clean, nil
}

// Execute executes a given download for the server and begins writing the file to the disk. Once
// completed the download will be removed from the cache.
func (dl *Download) Execute() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour*12)
	dl.cancelFunc = &cancel
	defer dl.Cancel()

	// SECURITY: Follow redirects manually to ensure each redirect target goes through SSRF validation
	// Our DialContext checks prevent redirects to internal networks (127.0.0.1, 10.0.0.0/8, etc)
	const maxRedirects = 10
	currentURL := dl.req.URL.String()

	var res *http.Response
	var err error

	for redirectCount := 0; redirectCount <= maxRedirects; redirectCount++ {
		// Create request for current URL (goes through SSRF-protected DialContext)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, currentURL, nil)
		if err != nil {
			return errors.WrapIf(err, "downloader: failed to create request")
		}
		req.Header.Set("User-Agent", "Pterodactyl Panel (https://pterodactyl.io)")

		// Execute request
		res, err = client.Do(req)
		if err != nil {
			return ErrDownloadFailed
		}

		// Check if this is a redirect (3xx status codes)
		if res.StatusCode >= 300 && res.StatusCode < 400 {
			location := res.Header.Get("Location")
			if location == "" {
				res.Body.Close()
				return errors.New("downloader: redirect response missing Location header")
			}

			// Parse redirect URL (may be relative)
			redirectURL, err := url.Parse(location)
			if err != nil {
				res.Body.Close()
				return errors.Wrap(err, "downloader: invalid redirect Location")
			}

			// Resolve against current URL (handles relative redirects like /path)
			redirectURL = req.URL.ResolveReference(redirectURL)

			// Log redirect for debugging
			dl.server.Log().WithFields(log.Fields{
				"from":   currentURL,
				"to":     redirectURL.String(),
				"status": res.StatusCode,
				"count":  redirectCount + 1,
			}).Debug("following redirect")

			// Close body (no content in redirect responses)
			res.Body.Close()

			// Check redirect limit
			if redirectCount >= maxRedirects {
				return errors.New(fmt.Sprintf("downloader: too many redirects (max %d)", maxRedirects))
			}

			// Update URL for next iteration
			currentURL = redirectURL.String()
			continue
		}

		// Not a redirect - check for success
		if res.StatusCode != http.StatusOK {
			res.Body.Close()
			return errors.New("downloader: got bad response status from endpoint: " + res.Status)
		}

		// Success! Break out of redirect loop
		break
	}

	// Ensure we have a response body to work with
	if res == nil {
		return errors.New("downloader: no response received")
	}
	defer res.Body.Close()

	// Check ContentLength:
	// - ContentLength > 0: known size
	// - ContentLength == -1: unknown size (chunked encoding) - ALLOWED
	// - ContentLength == 0: empty file - REJECTED
	hasKnownSize := res.ContentLength > 0
	if res.ContentLength == 0 {
		return errors.New("downloader: remote file is empty (ContentLength is 0)")
	}

	// SECURITY: Check maximum file size to prevent DoS via disk exhaustion
	// Only possible if we have a known size
	if hasKnownSize && res.ContentLength > maxDownloadSize {
		return errors.Wrap(ErrFileTooLarge, fmt.Sprintf("file size %d bytes exceeds maximum %d bytes", res.ContentLength, maxDownloadSize))
	}

	// Log download mode for debugging
	if hasKnownSize {
		dl.server.Log().WithField("content_length", res.ContentLength).Debug("downloading file with known size")
	} else {
		dl.server.Log().Warn("downloading file with unknown size (chunked encoding) - progress tracking will show bytes only")
	}

	// SECURITY: Extract filename from various sources and sanitize ALL of them
	var unsafeFilename string

	if dl.req.UseHeader {
		if contentDisposition := res.Header.Get("Content-Disposition"); contentDisposition != "" {
			_, params, err := mime.ParseMediaType(contentDisposition)
			if err != nil {
				return errors.WrapIf(err, "downloader: invalid \"Content-Disposition\" header")
			}

			if v, ok := params["filename"]; ok {
				// SECURITY FIX: Sanitize Content-Disposition filename (Attack Vector #1)
				unsafeFilename = v
			}
		}
	}
	if unsafeFilename == "" {
		if dl.req.FileName != "" {
			// SECURITY FIX: Sanitize user-provided filename (Attack Vector #2)
			unsafeFilename = dl.req.FileName
		} else {
			// SECURITY FIX: Sanitize URL path filename (Attack Vector #3)
			parts := strings.Split(dl.req.URL.Path, "/")
			unsafeFilename = parts[len(parts)-1]
		}
	}

	// CRITICAL SECURITY: Sanitize filename to prevent path traversal
	safeFilename, err := sanitizeFilename(unsafeFilename)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("downloader: unsafe filename rejected: %s", unsafeFilename))
	}

	dl.path = safeFilename
	dl.server.Log().WithFields(log.Fields{
		"unsafe_filename": unsafeFilename,
		"safe_filename":   safeFilename,
	}).Debug("sanitized download filename")

	// Send initial WebSocket event (if enabled for background downloads)
	if dl.sendEvents {
		dl.mu.Lock()
		dl.sendProgressEvent(0, 0, res.ContentLength, "downloading")
		dl.mu.Unlock()
	}

	// CRITICAL: Defer final event sending (executes at function exit)
	// Capture error at execution time, not declaration time!
	defer func() {
		if dl.sendEvents {
			dl.mu.Lock()
			defer dl.mu.Unlock()

			// Determine status based on err (captured at defer execution)
			var finalStatus string
			var finalPercentage int
			if err == nil {
				finalStatus = "completed"
				finalPercentage = 100
			} else {
				finalStatus = "failed"
				finalPercentage = -1 // Error indicator
			}

			// Get final bytes written (from progress)
			var bytesWritten int64
			if res.ContentLength > 0 {
				bytesWritten = int64(dl.progress * float64(res.ContentLength))
			} else {
				bytesWritten = int64(dl.progress) // Raw bytes for chunked
			}

			dl.sendProgressEvent(finalPercentage, bytesWritten, res.ContentLength, finalStatus)
		}
	}()

	p := dl.Path()
	dl.server.Log().WithField("path", p).Debug("writing remote file to disk")

	// Write the file while tracking the progress, Write will check that the
	// size of the file won't exceed the disk limit.
	r := io.TeeReader(res.Body, dl.counter(res.ContentLength))
	if err := dl.server.Filesystem().Write(p, r, res.ContentLength, 0o644); err != nil {
		return errors.WrapIf(err, "downloader: failed to write file to server directory")
	}
	return nil
}

// Cancel cancels a running download and frees up the associated resources. If a file is being
// written a partial file will remain present on the disk.
func (dl *Download) Cancel() {
	if dl.cancelFunc != nil {
		(*dl.cancelFunc)()
	}
	instance.remove(dl.Identifier)
}

// BelongsTo checks if the given download belongs to the provided server.
func (dl *Download) BelongsTo(s *server.Server) bool {
	return dl.server.ID() == s.ID()
}

// Progress returns the current progress of the download as a float value between 0 and 1 where
// 1 indicates that the download is completed.
func (dl *Download) Progress() float64 {
	dl.mu.RLock()
	defer dl.mu.RUnlock()
	return dl.progress
}

func (dl *Download) Path() string {
	return filepath.Join(dl.req.Directory, dl.path)
}

// Handles a write event by updating the progress completed percentage and firing off
// events to the server websocket as needed.
// For chunked encoding (contentLength == -1), progress will be reported as bytes downloaded.
func (dl *Download) counter(contentLength int64) *Counter {
	onWrite := func(t int) {
		dl.mu.Lock()
		defer dl.mu.Unlock()

		var percentage int
		if contentLength > 0 {
			// Known size: calculate percentage
			dl.progress = float64(t) / float64(contentLength)
			percentage = int(dl.progress * 100)
			if percentage > 100 {
				percentage = 100
			}
		} else {
			// Unknown size (chunked encoding): report bytes as "progress"
			dl.progress = float64(t)
			percentage = 0 // Can't calculate percentage without total
		}

		// Send WebSocket progress events (if enabled, with throttling)
		if dl.sendEvents {
			dl.sendProgressEvent(percentage, int64(t), contentLength, "downloading")
		}
	}
	return &Counter{
		onWrite: onWrite,
	}
}

// sendProgressEvent sends a WebSocket event with throttling (250ms between updates)
// MUST be called with dl.mu held (Lock or RLock)
func (dl *Download) sendProgressEvent(percentage int, bytesWritten, bytesTotal int64, status string) {
	now := time.Now().UnixNano()

	// Throttle: Only send events every 250ms (like backup progress)
	// ALWAYS send: 0%, 100%, or status change
	const throttleNanos = 250_000_000 // 250ms
	shouldSend := (now - dl.lastEventTime) >= throttleNanos ||
		percentage != dl.lastPercentage ||
		status != "downloading"

	if !shouldSend {
		return
	}

	dl.lastEventTime = now
	dl.lastPercentage = percentage

	event := DownloadProgressUpdate{
		Identifier:   dl.Identifier,
		Filename:     dl.path,
		Directory:    dl.req.Directory,
		URL:          dl.req.URL.String(),
		Percentage:   percentage,
		BytesWritten: bytesWritten,
		BytesTotal:   bytesTotal,
		Status:       status,
	}

	// Import server package for DownloadProgressEvent constant
	dl.server.Events().Publish("download progress", event)
}

// Downloader represents a global downloader that keeps track of all currently processing downloads
// for the machine.
type Downloader struct {
	mu            sync.RWMutex
	downloadCache map[string]*Download
	serverCache   map[string][]string
}

// track tracks a download in the internal cache for this instance.
func (d *Downloader) track(dl *Download) {
	d.mu.Lock()
	defer d.mu.Unlock()
	sid := dl.server.ID()
	if _, ok := d.downloadCache[dl.Identifier]; !ok {
		d.downloadCache[dl.Identifier] = dl
		if _, ok := d.serverCache[sid]; !ok {
			d.serverCache[sid] = []string{}
		}
		d.serverCache[sid] = append(d.serverCache[sid], dl.Identifier)
	}
}

// find finds a given download entry using the provided ID and returns it.
func (d *Downloader) find(dlid string) *Download {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if entry, ok := d.downloadCache[dlid]; ok {
		return entry
	}
	return nil
}

// remove removes the given download reference from the cache storing them. This also updates
// the slice of active downloads for a given server to not include this download.
func (d *Downloader) remove(dlID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.downloadCache[dlID]; !ok {
		return
	}
	sID := d.downloadCache[dlID].server.ID()
	delete(d.downloadCache, dlID)
	if tracked, ok := d.serverCache[sID]; ok {
		var out []string
		for _, k := range tracked {
			if k != dlID {
				out = append(out, k)
			}
		}
		d.serverCache[sID] = out
	}
}

func mustParseCIDR(ip string) *net.IPNet {
	_, block, err := net.ParseCIDR(ip)
	if err != nil {
		panic(fmt.Errorf("downloader: failed to parse CIDR: %s", err))
	}
	return block
}
