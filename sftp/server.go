package sftp

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ed25519"
	"golang.org/x/crypto/ssh"

	"github.com/Rene-Roscher/wings/config"
	"github.com/Rene-Roscher/wings/remote"
	"github.com/Rene-Roscher/wings/server"
)

// Usernames all follow the same format, so don't even bother hitting the API if the username is not
// at least in the expected format. This is very basic protection against random bots finding the SFTP
// server and sending a flood of usernames.
var validUsernameRegexp = regexp.MustCompile(`^(?i)(.+)\.([a-z0-9]{8})$`)

// SmartSecurityProtector - Intelligent, configurable brute force protection
type SmartSecurityProtector struct {
	mu           sync.RWMutex
	attempts     map[string][]time.Time   // IP -> attempt timestamps
	blockedUntil map[string]time.Time     // IP -> block expiry
	reputation   map[string]int           // IP -> reputation score (-100 to +100)
	blockHistory map[string][]time.Time   // IP -> previous block times for escalation
	config       *config.SftpSecurityConfiguration
}

// Global smart protector
var smartProtector *SmartSecurityProtector

// Initialize smart protector with configuration
func initSmartProtector() {
	cfg := config.Get().System.Sftp.Security
	smartProtector = &SmartSecurityProtector{
		attempts:     make(map[string][]time.Time),
		blockedUntil: make(map[string]time.Time),
		reputation:   make(map[string]int),
		blockHistory: make(map[string][]time.Time),
		config:       &cfg,
	}
	log.WithFields(log.Fields{
		"attempts_per_minute": cfg.Thresholds.AttemptsPerMinute,
		"base_block_minutes": cfg.Blocking.BaseBlockMinutes,
		"escalation_factor":  cfg.Blocking.EscalationFactor,
	}).Info("Smart SFTP security protection initialized")
}

// isBlocked checks if an IP is currently blocked with smart logic
func (sp *SmartSecurityProtector) isBlocked(ip string) bool {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	
	if !sp.config.Enabled {
		return false // Protection disabled
	}
	
	// Check active block
	if until, exists := sp.blockedUntil[ip]; exists {
		if time.Now().Before(until) {
			return true // Still blocked
		}
		// Block expired - apply decay to reputation
		if sp.config.Reputation.Enabled {
			if currentScore, hasScore := sp.reputation[ip]; hasScore {
				newScore := int(float64(currentScore) * sp.config.Blocking.DecayFactor)
				sp.reputation[ip] = newScore
				log.WithField("ip", ip).WithField("old_score", currentScore).WithField("new_score", newScore).Debug("Applied reputation decay after block expiry")
			}
		}
	}
	
	// Check reputation-based blocking
	if sp.config.Reputation.Enabled {
		if score, exists := sp.reputation[ip]; exists && score <= sp.config.Reputation.BlockThreshold {
			log.WithField("ip", ip).WithField("reputation_score", score).Info("SMART-SECURITY: IP blocked due to poor reputation")
			return true
		}
	}
	
	return false
}

// recordFailedAttempt records a failed attempt with intelligent blocking logic
func (sp *SmartSecurityProtector) recordFailedAttempt(ip string) bool {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	
	if !sp.config.Enabled {
		return false
	}
	
	now := time.Now()
	
	// Initialize tracking for IP
	if sp.attempts[ip] == nil {
		sp.attempts[ip] = make([]time.Time, 0, 50)
	}
	if sp.blockHistory[ip] == nil {
		sp.blockHistory[ip] = make([]time.Time, 0, 10)
	}
	
	// Clean old attempts (keep relevant timeframes)
	var recentAttempts []time.Time
	for _, attempt := range sp.attempts[ip] {
		if now.Sub(attempt) < 24*time.Hour { // Keep 24h history
			recentAttempts = append(recentAttempts, attempt)
		}
	}
	
	// Add current failed attempt
	recentAttempts = append(recentAttempts, now)
	sp.attempts[ip] = recentAttempts
	
	// Update reputation
	if sp.config.Reputation.Enabled {
		sp.reputation[ip] += sp.config.Reputation.BadBehaviorPenalty
		if sp.reputation[ip] < -100 {
			sp.reputation[ip] = -100 // Cap at minimum
		}
	}
	
	// Count attempts in different timeframes
	minuteCount := sp.countAttemptsInWindow(recentAttempts, time.Minute)
	hourCount := sp.countAttemptsInWindow(recentAttempts, time.Hour)
	dayCount := len(recentAttempts)
	
	// SMART BLOCKING LOGIC
	return sp.evaluateBlocking(ip, minuteCount, hourCount, dayCount, now)
}

// countAttemptsInWindow counts attempts within a time window
func (sp *SmartSecurityProtector) countAttemptsInWindow(attempts []time.Time, window time.Duration) int {
	now := time.Now()
	count := 0
	for _, attempt := range attempts {
		if now.Sub(attempt) <= window {
			count++
		}
	}
	return count
}

// evaluateBlocking implements smart blocking with escalation
func (sp *SmartSecurityProtector) evaluateBlocking(ip string, minuteCount, hourCount, dayCount int, now time.Time) bool {
	// Smart threshold evaluation
	if minuteCount >= sp.config.Thresholds.AttemptsPerMinute {
		// Calculate smart block duration based on history
		blockDuration := sp.calculateSmartBlockDuration(ip, "minute", minuteCount)
		sp.blockedUntil[ip] = now.Add(blockDuration)
		sp.blockHistory[ip] = append(sp.blockHistory[ip], now)
		
		log.WithFields(log.Fields{
			"ip":               ip,
			"minute_attempts":   minuteCount,
			"block_duration":    blockDuration.String(),
			"reputation_score": sp.reputation[ip],
			"total_blocks":     len(sp.blockHistory[ip]),
		}).Warn("SMART-SECURITY: IP blocked - smart escalation applied")
		return true
	}
	
	if hourCount >= sp.config.Thresholds.AttemptsPerHour {
		blockDuration := sp.calculateSmartBlockDuration(ip, "hour", hourCount)
		sp.blockedUntil[ip] = now.Add(blockDuration)
		sp.blockHistory[ip] = append(sp.blockHistory[ip], now)
		
		log.WithFields(log.Fields{
			"ip":               ip,
			"hour_attempts":     hourCount,
			"block_duration":    blockDuration.String(),
			"reputation_score": sp.reputation[ip],
		}).Error("SMART-SECURITY: IP blocked for sustained attack pattern")
		return true
	}
	
	if dayCount >= sp.config.Thresholds.AttemptsPerDay {
		blockDuration := sp.calculateSmartBlockDuration(ip, "day", dayCount)
		sp.blockedUntil[ip] = now.Add(blockDuration)
		sp.blockHistory[ip] = append(sp.blockHistory[ip], now)
		
		log.WithFields(log.Fields{
			"ip":               ip,
			"day_attempts":      dayCount,
			"block_duration":    blockDuration.String(),
			"reputation_score": sp.reputation[ip],
		}).Error("SMART-SECURITY: IP blocked for persistent attack behavior")
		return true
	}
	
	return false
}

// calculateSmartBlockDuration calculates intelligent block duration with escalation
func (sp *SmartSecurityProtector) calculateSmartBlockDuration(ip, trigger string, attemptCount int) time.Duration {
	baseDuration := time.Duration(sp.config.Blocking.BaseBlockMinutes) * time.Minute
	
	// Factor in previous blocks (escalation)
	previousBlocks := len(sp.blockHistory[ip])
	escalationMultiplier := 1.0
	for i := 0; i < previousBlocks; i++ {
		escalationMultiplier *= sp.config.Blocking.EscalationFactor
	}
	
	// Factor in severity of current violation
	severityMultiplier := 1.0
	switch trigger {
	case "minute":
		excessAttempts := attemptCount - sp.config.Thresholds.AttemptsPerMinute
		severityMultiplier = 1.0 + (float64(excessAttempts) * 0.5) // +50% per excess attempt
	case "hour":
		excessAttempts := attemptCount - sp.config.Thresholds.AttemptsPerHour
		severityMultiplier = 2.0 + (float64(excessAttempts) * 0.3) // Base 2x + 30% per excess
	case "day":
		excessAttempts := attemptCount - sp.config.Thresholds.AttemptsPerDay
		severityMultiplier = 4.0 + (float64(excessAttempts) * 0.2) // Base 4x + 20% per excess
	}
	
	// Calculate final duration
	finalDuration := time.Duration(float64(baseDuration) * escalationMultiplier * severityMultiplier)
	
	// Cap at maximum
	maxDuration := time.Duration(sp.config.Blocking.MaxBlockHours) * time.Hour
	if finalDuration > maxDuration {
		finalDuration = maxDuration
	}
	
	return finalDuration
}

// recordSuccessfulAuth records successful authentication for reputation bonus
func (sp *SmartSecurityProtector) recordSuccessfulAuth(ip string) {
	if !sp.config.Enabled || !sp.config.Reputation.Enabled {
		return
	}
	
	sp.mu.Lock()
	defer sp.mu.Unlock()
	
	// Improve reputation for successful auth
	sp.reputation[ip] += sp.config.Reputation.GoodBehaviorBonus
	if sp.reputation[ip] > 100 {
		sp.reputation[ip] = 100 // Cap at maximum
	}
	
	log.WithField("ip", ip).WithField("new_reputation", sp.reputation[ip]).Debug("Reputation improved for successful authentication")
}

// smartCleanup removes old entries with intelligent retention
func (sp *SmartSecurityProtector) smartCleanup() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	
	now := time.Now()
	memoryWindow := time.Duration(sp.config.Reputation.MemoryDays) * 24 * time.Hour
	
	// Clean old attempts (keep reputation memory window)
	for ip, attempts := range sp.attempts {
		var keep []time.Time
		for _, attempt := range attempts {
			if now.Sub(attempt) < memoryWindow {
				keep = append(keep, attempt)
			}
		}
		if len(keep) == 0 {
			delete(sp.attempts, ip)
			// Also clean reputation if no recent activity
			if _, hasReputation := sp.reputation[ip]; hasReputation {
				log.WithField("ip", ip).Debug("Cleared reputation for inactive IP")
				delete(sp.reputation, ip)
			}
		} else {
			sp.attempts[ip] = keep
		}
	}
	
	// Clean old block history
	for ip, blocks := range sp.blockHistory {
		var keep []time.Time
		for _, block := range blocks {
			if now.Sub(block) < memoryWindow {
				keep = append(keep, block)
			}
		}
		if len(keep) == 0 {
			delete(sp.blockHistory, ip)
		} else {
			sp.blockHistory[ip] = keep
		}
	}
	
	// Clean expired blocks and apply reputation decay
	for ip, until := range sp.blockedUntil {
		if now.After(until) {
			log.WithField("ip", ip).WithField("reputation", sp.reputation[ip]).Info("SMART-SECURITY: IP unblocked - reputation decay applied")
			delete(sp.blockedUntil, ip)
		}
	}
	
	// Log cleanup stats
	totalTracked := len(sp.attempts)
	totalBlocked := len(sp.blockedUntil)
	if totalTracked > 0 || totalBlocked > 0 {
		log.WithFields(log.Fields{
			"tracked_ips":    totalTracked,
			"blocked_ips":    totalBlocked,
			"memory_window":  memoryWindow.String(),
		}).Debug("Smart security cleanup completed")
	}
}

// Initialize smart protection with cleanup routine
func init() {
	go func() {
		// Wait for config to be loaded
		time.Sleep(1 * time.Second)
		initSmartProtector()
		
		// Start cleanup routine
		ticker := time.NewTicker(15 * time.Minute) // More frequent cleanup
		defer ticker.Stop()
		for range ticker.C {
			if smartProtector != nil {
				smartProtector.smartCleanup()
			}
		}
	}()
}

//goland:noinspection GoNameStartsWithPackageName
type SFTPServer struct {
	manager  *server.Manager
	BasePath string
	ReadOnly bool
	Listen   string
}

func New(m *server.Manager) *SFTPServer {
	cfg := config.Get().System
	return &SFTPServer{
		manager:  m,
		BasePath: cfg.Data,
		ReadOnly: cfg.Sftp.ReadOnly,
		Listen:   cfg.Sftp.Address + ":" + strconv.Itoa(cfg.Sftp.Port),
	}
}

// Run starts the SFTP server and add a persistent listener to handle inbound
// SFTP connections. This will automatically generate an ED25519 key if one does
// not already exist on the system for host key verification purposes.
func (c *SFTPServer) Run() error {
	if _, err := os.Stat(c.PrivateKeyPath()); os.IsNotExist(err) {
		if err := c.generateED25519PrivateKey(); err != nil {
			return err
		}
	} else if err != nil {
		return errors.Wrap(err, "sftp: could not stat private key file")
	}
	pb, err := os.ReadFile(c.PrivateKeyPath())
	if err != nil {
		return errors.Wrap(err, "sftp: could not read private key file")
	}
	private, err := ssh.ParsePrivateKey(pb)
	if err != nil {
		return err
	}

	conf := &ssh.ServerConfig{
		Config: ssh.Config{
			KeyExchanges: []string{
				"curve25519-sha256", "curve25519-sha256@libssh.org",
				"ecdh-sha2-nistp256", "ecdh-sha2-nistp384", "ecdh-sha2-nistp521",
				"diffie-hellman-group14-sha256",
			},
			Ciphers: []string{
				"aes128-gcm@openssh.com",
				"chacha20-poly1305@openssh.com",
				"aes128-ctr", "aes192-ctr", "aes256-ctr",
			},
			MACs: []string{
				"hmac-sha2-256-etm@openssh.com", "hmac-sha2-256",
			},
		},
		NoClientAuth: false,
		MaxAuthTries: 6,
		PasswordCallback: func(conn ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
			return c.makeCredentialsRequest(conn, remote.SftpAuthPassword, string(password))
		},
		PublicKeyCallback: func(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			return c.makeCredentialsRequest(conn, remote.SftpAuthPublicKey, string(ssh.MarshalAuthorizedKey(key)))
		},
	}
	conf.AddHostKey(private)

	listener, err := net.Listen("tcp", c.Listen)
	if err != nil {
		return err
	}

	public := string(ssh.MarshalAuthorizedKey(private.PublicKey()))
	log.WithField("listen", c.Listen).WithField("public_key", strings.Trim(public, "\n")).Info("sftp server listening for connections")

	for {
		if conn, _ := listener.Accept(); conn != nil {
			go func(conn net.Conn) {
				defer conn.Close()
				
				// CRITICAL: Extract client IP for brute force protection
				clientAddr := conn.RemoteAddr().String()
				clientIP := clientAddr
				if host, _, err := net.SplitHostPort(clientAddr); err == nil {
					clientIP = host
				}
				
				// SMART-SECURITY: Check if IP is blocked before processing
				if smartProtector != nil && smartProtector.isBlocked(clientIP) {
					log.WithField("ip", clientIP).Warn("SMART-SECURITY: Rejecting connection from blocked IP")
					return // Drop connection immediately
				}
				
				if err := c.AcceptInbound(conn, conf); err != nil {
					// SMART-SECURITY: Handle authentication results
					if smartProtector != nil {
						if _, isInvalidCreds := err.(*remote.SftpInvalidCredentialsError); isInvalidCreds {
							isBlocked := smartProtector.recordFailedAttempt(clientIP)
							if isBlocked {
								log.WithField("ip", clientIP).Error("SMART-SECURITY: IP blocked using intelligent escalation")
							}
						} else {
							// Record successful auth for reputation bonus
							smartProtector.recordSuccessfulAuth(clientIP)
						}
					}
					
					log.WithField("error", err).WithField("ip", clientAddr).Error("sftp: failed to accept inbound connection")
				}
			}(conn)
		}
	}
}

// AcceptInbound handles an inbound connection to the instance and determines if we should
// serve the request or not.
func (c *SFTPServer) AcceptInbound(conn net.Conn, config *ssh.ServerConfig) error {
	// Before beginning a handshake must be performed on the incoming net.Conn
	sconn, chans, reqs, err := ssh.NewServerConn(conn, config)
	if err != nil {
		return errors.WithStack(err)
	}
	defer sconn.Close()
	go ssh.DiscardRequests(reqs)

	for ch := range chans {
		// If its not a session channel we just move on because its not something we
		// know how to handle at this point.
		if ch.ChannelType() != "session" {
			ch.Reject(ssh.UnknownChannelType, "unknown channel type")
			continue
		}

		channel, requests, err := ch.Accept()
		if err != nil {
			continue
		}

		go func(in <-chan *ssh.Request) {
			for req := range in {
				// Channels have a type that is dependent on the protocol. For SFTP
				// this is "subsystem" with a payload that (should) be "sftp". Discard
				// anything else we receive ("pty", "shell", etc)
				req.Reply(req.Type == "subsystem" && string(req.Payload[4:]) == "sftp", nil)
			}
		}(requests)

		// If no UUID has been set on this inbound request then we can assume we
		// have screwed up something in the authentication code. This is a sanity
		// check, but should never be encountered (ideally...).
		//
		// This will also attempt to match a specific server out of the global server
		// store and return nil if there is no match.
		uuid := sconn.Permissions.Extensions["uuid"]
		srv := c.manager.Find(func(s *server.Server) bool {
			if uuid == "" {
				return false
			}
			return s.ID() == uuid
		})
		if srv == nil {
			continue
		}

		// Spin up a SFTP server instance for the authenticated user's server allowing
		// them access to the underlying filesystem.
		handler, err := NewHandler(sconn, srv)
		if err != nil {
			return errors.WithStackIf(err)
		}
		rs := sftp.NewRequestServer(channel, handler.Handlers())
		if err := rs.Serve(); err == io.EOF {
			_ = rs.Close()
		}
	}

	return nil
}

// Generates a new ED25519 private key that is used for host authentication when
// a user connects to the SFTP server.
func (c *SFTPServer) generateED25519PrivateKey() error {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return errors.Wrap(err, "sftp: failed to generate ED25519 private key")
	}
	if err := os.MkdirAll(path.Dir(c.PrivateKeyPath()), 0o755); err != nil {
		return errors.Wrap(err, "sftp: could not create internal sftp data directory")
	}
	o, err := os.OpenFile(c.PrivateKeyPath(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return errors.WithStack(err)
	}
	defer o.Close()

	b, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return errors.Wrap(err, "sftp: failed to marshal private key into bytes")
	}
	if err := pem.Encode(o, &pem.Block{Type: "PRIVATE KEY", Bytes: b}); err != nil {
		return errors.Wrap(err, "sftp: failed to write ED25519 private key to disk")
	}
	return nil
}

func (c *SFTPServer) makeCredentialsRequest(conn ssh.ConnMetadata, t remote.SftpAuthRequestType, p string) (*ssh.Permissions, error) {
	request := remote.SftpAuthRequest{
		Type:          t,
		User:          conn.User(),
		Pass:          p,
		IP:            conn.RemoteAddr().String(),
		SessionID:     conn.SessionID(),
		ClientVersion: conn.ClientVersion(),
	}

	logger := log.WithFields(log.Fields{"subsystem": "sftp", "method": request.Type, "username": request.User, "ip": request.IP})
	logger.Debug("validating credentials for SFTP connection")

	// SECURITY: Enhanced username validation with suspicious pattern detection
	if !validUsernameRegexp.MatchString(request.User) {
		// Check for common attack patterns
		suspiciousPatterns := []string{"root", "admin", "administrator", "user", "test", "guest", "ftp", "ssh"}
		for _, pattern := range suspiciousPatterns {
			if strings.EqualFold(request.User, pattern) {
				logger.WithField("attack_pattern", "common_username").Warn("SECURITY: Brute force attack detected - common username attempted")
				break
			}
		}
		
		// Log suspicious usernames for monitoring
		if len(request.User) < 3 || len(request.User) > 50 {
			logger.WithField("attack_pattern", "unusual_length").Warn("SECURITY: Suspicious username length detected")
		}
		
		logger.Warn("failed to validate user credentials (invalid format)")
		return nil, &remote.SftpInvalidCredentialsError{}
	}

	resp, err := c.manager.Client().ValidateSftpCredentials(context.Background(), request)
	if err != nil {
		if _, ok := err.(*remote.SftpInvalidCredentialsError); ok {
			logger.Warn("failed to validate user credentials (invalid username or password)")
		} else {
			logger.WithField("error", err).Error("encountered an error while trying to validate user credentials")
		}
		return nil, err
	}

	logger.WithField("server", resp.Server).Debug("credentials validated and matched to server instance")
	permissions := ssh.Permissions{
		Extensions: map[string]string{
			"ip":          conn.RemoteAddr().String(),
			"uuid":        resp.Server,
			"user":        resp.User,
			"permissions": strings.Join(resp.Permissions, ","),
		},
	}

	return &permissions, nil
}

// PrivateKeyPath returns the path the host private key for this server instance.
func (c *SFTPServer) PrivateKeyPath() string {
	return path.Join(c.BasePath, ".sftp/id_ed25519")
}
