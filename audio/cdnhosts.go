package audio

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	librespot "github.com/devgianlu/go-librespot"
)

// HostStats is what is remembered about one CDN host, across restarts.
type HostStats struct {
	LastSuccess time.Time `json:"last_success"`
	LastFailure time.Time `json:"last_failure"`
	// Running average of how long a chunk took from request to complete body, in ms.
	LatencyMs float64 `json:"latency_ms"`
	Successes int     `json:"successes"`
	Failures  int     `json:"failures"`
}

const (
	// Weight of the newest measurement in the latency average.
	latencyAlpha = 0.3
	// Writes are debounced: a burst of chunk results becomes one write.
	cdnHostsSaveDelay = 2 * time.Second
)

// CdnHosts ranks Spotify's CDN hosts by how they behaved, keeps the ranking in a file, and
// keeps hosts that failed recently out of the way. All methods are safe on a nil *CdnHosts
// (no memory, no file: Spotify's order is used as is).
type CdnHosts struct {
	log        librespot.Logger
	path       string
	quarantine time.Duration

	mu        sync.Mutex
	hosts     map[string]*HostStats
	saveTimer *time.Timer
}

// NewCdnHosts loads the table from path (missing or unreadable: an empty table, logged).
// A host counts as quarantined for `quarantine` after a failure that no success followed.
func NewCdnHosts(log librespot.Logger, path string, quarantine time.Duration) *CdnHosts {
	c := &CdnHosts{log: log, path: path, quarantine: quarantine, hosts: map[string]*HostStats{}}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(data, &c.hosts); err != nil {
			log.WithError(err).Warnf("ignoring unreadable cdn hosts file %s", path)
			c.hosts = map[string]*HostStats{}
		} else {
			log.Debugf("loaded %d cdn hosts from %s", len(c.hosts), path)
		}
	case !os.IsNotExist(err):
		log.WithError(err).Warnf("cannot read cdn hosts file %s", path)
	}
	return c
}

func (c *CdnHosts) stats(host string) *HostStats {
	s := c.hosts[host]
	if s == nil {
		s = &HostStats{}
		c.hosts[host] = s
	}
	return s
}

func (s *HostStats) quarantined(now time.Time, quarantine time.Duration) bool {
	return !s.LastFailure.IsZero() && s.LastFailure.After(s.LastSuccess) &&
		now.Sub(s.LastFailure) < quarantine
}

// Quarantined reports whether the host failed recently with no success since.
func (c *CdnHosts) Quarantined(host string) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.hosts[host]
	return ok && s.quarantined(time.Now(), c.quarantine)
}

// RecordSuccess notes a completed chunk and how long it took; it also lifts a quarantine.
func (c *CdnHosts) RecordSuccess(host string, took time.Duration) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats(host)
	ms := float64(took.Milliseconds())
	if s.Successes == 0 {
		s.LatencyMs = ms
	} else {
		s.LatencyMs = latencyAlpha*ms + (1-latencyAlpha)*s.LatencyMs
	}
	s.LastSuccess = time.Now()
	s.Successes++
	c.scheduleSave()
}

// RecordFailure notes a failed or stalled attempt: the host is quarantined from now.
func (c *CdnHosts) RecordFailure(host string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats(host)
	s.LastFailure = time.Now()
	s.Failures++
	c.scheduleSave()
}

// Order sorts the URLs for one file: hosts in good standing first, fastest first, hosts never
// seen after them (Spotify's order kept among equals), quarantined hosts last. The list is
// never shortened: a quarantined host is still the last resort.
func (c *CdnHosts) Order(urls []*url.URL) []*url.URL {
	if c == nil || len(urls) < 2 {
		return urls
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	type ranked struct {
		u    *url.URL
		rank int     // 0 known good, 1 unknown, 2 quarantined
		ms   float64 // within rank 0
		pos  int     // Spotify's order, for stability
	}
	items := make([]ranked, len(urls))
	for i, u := range urls {
		items[i] = ranked{u: u, rank: 1, pos: i}
		s, ok := c.hosts[u.Host]
		if !ok {
			continue
		}
		switch {
		case s.quarantined(now, c.quarantine):
			items[i].rank = 2
		case s.Successes > 0:
			items[i].rank = 0
			items[i].ms = s.LatencyMs
		}
	}
	sort.SliceStable(items, func(a, b int) bool {
		if items[a].rank != items[b].rank {
			return items[a].rank < items[b].rank
		}
		if items[a].rank == 0 && items[a].ms != items[b].ms {
			return items[a].ms < items[b].ms
		}
		return items[a].pos < items[b].pos
	})
	out := make([]*url.URL, len(items))
	for i, item := range items {
		out[i] = item.u
	}
	return out
}

func (c *CdnHosts) scheduleSave() {
	if c.path == "" {
		return
	}
	if c.saveTimer == nil {
		c.saveTimer = time.AfterFunc(cdnHostsSaveDelay, c.save)
	} else {
		c.saveTimer.Reset(cdnHostsSaveDelay)
	}
}

func (c *CdnHosts) save() {
	c.mu.Lock()
	data, err := json.MarshalIndent(c.hosts, "", "  ")
	c.mu.Unlock()
	if err != nil {
		c.log.WithError(err).Warnf("cannot encode cdn hosts")
		return
	}
	tmp := c.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(c.path), 0o700); err == nil {
		err = os.WriteFile(tmp, data, 0o600)
	}
	if err == nil {
		err = os.Rename(tmp, c.path)
	}
	if err != nil {
		c.log.WithError(err).Warnf("cannot write cdn hosts file %s", c.path)
	}
}
