package daemon

import "time"

// Config carries the runtime configuration for a daemon instance.
type Config struct {
	DeviceId    string
	DeviceName  string
	DeviceType  string
	ClientToken string

	AudioBackend                 string
	AudioBackendRuntimeSocket    string
	AudioDevice                  string
	MixerDevice                  string
	MixerControlName             string
	AudioBufferTime              int
	AudioPeriodCount             int
	AudioOutputPipe              string
	AudioOutputPipeFormat        string
	AudioOutputPipeWaitForReader bool

	Bitrate                   int
	VolumeSteps               uint32
	InitialVolume             uint32
	IgnoreLastVolume          bool
	NormalisationDisabled     bool
	NormalisationUseAlbumGain bool
	NormalisationPregain      float32
	CrossfadeDuration         int
	ExternalVolume            bool
	DisableAutoplay           bool

	// SkipDebounce is how long a skip that follows another within this window
	// waits before loading the track it landed on, so that a burst of next or
	// prev presses costs one load rather than one per press. Zero loads every
	// skip immediately.
	SkipDebounce time.Duration

	// UnplayableSkipLimit is how many unplayable tracks in a row (refused audio key,
	// restricted, no supported format) are skipped past before playback stops. A context
	// full of them otherwise ran through to its end and on into autoplay, a track a second.
	UnplayableSkipLimit int

	ZeroconfEnabled               bool
	ZeroconfPort                  int
	ZeroconfBackend               string
	ZeroconfInterfacesToAdvertise []string

	FlacEnabled bool

	// PreferFirewallFriendlyPorts tries accesspoints on 443 and 80 before the
	// default 4070, which some networks block outbound.
	PreferFirewallFriendlyPorts bool

	// ImageSize selects which cover-art image variant the API server returns:
	// "default", "small", "medium", "large", "xlarge".
	ImageSize string

	Cache CacheConfig

	// CdnRace is how many of Spotify's CDN hosts a track's first chunk is requested from
	// in parallel; the quickest serves the rest of the track. 1 disables the race.
	CdnRace int
	// CdnQuarantine is how long a CDN host that failed or stalled is kept last in line
	// (with no success since). Zero disables the quarantine.
	CdnQuarantine time.Duration
	// CdnHostsFile keeps what was learned about the CDN hosts across restarts; empty
	// keeps it in memory only.
	CdnHostsFile string

	Metadata MetadataConfig

	Credentials CredentialsConfig
}

// MetadataConfig configures the in-memory track metadata cache behind the
// next_track status field and the /context/tracks listing. Everything here is
// opt-in: a headless speaker has no use for metadata beyond the playing track
// and should not pay network requests for it.
type MetadataConfig struct {
	// Enabled turns on the metadata cache, the batched fetch of metadata for
	// the tracks around the playback position, and the /context/tracks
	// endpoint. Off, the daemon performs no metadata request playback does not
	// need.
	Enabled bool
	// ContextSweep additionally resolves metadata for the whole context when
	// one starts playing, so every track is known before the user skips
	// anywhere. Requires Enabled.
	ContextSweep bool
	// MaxTracks caps how many tracks of a context are enumerated and swept.
	MaxTracks int
}

// CacheConfig configures the on-disk cache for downloaded (encrypted) audio
// files.
type CacheConfig struct {
	// Enabled turns the audio file cache on or off.
	Enabled bool
	// Dir is the directory the cache is stored in.
	Dir string
	// SizeLimit is the maximum total size of the cached audio files in bytes.
	// A value of zero disables eviction (unbounded cache).
	SizeLimit int64
}

type CredentialsConfig struct {
	Type         string
	Interactive  InteractiveCredentials
	SpotifyToken SpotifyTokenCredentials
	Zeroconf     ZeroconfCredentials
}

type InteractiveCredentials struct {
	CallbackPort int
}

type SpotifyTokenCredentials struct {
	Username    string
	AccessToken string
}

type ZeroconfCredentials struct {
	PersistCredentials bool
}
