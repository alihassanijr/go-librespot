package audio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	librespot "github.com/devgianlu/go-librespot"
)

const (
	DefaultChunkSize = 512 * 1024
	PrefetchCount    = 3

	// A chunk attempt is abandoned once the body has delivered no byte for this long: a
	// healthy CDN host streams a chunk continuously; one that answers and then stalls the
	// body (seen with one Spotify host, as ~30 s stalls at track starts and mid-track) is
	// told apart within seconds instead of holding the player for the client's timeout.
	ChunkProgressTimeout = 3 * time.Second
	// Backstop for a whole attempt, body included, for a host that trickles rather than
	// stalls. At the lowest Spotify bitrate a chunk is ~26 s of audio.
	ChunkAttemptTimeout = 15 * time.Second
	// Attempts per chunk; each failed attempt moves on to the next CDN URL when there
	// is one, so a bad host is left behind instead of retried.
	ChunkAttempts = 4
)

var contentRangeRegexp = regexp.MustCompile("^bytes (\\d+)-(\\d+)/(\\d+)$")

func parseContentRange(resp *http.Response) (start int64, end int64, size int64, err error) {
	header := resp.Header.Get("Content-Range")
	if len(header) == 0 {
		return 0, 0, 0, fmt.Errorf("invalid first chunk response status: no Content-Range header")
	}

	match := contentRangeRegexp.FindStringSubmatch(header)
	if len(match) == 0 {
		return 0, 0, 0, fmt.Errorf("invalid content range header: %s", header)
	} else if start, err = strconv.ParseInt(match[1], 10, 0); err != nil {
		return 0, 0, 0, fmt.Errorf("invalid content range start: %w", err)
	} else if end, err = strconv.ParseInt(match[2], 10, 0); err != nil {
		return 0, 0, 0, fmt.Errorf("invalid content range end: %w", err)
	} else if size, err = strconv.ParseInt(match[3], 10, 0); err != nil {
		return 0, 0, 0, fmt.Errorf("invalid content range size: %w", err)
	}

	return start, end, size, nil
}

type chunkItem struct {
	*sync.Cond
	data     []byte
	fetching bool
}

func newChunkItem() *chunkItem {
	return &chunkItem{Cond: sync.NewCond(&sync.Mutex{})}
}

type HttpChunkedReader struct {
	log    librespot.Logger
	client *http.Client

	// The CDN URLs for this file, all serving the same bytes; urlIdx is the one in use.
	// A failed attempt moves to the next one.
	// TODO: these urls will expire at some point
	urlMu  sync.Mutex
	urls   []*url.URL
	urlIdx int

	// Ranks hosts and remembers failures across tracks and restarts; nil = no memory.
	hosts *CdnHosts
	// How many hosts the first chunk is requested from in parallel (at least 1).
	race int

	chunks []*chunkItem

	len int64
	pos int64

	prefetchMu sync.Mutex
	prefetchWg sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc

	completeMu      sync.Mutex
	completedChunks int
	onComplete      func(io.ReaderAt, int64)
	onCompleteFired bool

	latMu     sync.Mutex
	latencies []time.Duration
}

func NewHttpChunkedReader(log librespot.Logger, client *http.Client, audioUrl string) (_ *HttpChunkedReader, err error) {
	return NewHttpChunkedReaderWithFallbacks(log, client, []string{audioUrl}, nil, 1)
}

// NewHttpChunkedReaderWithFallbacks reads one file that every URL serves. The URLs are
// ordered by what hosts remembers (good and fast first, quarantined last); the first chunk
// is requested from the first `race` hosts at once and the quickest wins the rest of the
// file; a chunk attempt that fails or stalls moves on to the next host, for every chunk.
func NewHttpChunkedReaderWithFallbacks(log librespot.Logger, client *http.Client, audioUrls []string, hosts *CdnHosts, race int) (_ *HttpChunkedReader, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &HttpChunkedReader{
		log:    log,
		client: client,
		ctx:    ctx,
		cancel: cancel,
		hosts:  hosts,
		race:   max(race, 1),
	}

	for _, audioUrl := range audioUrls {
		parsed, err := url.Parse(audioUrl)
		if err != nil {
			log.WithError(err).Warnf("skipping unparsable resource url")
			continue
		}
		r.urls = append(r.urls, parsed)
	}
	if len(r.urls) == 0 {
		cancel()
		return nil, fmt.Errorf("no usable resource url")
	}
	r.urls = hosts.Order(r.urls)

	defer func() {
		if err != nil {
			r.cancel()
		}
	}()

	// request the first chunk, needed for the complete content length
	data, size, err := r.raceFirstChunk()
	if err != nil {
		return nil, fmt.Errorf("failed requesting first chunk: %w", err)
	}
	r.len = size

	// create the necessary amount of chunks
	totalChunks := (r.len + DefaultChunkSize - 1) / DefaultChunkSize

	r.chunks = make([]*chunkItem, totalChunks)
	for i := int64(0); i < totalChunks; i++ {
		r.chunks[i] = newChunkItem()
	}

	r.chunks[0].data = data

	// The first chunk is fetched eagerly here, so count it towards completion.
	r.completedChunks = 1

	log.Debugf("fetched first chunk of %d, total size is %d bytes", len(r.chunks), r.len)
	return r, nil
}

func (r *HttpChunkedReader) currentUrl() *url.URL {
	r.urlMu.Lock()
	defer r.urlMu.Unlock()
	return r.urls[r.urlIdx]
}

func (r *HttpChunkedReader) useUrl(winner *url.URL) {
	r.urlMu.Lock()
	defer r.urlMu.Unlock()
	for i, u := range r.urls {
		if u == winner {
			r.urlIdx = i
			return
		}
	}
}

// raceFirstChunk requests chunk 0 from the first `race` hosts in parallel; the first
// complete body wins and its host serves the rest of the file. Losers are cancelled, which
// is not held against them; a loser that fails or stalls before the winner finishes is. If
// every racer fails, the ordinary sequential retries take over.
func (r *HttpChunkedReader) raceFirstChunk() ([]byte, int64, error) {
	r.urlMu.Lock()
	candidates := append([]*url.URL(nil), r.urls[:min(r.race, len(r.urls))]...)
	r.urlMu.Unlock()
	if len(candidates) == 1 {
		return r.downloadAndRead(0)
	}

	type outcome struct {
		u    *url.URL
		data []byte
		size int64
		err  error
	}
	raceCtx, cancelRace := context.WithCancel(r.ctx)
	defer cancelRace()
	results := make(chan outcome, len(candidates))
	for _, u := range candidates {
		go func(u *url.URL) {
			data, size, err := r.fetchAttemptOn(0, u, raceCtx)
			results <- outcome{u: u, data: data, size: size, err: err}
		}(u)
	}
	hosts := make([]string, len(candidates))
	for i, u := range candidates {
		hosts[i] = u.Host
	}
	r.log.Debugf("racing first chunk on %v", hosts)

	failed := 0
	for failed < len(candidates) {
		res := <-results
		if res.err == nil {
			r.useUrl(res.u)
			cancelRace()
			return res.data, res.size, nil
		}
		if errors.Is(res.err, net.ErrClosed) {
			return nil, 0, res.err
		}
		failed++
	}
	r.log.Warnf("every racer for the first chunk failed, falling back to sequential attempts")
	return r.downloadAndRead(0)
}

// nextUrl moves to the next CDN URL (round robin) after a failed attempt on the given one,
// unless another attempt already moved on.
func (r *HttpChunkedReader) nextUrl(failed *url.URL) {
	r.urlMu.Lock()
	defer r.urlMu.Unlock()
	if len(r.urls) < 2 || r.urls[r.urlIdx] != failed {
		return
	}
	r.urlIdx = (r.urlIdx + 1) % len(r.urls)
	r.log.Warnf("switching cdn host %s -> %s", failed.Host, r.urls[r.urlIdx].Host)
}

func (r *HttpChunkedReader) closeErr(err error) error {
	if err != nil && r.isClosed() {
		return net.ErrClosed
	}

	return err
}

func (r *HttpChunkedReader) isClosed() bool {
	return r.ctx.Err() != nil
}

// fetchAttempt downloads one chunk from the current URL; a failure moves the reader to the
// next URL.
func (r *HttpChunkedReader) fetchAttempt(idx int) ([]byte, int64, error) {
	target := r.currentUrl()
	data, size, err := r.fetchAttemptOn(idx, target, r.ctx)
	if err != nil && !errors.Is(err, net.ErrClosed) {
		r.nextUrl(target)
	}
	return data, size, err
}

// errLostRace: the attempt was cancelled by the reader itself (another racer won).
var errLostRace = errors.New("lost the race")

// fetchAttemptOn downloads one chunk from target, body included, within ChunkAttemptTimeout
// and with no more than ChunkProgressTimeout between bytes. A failure is logged with its
// error (it tells DNS, connect, TLS, no response and a stalled body apart) and recorded
// against the host; a success is recorded with how long it took. Returns the chunk and the
// file's total size from the Content-Range header.
func (r *HttpChunkedReader) fetchAttemptOn(idx int, target *url.URL, parent context.Context) ([]byte, int64, error) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, ChunkAttemptTimeout)
	defer cancel()

	// No byte for ChunkProgressTimeout: cancel the attempt (the body read then fails).
	stalled := false
	watchdog := time.AfterFunc(ChunkProgressTimeout, func() {
		stalled = true
		cancel()
	})
	defer watchdog.Stop()

	fail := func(err error) ([]byte, int64, error) {
		err = r.closeErr(err)
		if errors.Is(err, net.ErrClosed) {
			return nil, 0, backoff.Permanent(err)
		}
		if !stalled && parent.Err() != nil && r.ctx.Err() == nil {
			// Cancelled by the race, not by the host: no mark against it.
			r.log.Debugf("chunk %d fetch on %s cancelled, another host was faster", idx, target.Host)
			return nil, 0, errLostRace
		}
		if stalled {
			err = fmt.Errorf("no data for %s: %w", ChunkProgressTimeout, err)
		}
		r.log.Warnf("chunk %d fetch attempt on %s failed: %v", idx, target.Host, err)
		r.hosts.RecordFailure(target.Host)
		return nil, 0, err
	}

	resp, err := r.client.Do((&http.Request{
		Method: "GET",
		URL:    target,
		Header: http.Header{
			"User-Agent": []string{librespot.UserAgent()},
			"Range": []string{fmt.Sprintf("bytes=%d-%d",
				idx*DefaultChunkSize,
				min(max(r.len, DefaultChunkSize), int64((idx+1)*DefaultChunkSize))-1,
			)},
		},
	}).WithContext(ctx))
	if err != nil {
		return fail(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusPartialContent {
		return fail(fmt.Errorf("unexpected chunk response status: %w",
			&librespot.HTTPStatusError{Endpoint: "audio file", StatusCode: resp.StatusCode}))
	}

	_, _, size, err := parseContentRange(resp)
	if err != nil {
		return fail(fmt.Errorf("invalid chunk content range response: %w", err))
	}

	// Each byte that arrives gives the watchdog another ChunkProgressTimeout.
	progress := &progressReader{Reader: resp.Body, OnData: func() {
		watchdog.Reset(ChunkProgressTimeout)
	}}
	data, err := io.ReadAll(r.measureLatency(progress))
	if err != nil {
		return fail(fmt.Errorf("reading body: %w", err))
	}

	r.hosts.RecordSuccess(target.Host, time.Since(started))
	return data, size, nil
}

// progressReader calls OnData after every read that delivered bytes.
type progressReader struct {
	io.Reader
	OnData func()
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.Reader.Read(b)
	if n > 0 && p.OnData != nil {
		p.OnData()
	}
	return n, err
}

type chunkResult struct {
	data []byte
	size int64
}

// downloadAndRead fetches a chunk with retries; each failed attempt has already moved on to
// the next CDN URL, so the retries spread over the hosts.
func (r *HttpChunkedReader) downloadAndRead(idx int) ([]byte, int64, error) {
	retryBackoff := backoff.WithContext(
		backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), ChunkAttempts-1),
		r.ctx,
	)

	result, err := backoff.RetryWithData(func() (chunkResult, error) {
		data, size, err := r.fetchAttempt(idx)
		return chunkResult{data: data, size: size}, err
	}, retryBackoff)
	if err != nil {
		return nil, 0, fmt.Errorf("failed downloading chunk %d: %w", idx, r.closeErr(err))
	}

	return result.data, result.size, nil
}

func (r *HttpChunkedReader) fetchChunk(idx int) ([]byte, error) {
	chunk := r.chunks[idx]

	chunk.L.Lock()
	for {
		if r.isClosed() {
			chunk.L.Unlock()
			return nil, net.ErrClosed
		}

		if chunk.data != nil {
			data := chunk.data
			chunk.L.Unlock()
			return data, nil
		}

		if !chunk.fetching {
			chunk.fetching = true
			chunk.L.Unlock()
			break
		}

		chunk.Wait()
	}

	data, _, err := r.downloadAndRead(idx)
	if err != nil {
		chunk.L.Lock()
		chunk.fetching = false
		chunk.Broadcast()
		chunk.L.Unlock()
		return nil, err
	}

	chunk.L.Lock()
	chunk.data = data
	chunk.fetching = false
	chunk.Broadcast()
	chunk.L.Unlock()

	r.markChunkComplete()

	r.log.Debugf("fetched chunk %d/%d, size: %d", idx, len(r.chunks)-1, len(data))
	if r.isClosed() {
		return nil, net.ErrClosed
	}

	return data, nil
}

func (r *HttpChunkedReader) prefetchChunks(curr int) {
	for i := curr + 1; i < curr+1+PrefetchCount; i++ {
		if i >= len(r.chunks) {
			break
		}

		if !r.startPrefetch(i) {
			return
		}
	}
}

func (r *HttpChunkedReader) startPrefetch(idx int) bool {
	r.prefetchMu.Lock()
	defer r.prefetchMu.Unlock()

	if r.isClosed() {
		return false
	}

	r.prefetchWg.Add(1)
	go func() {
		defer r.prefetchWg.Done()
		_, _ = r.fetchChunk(idx)
	}()

	return true
}

func (r *HttpChunkedReader) Read(p []byte) (n int, err error) {
	n, err = r.ReadAt(p, r.pos)
	r.pos += int64(n)
	return n, err
}

func (r *HttpChunkedReader) ReadAt(p []byte, pos int64) (n int, _ error) {
	if r.isClosed() {
		return 0, net.ErrClosed
	}

	chunkIdx, off := int(pos/DefaultChunkSize), int(pos%DefaultChunkSize)
	if chunkIdx >= len(r.chunks) {
		return 0, io.EOF
	}

	// start prefetching next chunks
	r.prefetchChunks(chunkIdx)

	n = 0
	for len(p) > 0 {
		if chunkIdx >= len(r.chunks) {
			return n, io.EOF
		}

		// get the chunk data
		chunk, err := r.fetchChunk(chunkIdx)
		if err != nil {
			return n, fmt.Errorf("failed reading chunk %d: %w", chunkIdx, err)
		}

		// read the chunk data
		c := chunk[min(off, len(chunk)):]
		if len(c) > len(p) {
			// the chunk is bigger than our output buffer, just copy everything and return
			n += copy(p, c[:len(p)])
			return n, nil
		}

		// the chunk is smaller than the available output buffer space, copy the chunk and advance
		n += copy(p, c)
		p = p[len(c):]

		// try to advance to next chunk
		chunkIdx++
		off = 0
	}

	return n, nil
}

func (r *HttpChunkedReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		if r.pos+offset < 0 || r.pos+offset >= r.len {
			return 0, fmt.Errorf("invalid seek position")
		}

		r.pos += offset
		return r.pos, nil
	case io.SeekStart:
		if offset < 0 || offset >= r.len {
			return 0, fmt.Errorf("invalid seek position")
		}

		r.pos = offset
		return r.pos, nil
	case io.SeekEnd:
		if r.len+offset < 0 || r.len+offset >= r.len {
			return 0, fmt.Errorf("invalid seek position")
		}

		r.pos = r.len + offset
		return r.pos, nil
	default:
		panic("unknown seek whence")
	}
}

func (r *HttpChunkedReader) measureLatency(rr io.Reader) io.Reader {
	return &LatencyReader{
		Reader:   rr,
		Callback: r.recordLatency,
	}
}

func (r *HttpChunkedReader) recordLatency(latency time.Duration) {
	r.latMu.Lock()
	defer r.latMu.Unlock()

	r.latencies = append(r.latencies, latency)
}

func (r *HttpChunkedReader) latencySnapshot() []time.Duration {
	r.latMu.Lock()
	defer r.latMu.Unlock()

	latencies := make([]time.Duration, len(r.latencies))
	copy(latencies, r.latencies)
	return latencies
}

func (r *HttpChunkedReader) Size() int64 {
	return r.len
}

// OnComplete registers a callback invoked once, in a separate goroutine, as
// soon as every chunk has been downloaded. The callback receives the reader as
// an io.ReaderAt (serving the fully-buffered chunks) and the total size, which
// allows the complete encrypted file to be persisted to a cache. If the file is
// already fully downloaded, the callback fires immediately.
func (r *HttpChunkedReader) OnComplete(cb func(io.ReaderAt, int64)) {
	r.completeMu.Lock()
	r.onComplete = cb
	r.maybeFireComplete()
	r.completeMu.Unlock()
}

// markChunkComplete records that one more chunk finished downloading and fires
// the completion callback when all chunks are present.
func (r *HttpChunkedReader) markChunkComplete() {
	r.completeMu.Lock()
	r.completedChunks++
	r.maybeFireComplete()
	r.completeMu.Unlock()
}

// maybeFireComplete fires the completion callback exactly once. The caller must
// hold completeMu.
func (r *HttpChunkedReader) maybeFireComplete() {
	if r.onCompleteFired || r.onComplete == nil || r.completedChunks < len(r.chunks) {
		return
	}

	r.onCompleteFired = true
	cb := r.onComplete
	go cb(r, r.len)
}

func (r *HttpChunkedReader) Url() *url.URL {
	return r.currentUrl()
}

func (r *HttpChunkedReader) InitialLatency() time.Duration {
	latencies := r.latencySnapshot()
	if len(latencies) == 0 {
		return 0
	}

	return latencies[0]
}

func (r *HttpChunkedReader) MaxLatency() time.Duration {
	latencies := r.latencySnapshot()
	if len(latencies) == 0 {
		return 0
	}

	maxLatency := latencies[0]
	for _, latency := range latencies {
		if latency > maxLatency {
			maxLatency = latency
		}
	}

	return maxLatency
}

func (r *HttpChunkedReader) MinLatency() time.Duration {
	latencies := r.latencySnapshot()
	if len(latencies) == 0 {
		return 0
	}

	minLatency := latencies[0]
	for _, latency := range latencies {
		if latency < minLatency {
			minLatency = latency
		}
	}

	return minLatency
}

func (r *HttpChunkedReader) AvgLatencyMs() float64 {
	latencies := r.latencySnapshot()
	if len(latencies) == 0 {
		return 0
	}

	var sum time.Duration
	for _, latency := range latencies {
		sum += latency
	}

	return float64(sum.Milliseconds()) / float64(len(latencies))
}

func (r *HttpChunkedReader) MedianLatency() time.Duration {
	latencies := r.latencySnapshot()
	if len(latencies) == 0 {
		return 0
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	mid := len(latencies) / 2
	if len(latencies)%2 == 0 {
		return (latencies[mid-1] + latencies[mid]) / 2
	}

	return latencies[mid]
}

func (r *HttpChunkedReader) TotalTime() time.Duration {
	latencies := r.latencySnapshot()

	var sum time.Duration
	for _, latency := range latencies {
		sum += latency
	}
	return sum
}

func (r *HttpChunkedReader) Close() error {
	r.prefetchMu.Lock()
	r.cancel()
	r.prefetchMu.Unlock()

	for _, chunk := range r.chunks {
		chunk.L.Lock()
		chunk.Broadcast()
		chunk.L.Unlock()
	}

	r.prefetchWg.Wait()
	return nil
}
