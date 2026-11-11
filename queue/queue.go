package queue

import (
	"errors"
	"net/url"
	"sync"
	"sync/atomic"

	whatwgUrl "github.com/nlnwa/whatwg-url/url"

	"github.com/gocolly/colly/v2"
)

// ErrInvalidThreads is returned when the queue is configured with a
// non-positive number of consumer threads, which would otherwise deadlock Run.
var ErrInvalidThreads = errors.New("queue Threads must be greater than 0")

var urlParser = whatwgUrl.NewParser(whatwgUrl.WithPercentEncodeSinglePercentSign())

// Storage is the interface of the queue's storage backend
// Storage must be concurrently safe for multiple goroutines.
type Storage interface {
	// Init initializes the storage
	Init() error
	// AddRequest adds a serialized request to the queue
	AddRequest([]byte) error
	// GetRequest pops the next request from the queue
	// or returns error if the queue is empty
	GetRequest() ([]byte, error)
	// QueueSize returns with the size of the queue
	QueueSize() (int, error)
}

// Queue is a request queue which uses a Collector to consume
// requests in multiple threads
type Queue struct {
	// Threads defines the number of consumer threads
	Threads int
	storage Storage
	mut     sync.Mutex // guards wake, stopCh and stopped
	// wake is a latch-style channel: every scheduler wakeup is delivered with
	// a non-blocking send, so Add* can never block even when no Run is active.
	wake chan struct{}
	// stopCh and stopped exist only while a Run is in progress.
	stopCh  chan struct{}
	stopped bool
}

// InMemoryQueueStorage is the default implementation of the Storage interface.
// InMemoryQueueStorage holds the request queue in memory.
type InMemoryQueueStorage struct {
	// MaxSize defines the capacity of the queue.
	// New requests are discarded if the queue size reaches MaxSize
	MaxSize int
	lock    *sync.RWMutex
	size    int
	first   *inMemoryQueueItem
	last    *inMemoryQueueItem
}

type inMemoryQueueItem struct {
	Request []byte
	Next    *inMemoryQueueItem
}

// New creates a new queue with a Storage specified in argument
// A standard InMemoryQueueStorage is used if Storage argument is nil.
func New(threads int, s Storage) (*Queue, error) {
	if threads <= 0 {
		return nil, ErrInvalidThreads
	}
	if s == nil {
		s = &InMemoryQueueStorage{MaxSize: 100000}
	}
	if err := s.Init(); err != nil {
		return nil, err
	}
	return &Queue{
		Threads: threads,
		storage: s,
	}, nil
}

// IsEmpty returns true if the queue is empty
func (q *Queue) IsEmpty() bool {
	s, _ := q.Size()
	return s == 0
}

// AddURL adds a new URL to the queue
func (q *Queue) AddURL(URL string) error {
	u, err := urlParser.Parse(URL)
	if err != nil {
		return err
	}
	u2, err := url.Parse(u.Href(false))
	if err != nil {
		return err
	}
	r := &colly.Request{
		URL:    u2,
		Method: "GET",
	}
	return q.AddRequest(r)
}

// AddRequest adds a new Request to the queue
func (q *Queue) AddRequest(r *colly.Request) error {
	err := q.storeRequest(r)
	if err != nil {
		return err
	}
	q.signal()
	return nil
}

// signal notifies a possibly running scheduler that new work is available.
// It is safe to call concurrently with Run and Stop and never blocks: the
// wake channel is unbuffered, so a signal is delivered only when the
// scheduler is actively selecting; a missed signal is harmless because every
// scheduler iteration re-checks the storage size first. Sends before a Run
// starts or after it ends are dropped because there is no consumer yet.
func (q *Queue) signal() {
	q.mut.Lock()
	wake := q.wake
	q.mut.Unlock()
	if wake == nil {
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (q *Queue) storeRequest(r *colly.Request) error {
	d, err := r.Marshal()
	if err != nil {
		return err
	}
	return q.storage.AddRequest(d)
}

// Size returns the size of the queue
func (q *Queue) Size() (int, error) {
	return q.storage.QueueSize()
}

// Run starts consumer threads and calls the Collector to perform requests. Run
// blocks while the queue has pending or in-flight requests, or until Stop is
// observed. Requests already dispatched to a worker, including their
// callbacks, always finish before Run returns. Requests that have not been
// dispatched remain in the Storage and are processed by a later Run call.
//
// After Run returns, the same Queue can be Run again to consume the remaining
// requests (including requests added after natural drain).
//
// Calling Run concurrently on the same Queue is not supported: a second Run
// while the previous one is still in progress panics. Wait for the previous
// Run to return before starting a new one.
func (q *Queue) Run(c *colly.Collector) error {
	if q.Threads <= 0 {
		return ErrInvalidThreads
	}
	q.mut.Lock()
	if q.wake != nil {
		q.mut.Unlock()
		panic("cannot call duplicate Queue.Run")
	}
	// Both channels are capacity-one latches: non-blocking sends coalesce, and
	// a signal sent before the scheduler starts waiting is retained instead of
	// lost, which closes the add/complete vs. select wakeup race. wake is the
	// external latch used by Add* and published on the Queue; done is internal
	// to this Run and only workers send on it, so its lifecycle never overlaps
	// another Run or an external signal.
	wake := make(chan struct{}, 1)
	done := make(chan struct{}, 1)
	stopCh := make(chan struct{})
	q.wake = wake
	q.stopCh = stopCh
	q.stopped = false
	q.mut.Unlock()
	defer func() {
		q.mut.Lock()
		q.wake = nil
		q.stopCh = nil
		q.stopped = false
		q.mut.Unlock()
	}()

	requestc := make(chan *colly.Request)
	var active atomic.Int32
	var activeWg sync.WaitGroup
	for i := 0; i < q.Threads; i++ {
		activeWg.Add(1)
		go independentRunner(requestc, done, &activeWg, &active)
	}
	loopErr := q.loop(c, requestc, wake, done, stopCh, &active)
	// No further requests are dispatched after this point. Closing the channel
	// makes the workers exit once every in-flight request is done.
	close(requestc)
	activeWg.Wait()
	return loopErr
}

// Stop requests the running queue to shut down. It is safe to call multiple
// times, concurrently with Run and Add*, and when no Run is active (in which
// case it is a no-op). Stop never blocks and only prevents further request
// dispatch: already dispatched requests and their callbacks run to
// completion, and Run waits for them before returning. Undispatched requests,
// including one the scheduler may have prefetched, stay in the Storage and
// are picked up by a subsequent Run.
func (q *Queue) Stop() {
	q.mut.Lock()
	defer q.mut.Unlock()
	if q.stopCh == nil || q.stopped {
		return
	}
	q.stopped = true
	close(q.stopCh)
}

func (q *Queue) loop(c *colly.Collector, requestc chan<- *colly.Request, wake <-chan struct{}, done <-chan struct{}, stopCh <-chan struct{}, active *atomic.Int32) error {
	// prefetched holds a request popped from storage but not yet handed to a
	// worker. It is returned to storage before the scheduler exits so it can
	// never be executed twice or lost. Keeping the parsed request and its raw
	// bytes together prevents the two from drifting across loop iterations.
	var prefetched *prefetchedRequest
	for {
		size, err := q.storage.QueueSize()
		if err != nil {
			return q.requeuePrefetched(prefetched, err)
		}
		if size == 0 && active.Load() == 0 && prefetched == nil {
			return q.requeuePrefetched(prefetched, nil)
		}
		sent := requestc
		var req *colly.Request
		if prefetched == nil && size > 0 {
			loaded, loadErr := q.loadRequest(c)
			if loadErr != nil {
				var storageErr *loopStorageError
				if errors.As(loadErr, &storageErr) {
					// Storage failures are fatal: exit with a clean shutdown.
					return q.requeuePrefetched(prefetched, storageErr.err)
				}
				if errors.Is(loadErr, errEmptyStorage) {
					// Concurrent drain race: the reported item vanished before
					// pop. Wait for an event and re-evaluate instead of spinning.
					select {
					case <-wake:
					case <-done:
					case <-stopCh:
						return q.requeuePrefetched(prefetched, nil)
					}
				}
				// Invalid serialized requests are skipped, matching the
				// previous behavior: GetRequest already removed them and the
				// unmarshal error is intentionally ignored.
				continue
			}
			prefetched = loaded
		}
		if prefetched == nil {
			// Nothing to dispatch right now: nil channel blocks the send case.
			sent = nil
		} else {
			req = prefetched.req
		}
		select {
		case sent <- req:
			active.Add(1)
			prefetched = nil
		case <-wake:
			// Re-check the queue: an Add* call may have stored new work while
			// all workers were busy, a racing add landed during natural drain,
			// or a worker just finished a request. The authoritative in-flight
			// count is read from active at the top of every iteration.
		case <-done:
			// A worker finished a request; active was decremented atomically.
		case <-stopCh:
			return q.requeuePrefetched(prefetched, nil)
		}
	}
}

type prefetchedRequest struct {
	req *colly.Request
	raw []byte
}

// requeuePrefetched returns a request popped from storage but never dispatched
// back to the storage. It is called on every scheduler exit path so a stop or
// error can neither lose the request nor execute it twice.
func (q *Queue) requeuePrefetched(prefetched *prefetchedRequest, cause error) error {
	if prefetched == nil {
		return cause
	}
	if err := q.storage.AddRequest(prefetched.raw); err != nil {
		if cause != nil {
			return cause
		}
		return err
	}
	return cause
}

func independentRunner(requestc <-chan *colly.Request, done chan<- struct{}, activeWg *sync.WaitGroup, active *atomic.Int32) {
	defer activeWg.Done()
	for req := range requestc {
		req.Do()
		active.Add(-1)
		// Latch done: non-blocking, coalescing and retained until the
		// scheduler selects, so a completion cannot be lost.
		select {
		case done <- struct{}{}:
		default:
		}
	}
}

// loopStorageError wraps a failure of the Storage backend, as opposed to an
// error decoding a single (invalid) serialized request which is skipped.
type loopStorageError struct {
	err error
}

func (e *loopStorageError) Error() string { return e.err.Error() }
func (e *loopStorageError) Unwrap() error { return e.err }

func (q *Queue) loadRequest(c *colly.Collector) (*prefetchedRequest, error) {
	buf, err := q.storage.GetRequest()
	if err != nil {
		return nil, &loopStorageError{err}
	}
	if len(buf) == 0 {
		// A compliant empty-queue response (the in-memory storage returns nil,
		// nil): nothing was popped, so simply retry the scheduler loop.
		return nil, errEmptyStorage
	}
	copied := make([]byte, len(buf))
	copy(copied, buf)
	req, err := c.UnmarshalRequest(copied)
	if err != nil {
		return nil, err
	}
	return &prefetchedRequest{req: req, raw: copied}, nil
}

// errEmptyStorage marks an empty GetRequest result that is not a failure.
var errEmptyStorage = errors.New("queue storage is empty")

// Init implements Storage.Init() function
func (q *InMemoryQueueStorage) Init() error {
	q.lock = &sync.RWMutex{}
	return nil
}

// AddRequest implements Storage.AddRequest() function
func (q *InMemoryQueueStorage) AddRequest(r []byte) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	// Discard URLs if size limit exceeded
	if q.MaxSize > 0 && q.size >= q.MaxSize {
		return colly.ErrQueueFull
	}
	i := &inMemoryQueueItem{Request: r}
	if q.first == nil {
		q.first = i
	} else {
		q.last.Next = i
	}
	q.last = i
	q.size++
	return nil
}

// GetRequest implements Storage.GetRequest() function
func (q *InMemoryQueueStorage) GetRequest() ([]byte, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.size == 0 {
		return nil, nil
	}
	r := q.first.Request
	q.first = q.first.Next
	q.size--
	return r, nil
}

// QueueSize implements Storage.QueueSize() function
func (q *InMemoryQueueStorage) QueueSize() (int, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.size, nil
}
