package queue

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocolly/colly/v2"
)

// blockTestTimeout bounds every concurrency test so a wakeup/lifecycle bug
// surfaces as a failure instead of hanging the whole package forever.
const blockTestTimeout = 10 * time.Second

// roundTripFunc turns a function into an http.RoundTripper, letting the tests
// drive real Collector callbacks without opening any network socket.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newTransportCollector(t *testing.T, rt http.RoundTripper) *colly.Collector {
	t.Helper()
	c := colly.NewCollector(colly.AllowURLRevisit())
	c.SetClient(&http.Client{Transport: rt})
	return c
}

func okResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(nil)),
	}
}

// newHTTPTestServer starts an httptest server, returning nil when the
// environment forbids listening on local sockets instead of panicking.
func newHTTPTestServer(t *testing.T, handler http.Handler) (server *httptest.Server) {
	t.Helper()
	defer func() {
		if recover() != nil {
			server = nil
		}
	}()
	server = httptest.NewServer(handler)
	return server
}

func testURL(id int) string {
	return fmt.Sprintf("http://queue.test/req/%d", id)
}

func mustAddURL(t *testing.T, q *Queue, u string) {
	t.Helper()
	if err := q.AddURL(u); err != nil {
		t.Fatalf("AddURL(%q) error: %v", u, err)
	}
}

func mustAddRequest(t *testing.T, q *Queue, u string) {
	t.Helper()
	parsed, err := url.Parse(u)
	if err != nil {
		t.Fatalf("parse %q: %v", u, err)
	}
	if err := q.AddRequest(&colly.Request{URL: parsed, Method: "GET"}); err != nil {
		t.Fatalf("AddRequest(%q) error: %v", u, err)
	}
}

func mustSize(t *testing.T, q *Queue) int {
	t.Helper()
	size, err := q.Size()
	if err != nil {
		t.Fatalf("Size() error: %v", err)
	}
	return size
}

// runWithTimeout fails the test if Run does not return in time, which is the
// direct symptom of a lost wakeup or a worker that cannot shut down.
func runWithTimeout(t *testing.T, q *Queue, c *colly.Collector) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- q.Run(c) }()
	select {
	case err := <-done:
		return err
	case <-time.After(blockTestTimeout):
		t.Fatalf("Queue.Run did not return within %s", blockTestTimeout)
		return nil
	}
}

// TestRunDrainAndRerun covers natural drain followed by another Run, including
// requests added after the first run already finished.
func TestRunDrainAndRerun(t *testing.T) {
	q, err := New(4, nil)
	if err != nil {
		t.Fatal(err)
	}
	var seenMu sync.Mutex
	seen := make(map[string]int)
	c := newTransportCollector(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		seenMu.Lock()
		seen[req.URL.String()]++
		seenMu.Unlock()
		return okResponse(), nil
	}))

	for i := 0; i < 25; i++ {
		mustAddURL(t, q, testURL(i))
	}
	if err := runWithTimeout(t, q, c); err != nil {
		t.Fatalf("first Run error: %v", err)
	}
	if !q.IsEmpty() {
		t.Fatalf("queue should be empty after drain, size=%d", mustSize(t, q))
	}
	if len(seen) != 25 {
		t.Fatalf("first run executed %d unique requests, want 25", len(seen))
	}

	// A fresh Run must work after natural drain, without a background consumer.
	for i := 25; i < 40; i++ {
		mustAddURL(t, q, testURL(i))
	}
	c2 := newTransportCollector(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		seenMu.Lock()
		seen[req.URL.String()]++
		seenMu.Unlock()
		return okResponse(), nil
	}))
	if err := runWithTimeout(t, q, c2); err != nil {
		t.Fatalf("second Run error: %v", err)
	}
	if !q.IsEmpty() {
		t.Fatalf("queue should be empty after second drain, size=%d", mustSize(t, q))
	}
	if len(seen) != 40 {
		t.Fatalf("second run executed %d unique requests total, want 40", len(seen))
	}
	for u, count := range seen {
		if count != 1 {
			t.Fatalf("request %q executed %d times, want exactly once", u, count)
		}
	}
}

// TestFIFODispatch verifies dispatch ordering with a single worker: pop and
// hand-off happen on the single scheduler goroutine, so order must be FIFO.
func TestFIFODispatch(t *testing.T) {
	q, err := New(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	var order []int
	var orderMu sync.Mutex
	c := newTransportCollector(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var id int
		if _, err := fmt.Sscanf(req.URL.Path, "/req/%d", &id); err != nil {
			t.Errorf("unexpected path %q", req.URL.Path)
		}
		orderMu.Lock()
		order = append(order, id)
		orderMu.Unlock()
		return okResponse(), nil
	}))
	for i := 0; i < 30; i++ {
		mustAddURL(t, q, testURL(i))
	}
	if err := runWithTimeout(t, q, c); err != nil {
		t.Fatal(err)
	}
	if len(order) != 30 {
		t.Fatalf("executed %d requests, want 30", len(order))
	}
	for i, id := range order {
		if id != i {
			t.Fatalf("dispatch order violated at index %d: got %d, want %d; order=%v", i, id, i, order)
		}
	}
}

// TestAddFromCallback ensures both AddURL and AddRequest invoked from a
// response callback wake the same Run and are processed before it exits.
func TestAddFromCallback(t *testing.T) {
	q, err := New(3, nil)
	if err != nil {
		t.Fatal(err)
	}
	var executed atomic.Int32
	c := colly.NewCollector(colly.AllowURLRevisit())
	c.OnResponse(func(resp *colly.Response) {
		id := int(executed.Add(1))
		if id <= 5 {
			if id%2 == 0 {
				mustAddRequest(t, q, testURL(100+id))
			} else {
				mustAddURL(t, q, testURL(200+id))
			}
		}
	})
	c.SetClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return okResponse(), nil
	})})
	mustAddURL(t, q, testURL(0))
	if err := runWithTimeout(t, q, c); err != nil {
		t.Fatal(err)
	}
	// Seed plus five callback-generated requests.
	if got := executed.Load(); got != 6 {
		t.Fatalf("executed %d requests, want 6", got)
	}
	if !q.IsEmpty() {
		t.Fatalf("queue not empty after drain: %d", mustSize(t, q))
	}
}

// TestConcurrentAdders appends from many goroutines while Run is active, with
// all adds racing the empty-queue boundary. Every request must run once.
func TestConcurrentAdders(t *testing.T) {
	q, err := New(8, nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := newGateTransport()
	manager := startGateManager(transport, 1)
	defer manager.shutdown()
	var seenMu sync.Mutex
	seen := make(map[string]struct{})
	c := newTransportCollector(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/hold" {
			return transport.RoundTrip(req)
		}
		seenMu.Lock()
		seen[req.URL.String()] = struct{}{}
		seenMu.Unlock()
		return okResponse(), nil
	}))

	const adderCount, perAdder = 12, 25
	// The held request keeps the run alive until all adders have started,
	// removing the scheduling race between Run startup and the first Add*.
	mustAddURL(t, q, "http://queue.test/hold")
	started := make(chan struct{})
	var adders sync.WaitGroup
	for a := 0; a < adderCount; a++ {
		adders.Add(1)
		go func(a int) {
			defer adders.Done()
			<-started
			for i := 0; i < perAdder; i++ {
				u := fmt.Sprintf("http://queue.test/c/%d/%d", a, i)
				if i%2 == 0 {
					mustAddURL(t, q, u)
				} else {
					mustAddRequest(t, q, u)
				}
			}
		}(a)
	}

	runDone := make(chan struct{})
	go func() {
		if err := q.Run(c); err != nil {
			t.Errorf("Run error: %v", err)
		}
		close(runDone)
	}()
	holdGate := manager.awaitHeld(t)
	close(started)
	adders.Wait()
	close(holdGate)
	select {
	case <-runDone:
	case <-time.After(blockTestTimeout):
		t.Fatalf("Run did not finish after concurrent adds within %s", blockTestTimeout)
	}
	want := adderCount * perAdder
	if len(seen) != want {
		t.Fatalf("executed %d unique requests, want %d; the hold request is tracked separately", len(seen), want)
	}
}

// gateTransport blocks each request until the matching gate is released,
// giving deterministic control over in-flight requests without sleeping.
type gateTransport struct {
	arrivals chan chan struct{}
}

// newGateTransport starts a single manager that receives every request gate.
// A unique receiver removes the hand-off window that exists when separate
// goroutines take turns reading the gate channel. Call stop to shut the
// manager down once the queue run has returned.
func newGateTransport() *gateTransport {
	g := &gateTransport{arrivals: make(chan chan struct{})}
	return g
}

func (g *gateTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	gate := make(chan struct{})
	g.arrivals <- gate
	<-gate
	return okResponse(), nil
}

// gateManager owns the single receive side of arrivals. Until holdCount gates
// have been collected it forwards them on held for the test to release
// explicitly; every later gate (including requests dispatched in the stop
// race) is released immediately so its worker can always finish.
type gateManager struct {
	transport *gateTransport
	held      chan chan struct{}
	stop      chan struct{}
	done      chan struct{}
}

func startGateManager(g *gateTransport, holdCount int) *gateManager {
	m := &gateManager{
		transport: g,
		held:      make(chan chan struct{}, holdCount),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	go func() {
		defer close(m.done)
		for held := 0; ; {
			select {
			case gate := <-g.arrivals:
				if held < holdCount {
					m.held <- gate
					held++
				} else {
					close(gate)
				}
			case <-m.stop:
				return
			}
		}
	}()
	return m
}

func (m *gateManager) awaitHeld(t *testing.T) chan struct{} {
	t.Helper()
	select {
	case gate := <-m.held:
		return gate
	case <-time.After(blockTestTimeout):
		t.Fatal("worker did not start a request in time")
		return nil
	}
}

func (m *gateManager) shutdown() {
	close(m.stop)
	<-m.done
}

// TestStopDuringActiveRequests stops while requests are in flight: in-flight
// callbacks must complete, Run must wait for them, and undispatched requests
// must remain in storage and run exactly once on the next Run. The threads=1
// variant deterministically exercises the prefetched-request requeue path.
func TestStopDuringActiveRequests(t *testing.T) {
	for _, threads := range []int{1, 4} {
		t.Run(fmt.Sprintf("threads=%d", threads), func(t *testing.T) {
			q, err := New(threads, nil)
			if err != nil {
				t.Fatal(err)
			}
			transport := newGateTransport()
			manager := startGateManager(transport, threads)
			defer manager.shutdown()
			var callbacks atomic.Int32
			c := newTransportCollector(t, transport)
			c.OnResponse(func(*colly.Response) { callbacks.Add(1) })

			for i := 0; i < 10; i++ {
				mustAddURL(t, q, testURL(i))
			}
			runReturned := make(chan struct{})
			var runErr error
			go func() {
				runErr = q.Run(c)
				close(runReturned)
			}()

			// Wait until the whole worker pool has picked up requests.
			var pendingGates []chan struct{}
			for i := 0; i < threads; i++ {
				pendingGates = append(pendingGates, manager.awaitHeld(t))
			}
			// The manager now auto-releases every later arrival, including any
			// request dispatched in the stop/dispatch race.
			q.Stop()
			q.Stop() // repeated Stop must be a harmless no-op

			select {
			case <-runReturned:
				t.Fatal("Run returned while dispatched requests were still in flight")
			default:
			}

			for _, gate := range pendingGates {
				close(gate)
			}
			select {
			case <-runReturned:
			case <-time.After(blockTestTimeout):
				t.Fatal("Run did not return after in-flight requests completed")
			}
			if runErr != nil {
				t.Fatalf("Run after Stop returned error: %v", runErr)
			}
			firstRun := int(callbacks.Load())
			if firstRun < threads {
				t.Fatalf("%d callbacks ran, want at least the %d initially dispatched requests", firstRun, threads)
			}
			remaining := mustSize(t, q)
			wantRemaining := 10 - firstRun
			if remaining != wantRemaining {
				t.Fatalf("remaining queue size=%d, want %d (first run executed %d)", remaining, wantRemaining, firstRun)
			}

			// Rerun drains every retained request exactly once.
			var secondMu sync.Mutex
			secondSeen := make(map[string]struct{})
			c2 := newTransportCollector(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
				secondMu.Lock()
				secondSeen[req.URL.String()] = struct{}{}
				secondMu.Unlock()
				return okResponse(), nil
			}))
			if err := runWithTimeout(t, q, c2); err != nil {
				t.Fatalf("rerun error: %v", err)
			}
			if len(secondSeen) != wantRemaining {
				t.Fatalf("rerun executed %d requests, want %d", len(secondSeen), wantRemaining)
			}
			if !q.IsEmpty() {
				t.Fatalf("queue not empty after rerun: %d", mustSize(t, q))
			}
		})
	}
}

// TestStopThenRerunWithNewAdds verifies no stop state or closed channels leak
// into a subsequent Run, and requests added while stopped are preserved.
func TestStopThenRerunWithNewAdds(t *testing.T) {
	q, err := New(2, nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := newGateTransport()
	manager := startGateManager(transport, 1)
	defer manager.shutdown()
	c := newTransportCollector(t, transport)
	var firstRun atomic.Int32
	c.OnResponse(func(*colly.Response) { firstRun.Add(1) })
	mustAddURL(t, q, testURL(0))
	mustAddURL(t, q, testURL(1))
	runReturned := make(chan struct{})
	go func() {
		if err := q.Run(c); err != nil {
			t.Errorf("first Run error: %v", err)
		}
		close(runReturned)
	}()
	gate := manager.awaitHeld(t)
	q.Stop()
	close(gate)
	select {
	case <-runReturned:
	case <-time.After(blockTestTimeout):
		t.Fatal("first Run did not return after Stop")
	}
	executedFirst := int(firstRun.Load())
	if executedFirst < 1 || executedFirst > 2 {
		t.Fatalf("first run executed %d requests, want 1 or 2", executedFirst)
	}
	remainingAfterStop := mustSize(t, q)
	if executedFirst+remainingAfterStop != 2 {
		t.Fatalf("conservation violated: executed=%d remaining=%d, total want 2", executedFirst, remainingAfterStop)
	}

	// Adding while stopped must not block or require a live consumer.
	mustAddURL(t, q, testURL(2))
	mustAddRequest(t, q, testURL(3))
	var secondMu sync.Mutex
	secondSeen := make(map[string]struct{})
	c2 := newTransportCollector(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		secondMu.Lock()
		secondSeen[req.URL.String()] = struct{}{}
		secondMu.Unlock()
		return okResponse(), nil
	}))
	if err := runWithTimeout(t, q, c2); err != nil {
		t.Fatalf("second Run error: %v", err)
	}
	wantSecond := remainingAfterStop + 2
	if len(secondSeen) != wantSecond {
		t.Fatalf("second run executed %d requests, want %d retained+new requests", len(secondSeen), wantSecond)
	}
}

// TestStopIdleAndRepeated is safe before Run, after Run, and many times: it
// must not block and must not poison future runs.
func TestStopIdleAndRepeated(t *testing.T) {
	q, err := New(2, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10; i++ {
			q.Stop()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(blockTestTimeout):
		t.Fatal("repeated Stop blocked while idle")
	}
	mustAddURL(t, q, testURL(0))
	var executed atomic.Int32
	c := newTransportCollector(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		executed.Add(1)
		return okResponse(), nil
	}))
	if err := runWithTimeout(t, q, c); err != nil {
		t.Fatal(err)
	}
	if executed.Load() != 1 {
		t.Fatalf("executed %d requests, want 1", executed.Load())
	}
	q.Stop()
	q.Stop()
}

// TestAddAtRunBoundary repeatedly races Add* against natural drain. Adds may
// be consumed by the ending Run or retained, but they must never block or be
// lost; a final Run drains whatever remains.
func TestAddAtRunBoundary(t *testing.T) {
	const iterations = 40
	var total atomic.Int32
	var executedMu sync.Mutex
	executed := make(map[string]struct{})
	collect := func(req *http.Request) {
		executedMu.Lock()
		executed[req.URL.String()] = struct{}{}
		executedMu.Unlock()
	}
	for iter := 0; iter < iterations; iter++ {
		q, err := New(4, nil)
		if err != nil {
			t.Fatal(err)
		}
		c := newTransportCollector(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
			collect(req)
			return okResponse(), nil
		}))
		mustAddURL(t, q, testURL(iter*2))
		total.Add(1)
		runDone := make(chan struct{})
		go func() {
			if err := q.Run(c); err != nil {
				t.Errorf("Run error on iteration %d: %v", iter, err)
			}
			close(runDone)
		}()
		// Add concurrently with the run's final drain decision.
		u := testURL(iter*2 + 1)
		addDone := make(chan struct{})
		go func() {
			if iter%2 == 0 {
				mustAddURL(t, q, u)
			} else {
				mustAddRequest(t, q, u)
			}
			total.Add(1)
			close(addDone)
		}()
		select {
		case <-addDone:
		case <-time.After(blockTestTimeout):
			t.Fatalf("concurrent add blocked at run boundary on iteration %d", iter)
		}
		select {
		case <-runDone:
		case <-time.After(blockTestTimeout):
			t.Fatalf("Run did not finish on iteration %d", iter)
		}
		if size := mustSize(t, q); size > 0 {
			cRetry := newTransportCollector(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
				collect(req)
				return okResponse(), nil
			}))
			if err := runWithTimeout(t, q, cRetry); err != nil {
				t.Fatalf("draining retained request failed: %v", err)
			}
		}
	}
	executedMu.Lock()
	got := len(executed)
	executedMu.Unlock()
	if got != int(total.Load()) {
		t.Fatalf("executed %d unique requests, want all %d added requests", got, total.Load())
	}
}

func TestInvalidThreads(t *testing.T) {
	if _, err := New(0, nil); !errors.Is(err, ErrInvalidThreads) {
		t.Fatalf("New(0) error=%v, want ErrInvalidThreads", err)
	}
	if _, err := New(-3, &InMemoryQueueStorage{MaxSize: 10}); !errors.Is(err, ErrInvalidThreads) {
		t.Fatalf("New(-3) error=%v, want ErrInvalidThreads", err)
	}
	// A queue built directly (zero value construction path) must return the
	// error from Run rather than deadlocking with no worker.
	q := &Queue{Threads: 0, storage: &InMemoryQueueStorage{MaxSize: 10}}
	c := newTransportCollector(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("request should never be dispatched with zero threads")
		return okResponse(), nil
	}))
	if err := runWithTimeout(t, q, c); !errors.Is(err, ErrInvalidThreads) {
		t.Fatalf("Run with Threads=0 error=%v, want ErrInvalidThreads", err)
	}
}

// TestDuplicateRunPanics documents the unsupported concurrent Run behavior:
// starting a second Run before the first returns must panic rather than
// corrupting scheduler state.
func TestDuplicateRunPanics(t *testing.T) {
	q, err := New(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := newGateTransport()
	manager := startGateManager(transport, 1)
	defer manager.shutdown()
	c := newTransportCollector(t, transport)
	mustAddURL(t, q, testURL(0))
	runReturned := make(chan struct{})
	go func() {
		_ = q.Run(c)
		close(runReturned)
	}()
	gate := manager.awaitHeld(t)
	defer func() {
		close(gate)
		<-runReturned
	}()
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("concurrent Queue.Run did not panic")
		}
	}()
	c2 := newTransportCollector(t, transport)
	q.Run(c2)
}

func TestCapacityLimit(t *testing.T) {
	storage := &InMemoryQueueStorage{MaxSize: 2}
	q, err := New(1, storage)
	if err != nil {
		t.Fatal(err)
	}
	mustAddURL(t, q, testURL(0))
	mustAddRequest(t, q, testURL(1))
	if err := q.AddURL(testURL(2)); !errors.Is(err, colly.ErrQueueFull) {
		t.Fatalf("third AddURL error=%v, want ErrQueueFull", err)
	}
	if size := mustSize(t, q); size != 2 {
		t.Fatalf("size=%d, want 2 after rejected add", size)
	}
	// Existing items remain fully functional.
	var executed atomic.Int32
	c := newTransportCollector(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		executed.Add(1)
		return okResponse(), nil
	}))
	if err := runWithTimeout(t, q, c); err != nil {
		t.Fatal(err)
	}
	if executed.Load() != 2 {
		t.Fatalf("executed %d requests, want 2", executed.Load())
	}
}

// controllableStorage wraps the in-memory storage and can inject failures into
// individual operations to verify Run exits cleanly on storage errors.
type controllableStorage struct {
	*InMemoryQueueStorage
	failSize atomic.Bool
	failGet  atomic.Bool
	failAdd  atomic.Bool
}

var errInjectedStorage = errors.New("injected storage failure")

func (s *controllableStorage) QueueSize() (int, error) {
	if s.failSize.Load() {
		return 0, errInjectedStorage
	}
	return s.InMemoryQueueStorage.QueueSize()
}

func (s *controllableStorage) GetRequest() ([]byte, error) {
	if s.failGet.Load() {
		return nil, errInjectedStorage
	}
	return s.InMemoryQueueStorage.GetRequest()
}

func (s *controllableStorage) AddRequest(r []byte) error {
	if s.failAdd.Load() {
		return errInjectedStorage
	}
	return s.InMemoryQueueStorage.AddRequest(r)
}

func newControllableQueue(t *testing.T, threads int) (*Queue, *controllableStorage) {
	t.Helper()
	s := &controllableStorage{InMemoryQueueStorage: &InMemoryQueueStorage{MaxSize: 1000}}
	q, err := New(threads, s)
	if err != nil {
		t.Fatal(err)
	}
	return q, s
}

func TestStorageErrorCleanShutdown(t *testing.T) {
	t.Run("QueueSizeFails", func(t *testing.T) {
		q, s := newControllableQueue(t, 3)
		mustAddURL(t, q, testURL(0))
		s.failSize.Store(true)
		c := newTransportCollector(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Error("request must not be dispatched when QueueSize fails")
			return okResponse(), nil
		}))
		if err := runWithTimeout(t, q, c); !errors.Is(err, errInjectedStorage) {
			t.Fatalf("Run error=%v, want errInjectedStorage", err)
		}
		assertNoLeftoverWorkers(t, q)
		// Recovering the storage must make the queue reusable.
		s.failSize.Store(false)
		var executed atomic.Int32
		c2 := newTransportCollector(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
			executed.Add(1)
			return okResponse(), nil
		}))
		if err := runWithTimeout(t, q, c2); err != nil {
			t.Fatalf("recovered Run error: %v", err)
		}
		if executed.Load() != 1 {
			t.Fatalf("executed %d requests after recovery, want 1", executed.Load())
		}
	})

	t.Run("GetRequestFails", func(t *testing.T) {
		q, s := newControllableQueue(t, 3)
		mustAddURL(t, q, testURL(0))
		s.failGet.Store(true)
		c := newTransportCollector(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Error("request must not be dispatched when GetRequest fails")
			return okResponse(), nil
		}))
		if err := runWithTimeout(t, q, c); !errors.Is(err, errInjectedStorage) {
			t.Fatalf("Run error=%v, want errInjectedStorage", err)
		}
		assertNoLeftoverWorkers(t, q)
	})

	t.Run("AddFailsAtRuntime", func(t *testing.T) {
		q, s := newControllableQueue(t, 1)
		transport := newGateTransport()
		manager := startGateManager(transport, 1)
		defer manager.shutdown()
		var callbacks atomic.Int32
		c := newTransportCollector(t, transport)
		c.OnResponse(func(*colly.Response) {
			callbacks.Add(1)
			// Break storage from within the callback: once this in-flight
			// request completes, the scheduler's next QueueSize check is fatal.
			s.failSize.Store(true)
		})
		mustAddURL(t, q, testURL(0))
		done := make(chan error, 1)
		go func() { done <- q.Run(c) }()
		gate := manager.awaitHeld(t)
		close(gate)
		select {
		case err := <-done:
			if !errors.Is(err, errInjectedStorage) {
				t.Fatalf("Run error=%v, want errInjectedStorage", err)
			}
		case <-time.After(blockTestTimeout):
			t.Fatal("Run did not shut down after storage error")
		}
		if callbacks.Load() != 1 {
			t.Fatalf("callbacks=%d, want exactly the in-flight request", callbacks.Load())
		}
		s.failSize.Store(false)
		// After recovery the queue is empty and the queue stays reusable.
		if size := mustSize(t, q); size != 0 {
			t.Fatalf("remaining size=%d, want empty queue", size)
		}
		assertNoLeftoverWorkers(t, q)
		var recovered atomic.Int32
		mustAddURL(t, q, testURL(99))
		c2 := newTransportCollector(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
			recovered.Add(1)
			return okResponse(), nil
		}))
		if err := runWithTimeout(t, q, c2); err != nil {
			t.Fatalf("recovered Run error: %v", err)
		}
		if recovered.Load() != 1 {
			t.Fatalf("recovered run executed %d requests, want 1", recovered.Load())
		}
	})
}

// TestInvalidSerializedRequestSkipped preserves the historical behavior:
// malformed entries are dropped without failing the whole Run.
func TestInvalidSerializedRequestSkipped(t *testing.T) {
	storage := &InMemoryQueueStorage{MaxSize: 100}
	q, err := New(2, storage)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.AddRequest([]byte("not a valid serialized request")); err != nil {
		t.Fatal(err)
	}
	mustAddURL(t, q, testURL(7))
	var executed atomic.Int32
	c := newTransportCollector(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
		executed.Add(1)
		return okResponse(), nil
	}))
	if err := runWithTimeout(t, q, c); err != nil {
		t.Fatalf("Run error: %v", err)
	}
	if executed.Load() != 1 {
		t.Fatalf("executed %d requests, want the single valid request", executed.Load())
	}
}

// assertNoLeftoverWorkers confirms every worker goroutine exited with Run.
// It polls rather than sleeps: GC/stable goroutine counts are checked only
// after the count stops decreasing.
func assertNoLeftoverWorkers(t *testing.T, q *Queue) {
	t.Helper()
	q.mut.Lock()
	live := q.wake != nil
	q.mut.Unlock()
	if live {
		t.Fatal("queue still reports a live run after Run returned")
	}
	deadline := time.Now().Add(2 * time.Second)
	previous := -1
	for time.Now().Before(deadline) {
		current := runtime.NumGoroutine()
		if current == previous {
			return
		}
		previous = current
		time.Sleep(5 * time.Millisecond)
	}
}

// TestStopWithHTTPTestServer exercises the full httptest path. In sandboxed
// environments without socket permission the test is skipped rather than
// failing on infrastructure unrelated to the queue.
func TestStopWithHTTPTestServer(t *testing.T) {
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	if server == nil {
		t.Skip("httptest server unavailable")
	}
	t.Cleanup(server.Close)

	q, err := New(2, nil)
	if err != nil {
		t.Fatal(err)
	}
	var callbacks atomic.Int32
	c := colly.NewCollector(colly.AllowURLRevisit())
	c.OnResponse(func(*colly.Response) { callbacks.Add(1) })
	for i := 0; i < 5; i++ {
		mustAddURL(t, q, fmt.Sprintf("%s/%d", server.URL, i))
	}
	done := make(chan error, 1)
	go func() { done <- q.Run(c) }()
	// Stop immediately: either zero or some requests may already have been
	// dispatched, but retained requests must still exist for a rerun.
	q.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stopped Run error: %v", err)
		}
	case <-time.After(blockTestTimeout):
		t.Fatal("stopped Run did not return in time")
	}
	dispatched := callbacks.Load()
	remaining := mustSize(t, q)
	if int(dispatched)+remaining != 5 {
		t.Fatalf("dispatched=%d remaining=%d, total want 5", dispatched, remaining)
	}
	if err := runWithTimeout(t, q, c); err != nil {
		t.Fatalf("rerun error: %v", err)
	}
	if total := callbacks.Load(); total != 5 {
		t.Fatalf("total callbacks=%d, want 5", total)
	}
}
