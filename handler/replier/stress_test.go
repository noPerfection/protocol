package replier

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// Default ZMQ context allows 1024 sockets; stress tests use one REQ socket per client.
	const maxSockets = 100128
	if err := zmq.SetMaxSockets(maxSockets); err != nil {
		fmt.Fprintf(os.Stderr, "zmq.SetMaxSockets(%d): %v\n", maxSockets, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestStressTest(t *testing.T) {
	cases := []struct {
		name              string
		clientAmount      int
		requestsPerClient int
		optIn             bool
	}{
		{name: "1000_clients_5_requests_each", clientAmount: 1000, requestsPerClient: 5},
		{name: "10000_clients_100_requests_each", clientAmount: 10000, requestsPerClient: 100, optIn: true},
		{name: "100000_clients_5_requests_each", clientAmount: 100000, requestsPerClient: 5, optIn: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.optIn && os.Getenv("REPLIER_STRESS_LARGE") != "1" {
				t.Skip("set REPLIER_STRESS_LARGE=1 to run large stress cases")
			}

			skipIfInsufficientResources(t, tc.clientAmount)
			runStressTest(t, tc.clientAmount, tc.requestsPerClient)
		})
	}
}

// stressResourcesNeeded estimates sockets and file descriptors for all clients plus handler overhead.
func stressResourcesNeeded(clientAmount int) int {
	const overhead = 64 // replier, control manager, test helpers
	return clientAmount + overhead
}

func softFileDescriptorLimit() (uint64, error) {
	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err != nil {
		return 0, err
	}
	return rlim.Cur, nil
}

func skipIfInsufficientResources(t *testing.T, clientAmount int) {
	t.Helper()

	if os.Getenv("REPLIER_STRESS_IGNORE_FDLIMIT") == "1" {
		return
	}

	required := stressResourcesNeeded(clientAmount)

	maxSockets, err := zmq.GetMaxSockets()
	if err != nil {
		t.Fatalf("zmq.GetMaxSockets: %v", err)
	}
	if maxSockets < required {
		if err := zmq.SetMaxSockets(required); err != nil {
			t.Skipf(
				"ZMQ context allows %d sockets but %d are needed for %d clients (default is 1024); SetMaxSockets failed: %v",
				maxSockets,
				required,
				clientAmount,
				err,
			)
		}
	}

	soft, err := softFileDescriptorLimit()
	if err != nil {
		t.Fatalf("syscall.Getrlimit(RLIMIT_NOFILE): %v", err)
	}
	if soft < uint64(required) {
		t.Skipf(
			"need at least %d open files for %d client sockets (soft limit %d); raise with: ulimit -n %d",
			required,
			clientAmount,
			soft,
			required,
		)
	}
}

type stressResult struct {
	clientID string
	requests int
	err      error
}

func runStressTest(t *testing.T, clientAmount int, requestsPerClient int) {
	handleTime, err := stressHandleTime()
	require.NoError(t, err)

	replier := New()
	err = replier.Route("db_request", func(request message.RequestInterface) message.ReplyInterface {
		time.Sleep(handleTime)
		clientID, err := request.RouteParameters().StringValue("client_id")
		if err != nil {
			return request.Fail("missing client_id parameter")
		}
		return request.Ok(datatype.New().Set("client_id", clientID))
	})
	require.NoError(t, err)

	logger, err := log.New(fmt.Sprintf("replier-stress-%d-%d", clientAmount, requestsPerClient), false)
	require.NoError(t, err)

	testID := strings.ReplaceAll(t.Name(), "/", "_")
	handlerConfig := message.NewEndpoint(testID, 0)
	require.NoError(t, replier.SetLogger(logger))

	replier.SetEndpoint(handlerConfig)
	require.NoError(t, replier.SetLogger(logger))
	require.NoError(t, replier.Start())

	managerClient, err := zmq.NewSocket(zmq.REQ)
	require.NoError(t, err)

	managerConfig := control.NewInternalControlEndpoint(handlerConfig)
	require.NoError(t, managerClient.Connect(managerConfig.ClientUrl()))

	t.Cleanup(func() {
		closeReplierStress(managerClient)
	})

	connectStartedAt := time.Now()
	clients := make([]*zmq.Socket, clientAmount)
	for i := range clients {
		socket, err := zmq.NewSocket(zmq.REQ)
		if err != nil {
			if isTooManyOpen(err) {
				t.Skipf(
					"opened %d/%d client sockets before limit (%v); raise ulimit -n or ZMQ max sockets",
					i,
					clientAmount,
					err,
				)
			}
			require.NoError(t, err)
		}
		socketTimeout := stressReplyTimeout(clientAmount, requestsPerClient, handleTime)
		require.NoError(t, socket.SetRcvtimeo(socketTimeout))
		require.NoError(t, socket.SetSndtimeo(socketTimeout))
		require.NoError(t, socket.Connect(handlerConfig.ClientUrl()))
		clients[i] = socket
	}
	connectDuration := time.Since(connectStartedAt)
	defer func() {
		for _, socket := range clients {
			if socket != nil {
				_ = socket.Close()
			}
		}
	}()

	requestStartedAt := time.Now()
	results := make(chan stressResult, clientAmount)
	jobs := make(chan int, clientAmount)
	for i := range clients {
		jobs <- i
	}
	close(jobs)

	workers := stressWorkerCount(clientAmount)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				clientID := fmt.Sprintf("client_%d", i)
				results <- stressClientRequests(clients[i], clientID, requestsPerClient)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	waitTimeout := stressReplyTimeout(clientAmount, requestsPerClient, handleTime)
	totalRequests := 0
	var maxReplyDuration time.Duration
	for i := 0; i < clientAmount; i++ {
		select {
		case result, ok := <-results:
			if !ok {
				t.Fatalf("results channel closed after %d/%d clients", i, clientAmount)
			}
			require.NoError(t, result.err, result.clientID)
			totalRequests += result.requests
			elapsed := time.Since(requestStartedAt)
			if elapsed > maxReplyDuration {
				maxReplyDuration = elapsed
			}
		case <-time.After(waitTimeout):
			t.Fatalf("timed out waiting for stress test replies after %d/%d clients", i, clientAmount)
		}
	}
	requestDuration := time.Since(requestStartedAt)
	requestsPerSecond := float64(totalRequests) / requestDuration.Seconds()

	t.Logf(
		"replier stress: clients=%d requests_per_client=%d handle_time=%s requests=%d connect=%s total_request_time=%s max_reply_time=%s throughput=%.2f req/s",
		clientAmount,
		requestsPerClient,
		handleTime,
		totalRequests,
		connectDuration.Round(time.Millisecond),
		requestDuration.Round(time.Millisecond),
		maxReplyDuration.Round(time.Millisecond),
		requestsPerSecond,
	)

	require.Less(t, requestDuration, stressMaxDuration(clientAmount, requestsPerClient, handleTime))
}

func stressHandleTime() (time.Duration, error) {
	value := os.Getenv("REPLIER_HANDLE_TIME")
	if value == "" {
		return 50 * time.Millisecond, nil
	}

	milliseconds, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("REPLIER_HANDLE_TIME must be milliseconds: %w", err)
	}
	if milliseconds < 1 || milliseconds > 2000 {
		return 0, fmt.Errorf("REPLIER_HANDLE_TIME must be between 1 and 2000 milliseconds")
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}

// stressWorkerCount limits concurrent client goroutines blocked in ZMQ RecvMessage (CGO threads).
func stressWorkerCount(clientAmount int) int {
	if clientAmount <= 1000 {
		return clientAmount
	}
	return 512
}

func stressReplyTimeout(clientAmount, requestsPerClient int, handleTime time.Duration) time.Duration {
	perRequest := handleTime + 150*time.Millisecond
	workers := stressWorkerCount(clientAmount)
	total := int64(clientAmount) * int64(requestsPerClient)
	estimate := time.Duration(total/int64(workers)) * perRequest
	const minTimeout = 30 * time.Second
	if estimate < minTimeout {
		return minTimeout
	}
	return estimate * 3
}

func stressMaxDuration(clientAmount, requestsPerClient int, handleTime time.Duration) time.Duration {
	perRequest := handleTime + 100*time.Millisecond
	workers := stressWorkerCount(clientAmount)
	total := int64(clientAmount) * int64(requestsPerClient)
	estimate := time.Duration(total/int64(workers)) * perRequest
	if clientAmount <= 1000 {
		const floor = 10 * time.Second
		if estimate*2 < floor {
			return floor
		}
		return estimate * 2
	}
	const floor = 10 * time.Minute
	if estimate*4 < floor {
		return floor
	}
	return estimate * 4
}

func stressClientRequests(socket *zmq.Socket, clientID string, requestsPerClient int) stressResult {
	for i := 0; i < requestsPerClient; i++ {
		if err := stressClientRequest(socket, clientID); err != nil {
			return stressResult{clientID: clientID, requests: i, err: err}
		}
	}
	return stressResult{clientID: clientID, requests: requestsPerClient}
}

func stressClientRequest(socket *zmq.Socket, clientID string) error {
	req := message.Request{
		Command:    "db_request",
		Parameters: datatype.New().Set("client_id", clientID),
	}
	packger := &message.MessagePacker{}
	envelope, err := packger.SerializeRequest(&req)
	if err != nil {
		return fmt.Errorf("packger.SerializeRequest: %w", err)
	}

	if _, err := socket.SendMessage(envelope); err != nil {
		return fmt.Errorf("socket.SendMessage: %w", err)
	}

	raw, err := socket.RecvMessage(0)
	if err != nil {
		return fmt.Errorf("socket.RecvMessage: %w", err)
	}

	reply, err := packger.DeserializeReply(raw)
	if err != nil {
		return fmt.Errorf("packger.DeserializeReply: %w", err)
	}
	if !reply.IsOK() {
		return fmt.Errorf("reply failed: %s", reply.ErrorMessage())
	}

	replyClientID, err := reply.ReplyParameters().StringValue("client_id")
	if err != nil {
		return fmt.Errorf("reply client_id: %w", err)
	}
	if replyClientID != clientID {
		return fmt.Errorf("reply client_id = %s", replyClientID)
	}

	return nil
}

func isTooManyOpen(err error) bool {
	return err != nil && strings.Contains(err.Error(), "too many open files")
}

func closeReplierStress(managerClient *zmq.Socket) {
	if managerClient == nil {
		return
	}

	controlReq := message.Request{Command: control.HandlerClose, Parameters: datatype.New()}
	packger := &message.MessagePacker{}
	envelope, err := packger.SerializeRequest(&controlReq)
	if err == nil {
		_, _ = managerClient.SendMessage(envelope)
		_, _ = managerClient.RecvMessage(0)
	}
	_ = managerClient.Close()
}
