//go:build stress

package protocoltest

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	cpair "github.com/noPerfection/protocol/client/pair"
	cpublisher "github.com/noPerfection/protocol/client/publisher"
	creplier "github.com/noPerfection/protocol/client/replier"
	csyncreplier "github.com/noPerfection/protocol/client/sync_replier"
	cworker "github.com/noPerfection/protocol/client/worker"
	"github.com/noPerfection/protocol/handler/config"
	hpair "github.com/noPerfection/protocol/handler/pair"
	hpublisher "github.com/noPerfection/protocol/handler/publisher"
	hreplier "github.com/noPerfection/protocol/handler/replier"
	hsyncreplier "github.com/noPerfection/protocol/handler/sync_replier"
	hworker "github.com/noPerfection/protocol/handler/worker"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

const (
	stressPressureMessages = 25
	stressReceiveMessages  = 40
	stressTimeout          = 100 * time.Millisecond
)

type stressResult struct {
	seq   uint64
	reply message.ReplyInterface
	err   error
}

func TestStressSends(t *testing.T) {
	t.Run("worker concurrent send pressure", stressWorkerSends)
	t.Run("replier concurrent send pressure", stressReplierSends)
	t.Run("pair concurrent send pressure", stressPairSends)
}

func TestStressRequests(t *testing.T) {
	t.Run("sync replier concurrent request pressure", stressSyncReplierRequests)
}

func TestStressReceive(t *testing.T) {
	t.Run("replier receives many replies", stressReplierReceive)
	t.Run("pair receives many replies", stressPairReceive)
	t.Run("publisher receives many broadcasts", stressPublisherReceive)
}

func stressWorkerSends(t *testing.T) {
	req := require.New(t)
	id := testID(t, "stress-worker-send")
	handled := make(chan uint64, stressPressureMessages)
	handler := hworker.New()
	handler.SetConfig(config.New(config.WorkerType, id, "test", 0))
	req.NoError(handler.Route("stress", func(request message.RequestInterface) message.ReplyInterface {
		seq, err := request.RouteParameters().Uint64Value("seq")
		if err == nil {
			handled <- seq
		}
		return request.Ok(datatype.New())
	}))
	req.NoError(handler.Start())

	control := newWorkerControl(t, id)
	defer closeControlBestEffort(t, control)

	client, err := cworker.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(stressTimeout)
	client.Attempt(1)

	results := runConcurrentStress(stressPressureMessages, func(seq uint64) stressResult {
		return stressResult{seq: seq, err: client.Send(newSeqRequest(seq))}
	})
	successes := assertPressureResults(t, "worker send", results)
	assertReceivedSeqSet(t, handled, successes)
}

func stressReplierSends(t *testing.T) {
	req := require.New(t)
	id := testID(t, "stress-replier-send")
	handler := hreplier.New()
	handler.SetConfig(config.New(config.ReplierType, id, "test", 0))
	req.NoError(handler.Route("stress", seqReplyRoute))
	req.NoError(handler.Start())

	control := newReplierControl(t, id)
	defer closeControlBestEffort(t, control)

	client, err := creplier.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(stressTimeout)
	client.Attempt(1)
	replies := client.Receive()

	results := runConcurrentStress(stressPressureMessages, func(seq uint64) stressResult {
		return stressResult{seq: seq, err: client.Send(newSeqRequest(seq))}
	})
	successes := assertPressureResults(t, "replier send", results)
	assertReplySeqSet(t, replies, successes)
}

func stressPairSends(t *testing.T) {
	req := require.New(t)
	id := testID(t, "stress-pair-send")
	handler := hpair.New()
	handler.SetConfig(config.New(config.PairType, id, "test", 0))
	req.NoError(handler.Route("stress", seqReplyRoute))
	req.NoError(handler.Start())

	control := newPairControl(t, id)
	defer closeControlBestEffort(t, control)

	client, err := cpair.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(stressTimeout)
	client.Attempt(1)
	replies := client.Receive()
	time.Sleep(50 * time.Millisecond)

	results := runConcurrentStress(stressPressureMessages, func(seq uint64) stressResult {
		return stressResult{seq: seq, err: client.Send(newSeqRequest(seq))}
	})
	successes := assertPressureResults(t, "pair send", results)
	assertReplySeqSet(t, replies, successes)
}

func stressSyncReplierRequests(t *testing.T) {
	req := require.New(t)
	id := testID(t, "stress-sync-request")
	handler := hsyncreplier.New()
	handler.SetConfig(config.New(config.SyncReplierType, id, "test", 0))
	req.NoError(handler.Route("stress", seqReplyRoute))
	req.NoError(handler.Start())

	control := newSyncControl(t, id)
	defer closeControlBestEffort(t, control)

	client, err := csyncreplier.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(stressTimeout)
	client.Attempt(1)

	results := runConcurrentStress(stressPressureMessages, func(seq uint64) stressResult {
		reply, err := client.Request(newSeqRequest(seq))
		return stressResult{seq: seq, reply: reply, err: err}
	})
	successes := assertPressureResults(t, "sync request", results)
	assertDirectReplySeqSet(t, results, successes)

	reply, err := client.Request(newSeqRequest(999))
	req.NoError(err)
	assertSeqReply(t, reply, 999)
}

func stressReplierReceive(t *testing.T) {
	req := require.New(t)
	id := testID(t, "stress-replier-receive")
	handler := hreplier.New()
	handler.SetConfig(config.New(config.ReplierType, id, "test", 0))
	req.NoError(handler.Route("stress", seqReplyRoute))
	req.NoError(handler.Start())

	control := newReplierControl(t, id)
	defer closeControlBestEffort(t, control)

	client, err := creplier.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(stressTimeout)
	client.Attempt(1)
	replies := client.Receive()

	sendSequentially(t, stressReceiveMessages, func(seq uint64) error {
		return client.Send(newSeqRequest(seq))
	})
	assertReplySeqs(t, replies, stressReceiveMessages)
}

func stressPairReceive(t *testing.T) {
	req := require.New(t)
	id := testID(t, "stress-pair-receive")
	handler := hpair.New()
	handler.SetConfig(config.New(config.PairType, id, "test", 0))
	req.NoError(handler.Route("stress", seqReplyRoute))
	req.NoError(handler.Start())

	control := newPairControl(t, id)
	defer closeControlBestEffort(t, control)

	client, err := cpair.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(stressTimeout)
	client.Attempt(1)
	replies := client.Receive()
	time.Sleep(50 * time.Millisecond)

	sendSequentially(t, stressReceiveMessages, func(seq uint64) error {
		return client.Send(newSeqRequest(seq))
	})
	assertReplySeqs(t, replies, stressReceiveMessages)
}

func stressPublisherReceive(t *testing.T) {
	req := require.New(t)
	id := testID(t, "stress-publisher-receive")
	handler := hpublisher.New()
	handler.SetConfig(config.New(config.PublisherType, id, "test", 0))
	req.NoError(handler.Start())

	control := newPublisherControl(t, id)
	defer closeControlBestEffort(t, control)

	client, err := cpublisher.NewClient(id, 0)
	req.NoError(err)
	defer func() { req.NoError(client.Close()) }()
	client.Timeout(stressTimeout)
	client.Attempt(1)
	replies := client.Receive()
	time.Sleep(50 * time.Millisecond)

	sendSequentially(t, stressReceiveMessages, func(seq uint64) error {
		return control.Broadcast(*newSeqRequest(seq))
	})
	assertReplySeqs(t, replies, stressReceiveMessages)
}

func runConcurrentStress(total int, op func(uint64) stressResult) []stressResult {
	start := make(chan struct{})
	results := make(chan stressResult, total)
	var wg sync.WaitGroup
	wg.Add(total)
	for i := 0; i < total; i++ {
		seq := uint64(i)
		go func() {
			defer wg.Done()
			<-start
			results <- op(seq)
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	collected := make([]stressResult, 0, total)
	for result := range results {
		collected = append(collected, result)
	}
	return collected
}

func assertPressureResults(t *testing.T, name string, results []stressResult) map[uint64]bool {
	t.Helper()
	successes := make(map[uint64]bool)
	limited := 0
	for _, result := range results {
		if result.err == nil {
			successes[result.seq] = true
			continue
		}
		if strings.Contains(result.err.Error(), "queue is full") ||
			strings.Contains(result.err.Error(), "send_timeout") {
			limited++
			continue
		}
		require.NoError(t, result.err)
	}
	t.Logf("%s pressure: %d succeeded, %d hit queue/send limit", name, len(successes), limited)
	require.NotEmpty(t, successes, "%s pressure accepted no messages", name)
	require.Equal(t, len(results), len(successes)+limited)
	return successes
}

func sendSequentially(t *testing.T, total int, send func(uint64) error) {
	t.Helper()
	for seq := uint64(0); seq < uint64(total); seq++ {
		done := make(chan error, 1)
		go func() {
			done <- send(seq)
		}()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatalf("stress send seq %d did not return", seq)
		}
	}
}

func seqReplyRoute(request message.RequestInterface) message.ReplyInterface {
	seq, err := request.RouteParameters().Uint64Value("seq")
	if err != nil {
		return request.Fail(fmt.Sprintf("missing seq: %v", err))
	}
	return request.Ok(datatype.New().Set("seq", seq))
}

func newSeqRequest(seq uint64) *message.Request {
	return &message.Request{
		Command:    "stress",
		Parameters: datatype.New().Set("seq", seq),
	}
}

func assertReplySeqs(t *testing.T, replies <-chan message.ReplyInterface, total int) {
	t.Helper()
	expected := make(map[uint64]bool, total)
	for seq := uint64(0); seq < uint64(total); seq++ {
		expected[seq] = true
	}
	assertReplySeqSet(t, replies, expected)
}

func assertReplySeqSet(t *testing.T, replies <-chan message.ReplyInterface, expected map[uint64]bool) {
	t.Helper()
	seen := make(map[uint64]bool, len(expected))
	deadline := time.After(5 * time.Second)
	for len(seen) < len(expected) {
		select {
		case reply, ok := <-replies:
			require.True(t, ok, "reply channel closed after %d/%d replies", len(seen), len(expected))
			seq := assertSeqReplyInSet(t, reply, expected)
			require.False(t, seen[seq], "duplicate seq %d", seq)
			seen[seq] = true
		case <-deadline:
			t.Fatalf("timed out after receiving %d/%d replies", len(seen), len(expected))
		}
	}
}

func assertDirectReplySeqSet(t *testing.T, results []stressResult, expected map[uint64]bool) {
	t.Helper()
	seen := make(map[uint64]bool, len(expected))
	for _, result := range results {
		if result.err != nil {
			continue
		}
		seq := assertSeqReplyInSet(t, result.reply, expected)
		require.False(t, seen[seq], "duplicate seq %d", seq)
		seen[seq] = true
	}
	require.Len(t, seen, len(expected))
}

func assertSeqReply(t *testing.T, reply message.ReplyInterface, seq uint64) {
	t.Helper()
	assertSeqReplyInSet(t, reply, map[uint64]bool{seq: true})
}

func assertSeqReplyInSet(t *testing.T, reply message.ReplyInterface, expected map[uint64]bool) uint64 {
	t.Helper()
	require.NotNil(t, reply)
	require.True(t, reply.IsOK(), "reply failed: %s", reply.ErrorMessage())
	seq, err := reply.ReplyParameters().Uint64Value("seq")
	require.NoError(t, err)
	require.True(t, expected[seq], "unexpected seq %d", seq)
	return seq
}

func assertReceivedSeqSet(t *testing.T, received <-chan uint64, expected map[uint64]bool) {
	t.Helper()
	seen := make(map[uint64]bool, len(expected))
	deadline := time.After(5 * time.Second)
	for len(seen) < len(expected) {
		select {
		case seq := <-received:
			require.True(t, expected[seq], "unexpected seq %d", seq)
			require.False(t, seen[seq], "duplicate seq %d", seq)
			seen[seq] = true
		case <-deadline:
			t.Fatalf("timed out after receiving %d/%d handled messages", len(seen), len(expected))
		}
	}
}

func closeControlBestEffort(t *testing.T, control handlerControl) {
	t.Helper()
	if err := control.HandlerClose(); err != nil {
		t.Logf("stress cleanup HandlerClose failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
}
