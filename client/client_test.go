package client

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
)

type TestClientSuite struct {
	suite.Suite

	socket   *Socket
	backend  *zmq.Socket
	funcName string
}

const minAttempt = uint8(1)

type rawReplyResult struct {
	raw []string
	err error
}

func (test *TestClientSuite) SetupTest() {
	require := test.Require

	socket, err := New("sample_router", 0, ReplierType)
	require().NoError(err)

	test.socket = socket
}

func (test *TestClientSuite) TearDownTest() {
	require := test.Require

	if test.backend != nil {
		require().NoError(test.backend.Close())
	}

	if test.socket != nil {
		require().NoError(test.socket.Close())
	}

	time.Sleep(time.Millisecond * 100)
}

func (test *TestClientSuite) runBackend(funcName string, url string, zmqType zmq.Type) {
	require := test.Require
	test.funcName = funcName

	var err error
	test.backend, err = zmq.NewSocket(zmqType)
	require().NoError(err)

	err = test.backend.Bind(url)
	require().NoError(err)

	msg, err := test.backend.RecvMessage(0)
	if err == zmq.ErrorSocketClosed {
		return
	}
	require().NoError(err)

	var reply []string
	if len(msg) >= 3 {
		content := (&message.Reply{
			Status:  message.OK,
			Message: "",
			Parameters: datatype.New().
				Set("reply", fmt.Sprintf("reply to '%s'", msg[2])),
		}).String()
		reply = []string{msg[0], msg[1], content}
	} else if len(msg) >= 2 {
		content := (&message.Reply{
			Status:  message.OK,
			Message: "",
			Parameters: datatype.New().
				Set("reply", fmt.Sprintf("reply to '%s'", msg[1])),
		}).String()
		reply = []string{msg[0], content}
	} else {
		content := (&message.Reply{
			Status:  message.OK,
			Message: "",
			Parameters: datatype.New().
				Set("reply", fmt.Sprintf("reply to '%s'", msg[0])),
		}).String()
		reply = []string{content}
	}

	_, err = test.backend.SendMessageDontwait(reply)
	require().NoError(err)

	err = test.backend.Close()
	require().NoError(err)

	test.backend = nil
}

func (test *TestClientSuite) runRouterBackend(url string, expectedMessages int) <-chan error {
	require := test.Require

	var err error
	test.backend, err = zmq.NewSocket(zmq.ROUTER)
	require().NoError(err)

	err = test.backend.SetRcvtimeo(time.Second * 10)
	require().NoError(err)

	err = test.backend.Bind(url)
	require().NoError(err)

	done := make(chan error, 1)
	go func() {
		for i := 0; i < expectedMessages; i++ {
			msg, err := test.backend.RecvMessage(0)
			if err != nil {
				done <- fmt.Errorf("backend.RecvMessage: %w", err)
				return
			}

			conId, reqMsg, _ := message.EnvelopeToMessage(msg)
			reply := (&message.Reply{
				Status:  message.OK,
				Message: "",
				Parameters: datatype.New().
					Set("reply", fmt.Sprintf("reply to '%s'", reqMsg)),
			}).String()

			_, err = test.backend.SendMessage(message.MessageToEnvelope(conId, reply))
			if err != nil {
				done <- fmt.Errorf("backend.SendMessage: %w", err)
				return
			}
		}

		done <- nil
	}()

	return done
}

func (test *TestClientSuite) Test_10_New() {
	require := test.Require

	id := "sample_router"
	port := uint64(0)
	targetType := ReplierType

	_, err := New(id, port, targetType)
	require().NoError(err)
}

func (test *TestClientSuite) Test_11_Parameters() {
	require := test.Require

	timeout := time.Millisecond
	attempt := uint8(0)

	require().Less(timeout, minTimeout, "set less than minTimeout")

	test.socket.Timeout(timeout).Attempt(attempt)

	require().EqualValues(minTimeout, test.socket.timeout)
	require().EqualValues(attempt, test.socket.attempt)
}

func (test *TestClientSuite) Test_12_SendByTimeout() {
	require := test.Require

	done := make(chan struct{})
	go func() {
		defer close(done)
		test.runBackend("Test_12_SendByTimeout", test.socket.endpoint.ClientUrl(), zmq.ROUTER)
	}()
	time.Sleep(time.Millisecond * 100)

	req := (&message.Request{
		Command:    "hello",
		Parameters: datatype.New().Set("unit", "Test_12_SendByTimeout"),
	}).String()
	err := test.socket.attemptSending(message.MessageToEnvelope("", req))
	require().NoError(err)
	<-done
}

func (test *TestClientSuite) Test_13_Request() {
	require := test.Require

	done := make(chan struct{})
	go func() {
		defer close(done)
		test.runBackend("Test_13_Request", test.socket.endpoint.ClientUrl(), zmq.ROUTER)
	}()
	time.Sleep(time.Millisecond * 100)

	req := &message.Request{
		Command:    "hello",
		Parameters: datatype.New().Set("unit", "Test_13_Request"),
	}
	reply, err := test.socket.Request(req)
	require().NoError(err)
	fmt.Printf("client recevied: %s\n", reply)
	<-done
}

func (test *TestClientSuite) Test_14_Send() {
	require := test.Require

	done := make(chan struct{})
	go func() {
		defer close(done)
		test.runBackend("Test_14_Send", test.socket.endpoint.ClientUrl(), zmq.ROUTER)
	}()
	time.Sleep(time.Millisecond * 100)

	req := &message.Request{
		Command:    "hello",
		Parameters: datatype.New().Set("unit", "Test_14_Send"),
	}
	err := test.socket.Send(req)
	require().NoError(err)
	<-done
}

func (test *TestClientSuite) Test_15_DealerRequest() {
	require := test.Require

	done := make(chan struct{})
	go func() {
		defer close(done)
		test.runBackend("Test_15_DealerRequest", test.socket.endpoint.ClientUrl(), zmq.ROUTER)
	}()
	time.Sleep(time.Millisecond * 100)

	test.socket.Timeout(time.Second).Attempt(minAttempt)

	req := &message.Request{
		Command:    "hello",
		Parameters: datatype.New().Set("unit", "Test_15_DealerRequest"),
	}
	reply, err := test.socket.Request(req)
	require().NoError(err)
	fmt.Printf("client recevied: %s\n", reply)
	<-done
}

func (test *TestClientSuite) Test_16_DealerSend() {
	require := test.Require

	done := make(chan struct{})
	go func() {
		defer close(done)
		test.runBackend("Test_16_DealerSend", test.socket.endpoint.ClientUrl(), zmq.ROUTER)
	}()
	time.Sleep(time.Millisecond * 100)

	test.socket.Timeout(time.Second).Attempt(minAttempt)

	req := &message.Request{
		Command:    "hello",
		Parameters: datatype.New().Set("unit", "Test_16_DealerSend"),
	}
	err := test.socket.Send(req)
	require().NoError(err)
	<-done
}

func (test *TestClientSuite) Test_17_RequestToRep() {
	require := test.Require

	url := "inproc://sample_router"
	done := make(chan struct{})
	go func() {
		defer close(done)
		test.runBackend("Test_17_RequestToRep", url, zmq.REP)
	}()
	time.Sleep(time.Millisecond * 100)

	socket, err := New("sample_router", 0, SyncReplierType)
	require().NoError(err)
	test.socket = socket

	test.socket.Timeout(time.Second).Attempt(minAttempt)

	req := &message.Request{
		Command:    "hello",
		Parameters: datatype.New().Set("unit", "Test_17_RequestToRep"),
	}
	reply, err := test.socket.Request(req)
	require().NoError(err)
	fmt.Printf("client recevied: %s\n", reply)
	<-done
}

func (test *TestClientSuite) Test_18_ThreadSafety() {
	require := test.Require

	const (
		workers = 5
		loops   = 5
	)

	test.socket.Timeout(time.Second * 2).Attempt(minAttempt)
	backendDone := test.runRouterBackend(test.socket.endpoint.ClientUrl(), workers*loops)

	packer := &message.MessagePacker{}
	makeRawRequest := func(worker string, index int) string {
		req := &message.Request{
			Command: fmt.Sprintf("thread-safe-%s-%d", worker, index),
			Parameters: datatype.New().
				Set("worker", worker).
				Set("index", index),
		}
		return req.String()
	}
	requestAsync := func(socket *Socket, raw string) <-chan rawReplyResult {
		result := make(chan rawReplyResult, 1)
		go func() {
			reply, err := socket.dispatcher.request(message.MessageToEnvelope("", raw))
			result <- rawReplyResult{raw: reply, err: err}
		}()
		return result
	}
	queueRequest := func(raw string) (*Transmit, error) {
		transmit := &Transmit{
			replyMsg:   make(chan []string),
			delayedErr: make(chan error),
			envelope:   message.MessageToEnvelope("", raw),
		}
		if err := test.socket.dispatcher.enqueueTransmit(transmit); err != nil {
			return nil, err
		}
		return transmit, nil
	}
	receiveQueued := func(transmit *Transmit) rawReplyResult {
		if err := <-transmit.delayedErr; err != nil {
			return rawReplyResult{err: err}
		}
		return rawReplyResult{raw: <-transmit.replyMsg}
	}
	checkReply := func(raw string, result rawReplyResult) error {
		if result.err != nil {
			return result.err
		}

		reply, _, err := packer.DeserializeReply(result.raw)
		if err != nil {
			return fmt.Errorf("packer.DeserializeReply: %w", err)
		}

		replyValue, err := reply.ReplyParameters().StringValue("reply")
		if err != nil {
			return fmt.Errorf("reply.Parameters.StringValue('reply'): %w", err)
		}
		expected := fmt.Sprintf("reply to '%s'", raw)
		if replyValue != expected {
			return fmt.Errorf("reply mismatch: expected %q, got %q", expected, replyValue)
		}

		return nil
	}

	errCh := make(chan error, workers*loops)
	var wg sync.WaitGroup

	sequentialWorker := func(worker string) {
		defer wg.Done()
		for i := 0; i < loops; i++ {
			raw := makeRawRequest(worker, i)
			result := requestAsync(test.socket, raw)
			time.Sleep(time.Millisecond * 100)
			if err := checkReply(raw, <-result); err != nil {
				errCh <- fmt.Errorf("%s loop %d: %w", worker, i, err)
				return
			}
		}
	}

	batchWorker := func(worker string) {
		defer wg.Done()

		requests := make([]struct {
			raw      string
			transmit *Transmit
		}, 0, loops)

		for i := 0; i < loops; i++ {
			raw := makeRawRequest(worker, i)
			transmit, err := queueRequest(raw)
			if err != nil {
				errCh <- fmt.Errorf("%s queue %d: %w", worker, i, err)
				return
			}

			requests = append(requests, struct {
				raw      string
				transmit *Transmit
			}{
				raw:      raw,
				transmit: transmit,
			})
			time.Sleep(time.Millisecond * 100)
		}

		for i, request := range requests {
			if err := checkReply(request.raw, receiveQueued(request.transmit)); err != nil {
				errCh <- fmt.Errorf("%s receive %d: %w", worker, i, err)
				return
			}
		}
	}

	receiveWithClient := func(socket *Socket, raw string) <-chan rawReplyResult {
		return requestAsync(socket, raw)
	}
	nestedReceiverWorker := func(worker string) {
		defer wg.Done()
		for i := 0; i < loops; i++ {
			raw := makeRawRequest(worker, i)
			result := receiveWithClient(test.socket, raw)
			if err := checkReply(raw, <-result); err != nil {
				errCh <- fmt.Errorf("%s nested receive %d: %w", worker, i, err)
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}

	wg.Add(workers)
	go sequentialWorker("sequential-a")
	go batchWorker("batch")
	go nestedReceiverWorker("nested-a")
	go sequentialWorker("sequential-b")
	go nestedReceiverWorker("nested-b")

	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
	case <-time.After(time.Second * 15):
		require().FailNow("thread safety test timed out")
	}

	close(errCh)
	for err := range errCh {
		require().NoError(err)
	}

	require().NoError(<-backendDone)
}

func TestClient(t *testing.T) {
	suite.Run(t, new(TestClientSuite))
}
