package client

import (
	"fmt"
	"github.com/sds-framework/client-lib/config"
	"github.com/sds-framework/datatype-lib/data_type/key_value"
	"github.com/sds-framework/datatype-lib/message"
	zmq "github.com/pebbe/zmq4"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// We won't test the requests.
// The requests are tested in the controllers
// Define the suite, and absorb the built-in basic suite
// functionality from test suite - including a T() method which
// returns the current testing orchestra
type TestClientSuite struct {
	suite.Suite

	socket   *Socket
	backend  *zmq.Socket
	funcName string
}

func (test *TestClientSuite) SetupTest() {
	require := test.Require

	socket, err := NewRaw(zmq.ROUTER, "inproc://sample_router")
	require().NoError(err)

	test.socket = socket
}

func (test *TestClientSuite) TearDownTest() {
	require := test.Require

	if test.backend != nil {
		require().NoError(test.backend.Close())
	}

	if test.socket.zmqSocket != nil {
		require().NoError(test.socket.Close())
	}

	time.Sleep(time.Millisecond * 100)
}

// runBackend to imitate the backend as a goroutine.
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
			Parameters: key_value.New().
				Set("reply", fmt.Sprintf("reply to '%s'", msg[2])),
		}).String()
		reply = []string{msg[0], msg[1], content}
	} else if len(msg) >= 2 {
		content := (&message.Reply{
			Status:  message.OK,
			Message: "",
			Parameters: key_value.New().
				Set("reply", fmt.Sprintf("reply to '%s'", msg[1])),
		}).String()
		reply = []string{msg[0], content}
	} else {
		content := (&message.Reply{
			Status:  message.OK,
			Message: "",
			Parameters: key_value.New().
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

// Test_10_New tests creation of the client
func (test *TestClientSuite) Test_10_New() {
	require := test.Require

	serviceUrl := "github.com/sds-framework/service"
	id := "sample_router"
	port := uint64(0)
	socketType := zmq.ROUTER
	client := config.New(serviceUrl, id, port, socketType)

	// Must fail, since config can not generate url
	_, err := New(client)
	require().Error(err)

	// Client is corrected
	client.UrlFunc(config.Url)
	_, err = New(client)
	require().NoError(err)
}

// Test_11_Parameters tests setting socket parameters such as timeout and attempts
func (test *TestClientSuite) Test_11_Parameters() {
	require := test.Require

	timeout := time.Millisecond // less than min
	attempt := uint8(0)

	require().Less(timeout, minTimeout, "set less than minTimeout")
	require().Less(attempt, minAttempt, "set less than minAttempt")

	// Setting a value less than minimal value should set to min
	test.socket.Timeout(timeout).Attempt(attempt)

	// The timeout must be minimal values
	require().EqualValues(minTimeout, test.socket.timeout)
	require().EqualValues(minAttempt, test.socket.attempt)
}

// Test_12_rawSubmit test submitting the message.
func (test *TestClientSuite) Test_12_rawSubmit() {
	require := test.Require

	go test.runBackend("Test_12_rawSubmit", test.socket.url, test.socket.target)
	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 100)

	req := "hello Test_12_rawSubmit"
	timeout, err := test.socket.rawSubmit(req)
	require().False(timeout)
	require().NoError(err)
	// wait for a message transfer a bit before closing
	time.Sleep(time.Microsecond * 100)
}

// Test_13_RawRequest test requesting data.
func (test *TestClientSuite) Test_13_RawRequest() {
	require := test.Require

	go test.runBackend("Test_13_RawRequest", test.socket.url, test.socket.target)
	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 100)

	req := "hello Test_13_RawRequest"
	reply, err := test.socket.RawRequest(req)
	require().NoError(err)
	fmt.Printf("client recevied: %s\n", reply)
}

// Test_14_RawSubmit test submitting the message.
func (test *TestClientSuite) Test_14_RawSubmit() {
	require := test.Require

	go test.runBackend("Test_14_RawSubmit", test.socket.url, test.socket.target)

	req := "hello Test_14_RawSubmit"
	err := test.socket.RawSubmit(req)
	require().NoError(err)

	// wait a bit for the backend clearing up it queue.
	// otherwise TearDown will close the backend before backend sends it's message.
	time.Sleep(time.Microsecond * 100)
}

// Test_15_DealerRawRequest test requesting data in asynchronous way.
func (test *TestClientSuite) Test_15_DealerRawRequest() {
	require := test.Require

	go test.runBackend("Test_15_DealerRawRequest", test.socket.url, test.socket.target)
	time.Sleep(time.Millisecond * 100)

	socket, err := NewRaw(zmq.ROUTER, "inproc://sample_router")
	require().NoError(err)
	test.socket = socket
	test.socket.socketType = zmq.DEALER

	// Set minimal timeout and attempt for fast testing
	test.socket.Timeout(time.Second).Attempt(minAttempt)

	req := "hello Test_15_DealerRawRequest"
	reply, err := test.socket.RawRequest(req)
	require().NoError(err)
	fmt.Printf("client recevied: %s\n", reply)
}

// Test_16_DealerRawSubmit test submitting the message in request way.
func (test *TestClientSuite) Test_16_DealerRawSubmit() {
	require := test.Require

	go test.runBackend("Test_16_DealerRawSubmit", test.socket.url, test.socket.target)
	time.Sleep(time.Millisecond * 100)

	socket, err := NewRaw(zmq.ROUTER, "inproc://sample_router")
	require().NoError(err)
	test.socket = socket
	test.socket.socketType = zmq.DEALER
	// Set minimal timeout and attempt for fast testing
	test.socket.Timeout(time.Second).Attempt(minAttempt)

	req := "hello Test_16_DealerRawSubmit"
	err = test.socket.RawSubmit(req)
	require().NoError(err)

	// Wait a bit before closing so that message transfer to the handler delivered
	time.Sleep(time.Millisecond * 100)
}

// Test_17_RequestToRep test requesting data to the zmq.REP (handlerConfig.SyncReplier) way.
func (test *TestClientSuite) Test_17_RequestToRep() {
	require := test.Require

	url := "inproc://sample_router"
	go test.runBackend("Test_17_RequestToRep", url, zmq.REP)
	time.Sleep(time.Millisecond * 100)

	socket, err := NewRaw(zmq.REP, url)
	require().NoError(err)
	test.socket = socket

	// Set minimal timeout and attempt for fast testing
	test.socket.Timeout(time.Second).Attempt(minAttempt)

	req := &message.Request{
		Command:    "hello",
		Parameters: key_value.New().Set("unit", "Test_17_RequestToRep"),
	}
	reply, err := test.socket.Request(req)
	require().NoError(err)
	fmt.Printf("client recevied: %s\n", reply)

}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestClient(t *testing.T) {
	suite.Run(t, new(TestClientSuite))
}
