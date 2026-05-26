package client

import (
	"fmt"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/client/config"
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

func (test *TestClientSuite) Test_10_New() {
	require := test.Require

	serviceUrl := "github.com/sds-framework/service"
	id := "sample_router"
	port := uint64(0)
	socketType := zmq.ROUTER
	client := config.New(serviceUrl, id, port, socketType)

	_, err := New(client)
	require().Error(err)

	client.UrlFunc(config.Url)
	_, err = New(client)
	require().NoError(err)
}

func (test *TestClientSuite) Test_11_Parameters() {
	require := test.Require

	timeout := time.Millisecond
	attempt := uint8(0)

	require().Less(timeout, minTimeout, "set less than minTimeout")
	require().Less(attempt, minAttempt, "set less than minAttempt")

	test.socket.Timeout(timeout).Attempt(attempt)

	require().EqualValues(minTimeout, test.socket.timeout)
	require().EqualValues(minAttempt, test.socket.attempt)
}

func (test *TestClientSuite) Test_12_rawSubmit() {
	require := test.Require

	go test.runBackend("Test_12_rawSubmit", test.socket.url, test.socket.target)
	time.Sleep(time.Millisecond * 100)

	req := "hello Test_12_rawSubmit"
	err := test.socket.rawSubmitByTimeout(req)
	require().NoError(err)
}

func (test *TestClientSuite) Test_13_RawRequest() {
	require := test.Require

	go test.runBackend("Test_13_RawRequest", test.socket.url, test.socket.target)
	time.Sleep(time.Millisecond * 100)

	req := "hello Test_13_RawRequest"
	reply, err := test.socket.RawRequest(req)
	require().NoError(err)
	fmt.Printf("client recevied: %s\n", reply)
}

func (test *TestClientSuite) Test_14_RawSubmit() {
	require := test.Require

	go test.runBackend("Test_14_RawSubmit", test.socket.url, test.socket.target)
	time.Sleep(time.Millisecond * 100)

	req := "hello Test_14_RawSubmit"
	err := test.socket.RawSubmit(req)
	require().NoError(err)
}

func (test *TestClientSuite) Test_15_DealerRawRequest() {
	require := test.Require

	go test.runBackend("Test_15_DealerRawRequest", test.socket.url, test.socket.target)
	time.Sleep(time.Millisecond * 100)

	socket, err := NewRaw(zmq.ROUTER, "inproc://sample_router")
	require().NoError(err)
	test.socket = socket
	test.socket.socketType = zmq.DEALER

	test.socket.Timeout(time.Second).Attempt(minAttempt)

	req := "hello Test_15_DealerRawRequest"
	reply, err := test.socket.RawRequest(req)
	require().NoError(err)
	fmt.Printf("client recevied: %s\n", reply)
}

func (test *TestClientSuite) Test_16_DealerRawSubmit() {
	require := test.Require

	go test.runBackend("Test_16_DealerRawSubmit", test.socket.url, test.socket.target)
	time.Sleep(time.Millisecond * 100)

	socket, err := NewRaw(zmq.ROUTER, "inproc://sample_router")
	require().NoError(err)
	test.socket = socket
	test.socket.socketType = zmq.DEALER
	test.socket.Timeout(time.Second).Attempt(minAttempt)

	req := "hello Test_16_DealerRawSubmit"
	err = test.socket.RawSubmit(req)
	require().NoError(err)
}

func (test *TestClientSuite) Test_17_RequestToRep() {
	require := test.Require

	url := "inproc://sample_router"
	go test.runBackend("Test_17_RequestToRep", url, zmq.REP)
	time.Sleep(time.Millisecond * 100)

	socket, err := NewRaw(zmq.REP, url)
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
}

func TestClient(t *testing.T) {
	suite.Run(t, new(TestClientSuite))
}
