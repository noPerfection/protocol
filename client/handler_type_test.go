package client

import (
	"testing"

	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
)

type HandlerTypeSuite struct {
	suite.Suite
}

func (test *HandlerTypeSuite) Test_10_IsTarget() {
	require := test.Require
	require().True(isTarget(SyncReplierType))
	require().True(isTarget(WorkerType))
	require().True(isTarget(PairType))
	require().True(isTarget(PublisherType))
	require().True(isTarget(ReplierType))

	require().False(isTarget(HandlerType("")))
}

func (test *HandlerTypeSuite) Test_11_TargetToClient() {
	require := test.Require

	require().Equal(zmq.REQ, targetToClient(SyncReplierType))
	require().Equal(zmq.PUSH, targetToClient(WorkerType))
	require().Equal(zmq.PAIR, targetToClient(PairType))
	require().Equal(zmq.SUB, targetToClient(PublisherType))
	require().Equal(zmq.DEALER, targetToClient(ReplierType))

	require().Equal(zmq.Type(-1), targetToClient(HandlerType("")))
}

func TestHandlerType(t *testing.T) {
	suite.Run(t, new(HandlerTypeSuite))
}
