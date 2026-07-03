package base

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type TestHandlerTypeSuite struct {
	suite.Suite
}

func (test *TestHandlerTypeSuite) Test_10_IsValid() {
	s := test.Suite.Require

	s().Error(IsValid(UnknownType))
	s().NoError(IsValid(PublisherType))
}

func TestHandlerType(t *testing.T) {
	suite.Run(t, new(TestHandlerTypeSuite))
}
