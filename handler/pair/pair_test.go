package pair

import (
	"testing"

	"github.com/noPerfection/protocol/handler/config"

	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestPairSuite struct {
	suite.Suite

	externalConfig *config.Handler
}

func (test *TestPairSuite) SetupTest() {
	test.externalConfig = config.New(config.ReplierType, "external_1", "external_main", 100)
}

// Test_0_Config tests converting external configuration to the pair type
func (test *TestPairSuite) Test_0_Config() {
	s := test.Suite.Require

	pairConfig := Config(test.externalConfig)
	s().Equal(config.PairType, pairConfig.Type)
	s().Equal(test.externalConfig.Id+"_pair", pairConfig.Id)
	s().Equal(test.externalConfig.Category+"_pair", pairConfig.Category)
	s().Zero(pairConfig.Port)
}

// Test_1_PairClient tests the creation of the pair client for the given external handler.
func (test *TestPairSuite) Test_1_PairClient() {
	s := test.Suite.Require

	pairClient, err := NewClient(test.externalConfig)
	s().NoError(err)
	s().NotNil(pairClient)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestPair(t *testing.T) {
	suite.Run(t, new(TestPairSuite))
}
