package groclick

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	tc := suite.Case(t)
	tc.When()
	require.NotNil(t, tc.Deps.Conn)
}
