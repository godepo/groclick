package groclick

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	tc := suite.Case(t)

	require.NotNil(t, tc.Deps.Conn)
	require.NotNil(t, tc.Deps.Cfg)
	require.NotEmpty(t, tc.Deps.DSN)
}
