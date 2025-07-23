package groclick

import (
	"os"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/godepo/groat"
	"github.com/godepo/groat/integration"
)

type (
	SystemUnderTest struct {
	}

	State struct {
	}
	Deps struct {
		Conn *Connect            `groat:"grohouse"`
		Cfg  *clickhouse.Options `groat:"grocfg"`
		DSN  string              `groat:"grodsn"`
	}
)

var suite *integration.Container[Deps, State, *SystemUnderTest]

func TestMain(m *testing.M) {
	_ = os.Setenv("GROAT_I9N_CH_IMAGE", "clickhouse/clickhouse-server:23.3.8.21-alpine")

	suite = integration.New[Deps, State, *SystemUnderTest](
		m,
		func(t *testing.T) *groat.Case[Deps, State, *SystemUnderTest] {
			tcs := groat.New[Deps, State, *SystemUnderTest](t, func(t *testing.T, deps Deps) *SystemUnderTest {
				return &SystemUnderTest{}
			})
			return tcs
		},
		New[Deps](
			WithMigrationsPath("./sql"),
			WithInjectLabel("grohouse"),
			WithContainerImage("clickhouse/clickhouse-server:23.3.8.21-alpine"),
			WithUsername("default"),
			WithPassword("test"),
			WithInjectLabelForConfig("grocfg"),
			WithInjectLabelForDSN("grodsn"),
		),
	)
	os.Exit(suite.Go())
}
