package groclick

import (
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewContainer(t *testing.T) {
	t.Run("should be able fail", func(t *testing.T) {
		t.Run("when can't get connection string", func(t *testing.T) {
			cont := NewMockClickhouseContainer(t)
			exp := errors.New(uuid.NewString())

			cont.EXPECT().ConnectionString(t.Context()).Return("", exp)

			cfg := config{}
			res, err := newContainer[Deps](t.Context(), cont, cfg)
			require.Error(t, err)
			require.Nil(t, res)
			assert.ErrorIs(t, err, exp)
		})

		t.Run("when can't parse connection string", func(t *testing.T) {
			cont := NewMockClickhouseContainer(t)

			cont.EXPECT().ConnectionString(t.Context()).Return(uuid.NewString(), nil)

			cfg := config{}
			res, err := newContainer[Deps](t.Context(), cont, cfg)
			require.Error(t, err)
			require.Nil(t, res)
		})

		t.Run("when can't create connection to clickhouse", func(t *testing.T) {
			cont := NewMockClickhouseContainer(t)
			exp := errors.New(uuid.NewString())

			cont.EXPECT().ConnectionString(t.Context()).
				Return(
					"clickhouse://username:password@host1:9000,host2:9000/database?dial_timeout=200ms&max_execution_time=60",
					nil,
				)

			cfg := config{
				connConstructor: func(opt *clickhouse.Options) (driver.Conn, error) {

					return nil, exp

				},
			}
			res, err := newContainer[Deps](t.Context(), cont, cfg)
			require.Error(t, err)
			require.Nil(t, res)
			assert.ErrorIs(t, err, exp)
		})
	})
}
