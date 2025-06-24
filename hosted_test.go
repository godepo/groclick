package groclick

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestHostedClickhouse(t *testing.T) {
	envHost := "http://locaghost:8123/?dial_timeout=200ms&max_execution_time=60"
	t.Run("should be able to be able", func(t *testing.T) {
		conn := NewMockConn(t)

		cfg := config{
			hostedDSN:         envHost,
			fs:                afero.OsFs{},
			migrationsPath:    "./sql",
			hostedDBNamespace: uuid.NewString(),
			connConstructor: func(opt *clickhouse.Options) (driver.Conn, error) {
				return conn, nil
			},
		}

		res, err := hostedBootstrapper[Deps](cfg)(t.Context())
		require.NoError(t, err)
		require.NotNil(t, res)
	})
	t.Run("should be able failed", func(t *testing.T) {
		t.Run("when given not exists namespace", func(t *testing.T) {
			t.Setenv("GROAT_I9N_CH_DSN",
				uuid.NewString())

			res, err := New[Deps](
				WithMigrator(
					func(ctx context.Context, migratorConfig MigratorConfig) error {
						return nil
					},
				),
			)(t.Context())

			require.ErrorIs(t, ErrRequireNamespacePrefixForHostedDB, err)
			require.Nil(t, res)
		})

		t.Run("when dsn can't be parsed", func(t *testing.T) {
			t.Setenv("GROAT_I9N_CH_DSN", uuid.NewString())

			res, err := New[Deps](WithHostedDBNamespace(uuid.NewString()), WithMigrationsPath("./sql"))(t.Context())
			require.Error(t, err)
			assert.ErrorContains(t, err, "can't parse hosted dsn", err.Error())
			require.Nil(t, res)
		})

		t.Run("when can't create connection to hosted db", func(t *testing.T) {
			exp := errors.New(uuid.NewString())
			cfg := config{
				hostedDSN:         envHost,
				fs:                afero.OsFs{},
				hostedDBNamespace: uuid.NewString(),
				migrationsPath:    "./sql",
				connConstructor: func(opt *clickhouse.Options) (driver.Conn, error) {
					return nil, exp
				},
			}

			res, err := hostedBootstrapper[Deps](cfg)(t.Context())
			require.Error(t, err)
			assert.ErrorIs(t, err, exp, err.Error())
			require.Nil(t, res)
		})

		t.Run("when can't open migrations dir", func(t *testing.T) {
			cfg := config{
				hostedDSN:         envHost,
				fs:                afero.OsFs{},
				hostedDBNamespace: uuid.NewString(),
				migrationsPath:    uuid.NewString(),
			}

			res, err := hostedBootstrapper[Deps](cfg)(t.Context())
			require.Error(t, err)
			assert.ErrorContains(t, err, "no such file or directory")
			require.Nil(t, res)
		})
	})
}

func TestHostedClickhouse_Injector(t *testing.T) {
	t.Run("should be able to be able", func(t *testing.T) {
		envHost := "http://locaghost:8123/?dial_timeout=200ms&max_execution_time=60"

		root := NewMockConn(t)
		conn := NewMockConn(t)

		hostedClick := hostedClickhouse[Deps]{
			root: root,
			cfg: config{
				hostedDSN:         envHost,
				hostedDBNamespace: uuid.NewString(),
				connConstructor: func(opt *clickhouse.Options) (driver.Conn, error) {
					return conn, nil
				},
				migrator: func(ctx context.Context, migratorConfig MigratorConfig) error {
					return nil
				},
			},
			forks: &atomic.Int32{},
			ctx:   t.Context(),
		}
		var deps Deps

		root.EXPECT().
			Exec(
				mock.Anything,
				"CREATE DATABASE "+hostedClick.cfg.hostedDBNamespace+"_1",
			).Return(nil)

		conn.EXPECT().Ping(mock.Anything).Return(nil)

		root.EXPECT().
			Exec(
				mock.Anything,
				"DROP DATABASE "+hostedClick.cfg.hostedDBNamespace+"_1",
			).Return(nil)

		t.Log("DROP DATABASE " + hostedClick.cfg.hostedDBNamespace + "_1")
		t.Log("WTF?")

		deps = hostedClick.Injector(t, deps)
	})

	t.Run("should be able failed", func(t *testing.T) {
		t.Run("when can't drop database", func(t *testing.T) {
			envHost := "http://locaghost:8123/?dial_timeout=200ms&max_execution_time=60"

			root := NewMockConn(t)
			conn := NewMockConn(t)

			hostedClick := hostedClickhouse[Deps]{
				root: root,
				cfg: config{
					hostedDSN:         envHost,
					hostedDBNamespace: uuid.NewString(),
					connConstructor: func(opt *clickhouse.Options) (driver.Conn, error) {
						return conn, nil
					},
					migrator: func(ctx context.Context, migratorConfig MigratorConfig) error {
						return nil
					},
				},
				forks: &atomic.Int32{},
				ctx:   t.Context(),
			}
			var deps Deps

			root.EXPECT().
				Exec(
					mock.Anything,
					"CREATE DATABASE "+hostedClick.cfg.hostedDBNamespace+"_1",
				).Return(nil)

			conn.EXPECT().Ping(mock.Anything).Return(nil)

			root.EXPECT().
				Exec(
					mock.Anything,
					"DROP DATABASE "+hostedClick.cfg.hostedDBNamespace+"_1",
				).Return(errors.New(uuid.NewString()))

			deps = hostedClick.Injector(t, deps)
		})
	})

}
