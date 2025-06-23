package groclick

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/godepo/groat/integration"
	"github.com/godepo/groat/pkg/generics"
	"github.com/stretchr/testify/require"
)

var ErrRequireNamespacePrefixForHostedDB = errors.New("hosted db requires namespace prefix")

type hostedClickhouse[T any] struct {
	root  driver.Conn
	cfg   config
	forks *atomic.Int32
	ctx   context.Context
}

func hostedBootstrapper[T any](cfg config) integration.Bootstrap[T] {
	return func(ctx context.Context) (integration.Injector[T], error) {
		if cfg.hostedDBNamespace == "" {
			return nil, ErrRequireNamespacePrefixForHostedDB
		}

		if cfg.migrator == nil {
			mig, err := PlainMigrator(cfg.fs, cfg.migrationsPath)
			if err != nil {
				return nil, err
			}
			cfg.migrator = mig
		}

		local := &hostedClickhouse[T]{
			cfg:   cfg,
			forks: &atomic.Int32{},
			ctx:   ctx,
		}

		opts, err := clickhouse.ParseDSN(cfg.hostedDSN)
		if err != nil {
			return nil, fmt.Errorf("can't parse hosted dsn: %w", err)
		}

		conn, err := cfg.connConstructor(opts)
		if err != nil {
			return nil, fmt.Errorf("can't create connection to hosted db: %w", err)
		}

		local.root = conn

		return local.Injector, nil
	}
}

func (c *hostedClickhouse[T]) Injector(t *testing.T, to T) T {
	t.Helper()

	cfg, err := clickhouse.ParseDSN(c.cfg.hostedDSN)
	require.NoError(t, err)

	cfg.Auth.Database = fmt.Sprintf("%s_%d", cfg.Auth.Database, c.forks.Add(1))

	dbName := c.cfg.hostedDBNamespace + cfg.Auth.Database
	err = c.root.Exec(
		c.ctx,
		"CREATE DATABASE "+dbName,
	)
	require.NoError(t, err,
		"can't created database=%s for user %s",
		dbName, cfg.Auth.Username,
	)

	t.Cleanup(func() {
		err := c.root.Exec(c.ctx, "DROP DATABASE "+dbName)
		if err != nil {
			t.Logf("can't cleanup database %s: %v", dbName, err)
		}
	})

	cfg.Auth.Database = dbName

	con, err := c.cfg.connConstructor(cfg)
	require.NoError(t, err)

	require.NoError(t, con.Ping(c.ctx))

	err = c.cfg.migrator(c.ctx, MigratorConfig{
		DB:       con,
		DBName:   cfg.Auth.Database,
		Path:     c.cfg.migrationsPath,
		UserName: cfg.Auth.Username,
		Password: cfg.Auth.Password,
	})
	require.NoError(t, err)
	res := generics.Injector(t, &Connect{con}, to, c.cfg.injectLabel)

	return res
}
