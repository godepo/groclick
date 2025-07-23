package groclick

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/godepo/groat/pkg/generics"
	"github.com/stretchr/testify/require"
)

func newContainer[T any](
	ctx context.Context,
	click ClickhouseContainer,
	cfg config,
) (*Container[T], error) {
	container := &Container[T]{
		forks:           &atomic.Int32{},
		click:           click,
		ctx:             ctx,
		migrator:        cfg.migrator,
		migrationsPath:  cfg.migrationsPath,
		connConstructor: cfg.connConstructor,
	}

	connString, err := click.ConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("can't get connection string: %w", err)
	}

	container.connString = connString
	container.injectLabel = cfg.injectLabel
	container.injectLabelForConfig = cfg.injectLabelForConfig
	container.injectLabelForDSN = cfg.injectLabelForDSN

	container.opts, err = clickhouse.ParseDSN(connString)
	if err != nil {
		return nil, fmt.Errorf("can't parse dsn: %w", err)
	}

	root, err := cfg.connConstructor(container.opts)
	if err != nil {
		return nil, fmt.Errorf("can't create connection to root db: %w", err)
	}
	container.root = root

	return container, nil
}

func (c *Container[T]) Injector(t *testing.T, to T) T {
	t.Helper()

	cfg, err := clickhouse.ParseDSN(c.connString)
	require.NoError(t, err)

	cfg.Auth.Database = fmt.Sprintf("%s_%d", cfg.Auth.Database, c.forks.Add(1))

	err = c.root.Exec(
		c.ctx,
		"CREATE DATABASE "+cfg.Auth.Database,
	)
	require.NoError(t, err,
		"can't created database=%s for user %s",
		cfg.Auth.Database, cfg.Auth.Username,
	)

	con, err := clickhouse.Open(cfg)
	require.NoError(t, err)

	require.NoError(t, con.Ping(c.ctx))

	err = c.migrator(c.ctx, MigratorConfig{
		Config:   cfg,
		DB:       con,
		DBName:   cfg.Auth.Database,
		Path:     c.migrationsPath,
		UserName: cfg.Auth.Username,
		Password: cfg.Auth.Password,
	})
	require.NoError(t, err)
	res := generics.Injector(t, &Connect{con}, to, c.injectLabel)
	res = generics.Injector(t, cfg, res, c.injectLabelForConfig)
	res = generics.Injector(t, c.connString, res, c.injectLabelForDSN)

	return res
}
