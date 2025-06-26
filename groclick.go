//go:generate go tool mockery
package groclick

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/godepo/groat/integration"
	"github.com/godepo/groat/pkg/ctxgroup"
	"github.com/godepo/groclick/internal/pkg/containersync"
	"github.com/spf13/afero"

	clickConn "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

type (
	ClickhouseContainer interface {
		ConnectionString(ctx context.Context, args ...string) (string, error)
		Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
	}

	Connect struct {
		driver.Conn
	}

	containerRunner func(
		ctx context.Context,
		img string,
		opts ...testcontainers.ContainerCustomizer,
	) (ClickhouseContainer, error)

	Container[T any] struct {
		forks                *atomic.Int32
		click                ClickhouseContainer
		ctx                  context.Context
		migrator             Migrator
		migrationsPath       string
		connString           string
		injectLabel          string
		opts                 *clickConn.Options
		root                 driver.Conn
		connConstructor      func(opt *clickConn.Options) (driver.Conn, error)
		injectLabelForConfig string
	}
	config struct {
		user                 string
		password             string
		containerImage       string
		imageEnvValue        string
		migrator             Migrator
		fs                   afero.Fs
		migrationsPath       string
		injectLabel          string
		runner               containerRunner
		connConstructor      func(opt *clickConn.Options) (driver.Conn, error)
		hostedDBNamespace    string
		hostedDSN            string
		injectLabelForConfig string
	}

	DB interface {
		Exec(ctx context.Context, query string, args ...any) error
	}

	MigratorConfig struct {
		DBName   string
		Path     string
		DB       DB
		UserName string
		Password string
		Config   *clickConn.Options
	}

	Migrator func(ctx context.Context, migratorConfig MigratorConfig) error

	Option func(*config)
)

func WithContainerImage(image string) Option {
	return func(c *config) {
		c.containerImage = image
	}
}

func WithUsername(user string) Option {
	return func(c *config) {
		c.user = user
	}
}

func WithPassword(password string) Option {
	return func(c *config) {
		c.password = password
	}
}

func WithMigrator(migrator Migrator) Option {
	return func(c *config) {
		c.migrator = migrator
	}
}

func WithMigrationsPath(path string) Option {
	return func(c *config) {
		c.migrationsPath = path
	}
}

func WithInjectLabel(label string) Option {
	return func(c *config) {
		c.injectLabel = label
	}
}

func WithHostedDBNamespace(namespace string) Option {
	return func(c *config) {
		c.hostedDBNamespace = namespace
	}
}

func WithInjectLabelForConfig(label string) Option {
	return func(c *config) {
		c.injectLabelForConfig = label
	}
}

func New[T any](options ...Option) integration.Bootstrap[T] {
	cfg := config{
		user:                 "",
		password:             "",
		containerImage:       "clickhouse/clickhouse-server:23.3.8.21-alpine",
		imageEnvValue:        "GROAT_I9N_CH_IMAGE",
		injectLabel:          "clickhouse",
		migrationsPath:       "../sql/migrations",
		fs:                   afero.NewOsFs(),
		connConstructor:      clickConn.Open,
		injectLabelForConfig: "clickhouse.config",
		runner: func(
			ctx context.Context,
			img string, opts ...testcontainers.ContainerCustomizer,
		) (ClickhouseContainer, error) {
			return clickhouse.Run(ctx, img, opts...)
		},
	}

	for _, op := range options {
		op(&cfg)
	}

	if env := os.Getenv(cfg.imageEnvValue); env != "" {
		cfg.containerImage = env
	}

	if env := os.Getenv("GROAT_I9N_CH_DSN"); env != "" {
		cfg.hostedDSN = env

		return hostedBootstrapper[T](cfg)
	}

	return bootstrapper[T](cfg)
}

func bootstrapper[T any](cfg config) integration.Bootstrap[T] {
	return func(ctx context.Context) (integration.Injector[T], error) {
		if cfg.migrator == nil {
			mig, err := PlainMigrator(cfg.fs, cfg.migrationsPath)
			if err != nil {
				return nil, err
			}
			cfg.migrator = mig
		}

		clickhouseContainer, err := cfg.runner(
			ctx,
			cfg.containerImage,
			clickhouse.WithUsername(cfg.user),
			clickhouse.WithPassword(cfg.password),
			testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Tmpfs: map[string]string{"/tmpfs": "rw"},
					Env:   map[string]string{"PGDATA": "/tmpfs"},
				},
			}),
		)

		if err != nil {
			return nil, fmt.Errorf("postgres container failed to run: %w", err)
		}

		ctxgroup.IncAt(ctx)

		go containersync.Terminator(ctx, clickhouseContainer.Terminate)()

		container, err := newContainer[T](ctx, clickhouseContainer, cfg)
		if err != nil {
			return nil, err
		}

		return container.Injector, nil
	}
}
