package groclick

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/testcontainers/testcontainers-go"
)

func TestBootstrapper(t *testing.T) {
	t.Run("should be able to be able fail", func(t *testing.T) {
		t.Run("when can't open file dir", func(t *testing.T) {
			tc := newMigratorTestCase(t)

			tc.Given(
				ArrangeExpectError,
				ArrangePlainMigratorConfig(tc.Deps.DB),
			).Then(
				ExpectError,
			)

			tc.Deps.FS.EXPECT().Open(mock.Anything).Return(nil, tc.State.ExpectError)

			_, tc.State.ResultError = bootstrapper[Deps](config{
				fs: tc.Deps.FS,
			})(context.Background())
		})
		t.Run("when can't run clickhouse container", func(t *testing.T) {
			tc := newMigratorTestCase(t)

			tc.Given(
				ArrangeExpectError,
			).Then(
				ExpectError,
			)

			_, tc.State.ResultError = bootstrapper[Deps](config{
				fs: tc.Deps.FS,
				migrator: func(ctx context.Context, migratorConfig MigratorConfig) error {
					return nil
				},
				runner: func(
					ctx context.Context,
					img string,
					opts ...testcontainers.ContainerCustomizer,
				) (ClickhouseContainer, error) {
					return nil, tc.State.ExpectError
				},
			})(context.Background())
		})

		t.Run("when can't get connection string from postgres container", func(t *testing.T) {
			tc := newMigratorTestCase(t)

			tc.Given(
				ArrangeExpectError,
			).Then(
				ExpectError,
			)

			cont := NewMockClickhouseContainer(t)

			cont.EXPECT().ConnectionString(mock.Anything).Return("", tc.State.ExpectError)

			_, tc.State.ResultError = bootstrapper[Deps](config{
				migrator: func(ctx context.Context, migratorConfig MigratorConfig) error {
					return nil
				},
				runner: func(
					ctx context.Context,
					img string, opts ...testcontainers.ContainerCustomizer,
				) (ClickhouseContainer, error) {
					return cont, nil
				},
			})(context.Background())
		})

	})

}
