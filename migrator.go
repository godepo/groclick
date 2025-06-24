package groclick

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
)

const defaultExpMigrations = 8

func PlainMigrator(fs afero.Fs, path string) (Migrator, error) {
	migrations := make([]string, 0, defaultExpMigrations)

	dir, err := fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("can't open migration dir: %w", err)
	}

	defer func(dir afero.File) {
		_ = dir.Close()
	}(dir)

	list, err := dir.Readdir(0)
	if err != nil {
		return nil, fmt.Errorf("can't read migrations file list: %w", err)
	}

	for _, info := range list {
		if info.IsDir() {
			continue
		}

		data, err := readMigrationFile(fs, filepath.Join(path, info.Name()))
		if err != nil {
			return nil, err
		}

		migrations = append(migrations, data)
	}

	return func(ctx context.Context, cfg MigratorConfig) error {
		for i, migration := range migrations {
			for j, cmd := range strings.Split(migration, ";") {
				cmd = strings.TrimSpace(cmd)
				if cmd == "" {
					continue
				}

				if err := cfg.DB.Exec(ctx, cmd); err != nil {
					return fmt.Errorf(
						"can't execute migration num=%d and command=%d %s: %w",
						i, j,
						cmd,
						err,
					)
				}
			}
		}

		return nil
	}, nil
}

func readMigrationFile(fs afero.Fs, filePath string) (string, error) {
	fh, err := fs.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("can't open migration file %s: %w", filePath, err)
	}

	defer func(fh afero.File) {
		_ = fh.Close()
	}(fh)

	data, err := io.ReadAll(fh)
	if err != nil {
		return "", fmt.Errorf("can't read file %s: %w", filePath, err)
	}

	return string(data), nil
}
