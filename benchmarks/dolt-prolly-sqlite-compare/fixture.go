package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type fixtureLayout struct {
	output     string
	records    int
	repetition int
}

func (l fixtureLayout) sourceDir() string {
	return filepath.Join(l.output, "fixtures", fmt.Sprint(l.records), fmt.Sprintf("run-%d", l.repetition))
}
func (l fixtureLayout) sourceDB() string { return filepath.Join(l.sourceDir(), "prolly.db") }
func (l fixtureLayout) cellDir(spec cellSpec) string {
	return filepath.Join(l.output, "cells", fmt.Sprint(l.records), fmt.Sprintf("run-%d", l.repetition), string(spec.operation), string(spec.pattern), string(spec.cacheState))
}
func (l fixtureLayout) cellDB(spec cellSpec) string {
	return filepath.Join(l.cellDir(spec), "prolly.db")
}

func cloneFixture(source, destination string) error {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("fixture source must be a real directory: %s", source)
	}
	if _, err := os.Lstat(destination); !os.IsNotExist(err) {
		return fmt.Errorf("fixture destination already exists: %s", destination)
	}
	return copyDirectory(source, destination)
}

func copyDirectory(source, destination string) error {
	if err := os.MkdirAll(destination, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		sourcePath := filepath.Join(source, entry.Name())
		destinationPath := filepath.Join(destination, entry.Name())
		info, err := os.Lstat(sourcePath)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("fixture contains symlink: %s", sourcePath)
		}
		if info.IsDir() {
			if err := copyDirectory(sourcePath, destinationPath); err != nil {
				return err
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("fixture contains unsupported entry: %s", sourcePath)
		}
		if err := copyFile(sourcePath, destinationPath, info.Mode().Perm()); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(source, destination string, mode os.FileMode) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func safeRemove(root, target string) error {
	cleanRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	cleanTarget, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(cleanRoot, cleanTarget)
	if err != nil || relative == "." || relative == ".." || len(relative) >= 3 && relative[:3] == ".."+string(os.PathSeparator) {
		return fmt.Errorf("refusing to remove path outside generated root: %s", target)
	}
	if info, err := os.Lstat(cleanTarget); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing to remove symlink: %s", target)
	} else if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	return os.RemoveAll(cleanTarget)
}

func sqliteFileBytes(database string) (dbBytes, walBytes, shmBytes, total uint64, err error) {
	length := func(path string) (uint64, error) {
		info, statErr := os.Stat(path)
		if os.IsNotExist(statErr) {
			return 0, nil
		}
		if statErr != nil {
			return 0, statErr
		}
		return uint64(info.Size()), nil
	}
	if dbBytes, err = length(database); err != nil {
		return
	}
	if walBytes, err = length(database + "-wal"); err != nil {
		return
	}
	if shmBytes, err = length(database + "-shm"); err != nil {
		return
	}
	total = dbBytes + walBytes + shmBytes
	return
}
