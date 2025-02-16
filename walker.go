package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// PathItem represents a file path item.
type PathItem struct {
	BaseDir  string
	FullPath string
	RelPath  string
}

// pathWalker walks the directory tree and sends the file paths to the filenames channel.
func pathWalker(searchPath string, info os.FileInfo, filenames chan PathItem, chunkWaitGroup *sync.WaitGroup) {
	defer close(filenames)
	defer chunkWaitGroup.Done()

	if info.IsDir() {
		baseDir := searchPath
		filepath.WalkDir(searchPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if !d.IsDir() {
				relPath, _ := filepath.Rel(baseDir, path)
				filenames <- PathItem{BaseDir: baseDir, FullPath: path, RelPath: relPath}
			}

			return nil
		})
	} else {
		baseDir := filepath.Base(searchPath)
		relPath, _ := filepath.Rel(baseDir, searchPath)
		filenames <- PathItem{BaseDir: baseDir, FullPath: searchPath, RelPath: relPath}
	}
}
