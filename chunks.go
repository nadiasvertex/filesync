package main

import (
	"fmt"
	"hash/adler32"
	"io"
	"log"
	"os"
	"sync"

	cdc "github.com/jotfs/fastcdc-go"
)

type ChunkItem struct {
	Path      string
	FileIndex int
	Content   cdc.Chunk
}

type ChunkFrameKind uint8

const (
	FileStart ChunkFrameKind = iota
	FileData
	FileEnd
)

const (
	ChunkFrameKindStart ChunkFrameKind = iota
	ChunkFrameKindData
	ChunkFrameKindEnd
)

type ChunkDataFrame struct {
	Offset      uint64
	Length      uint64
	Fingerprint uint64
}

// chunkToKey converts a ChunkItem to a key string.
func chunkToKey(chunk ChunkItem) []byte {
	key := fmt.Sprintf("%s:%d", chunk.Path, chunk.Content.Offset)
	return []byte(key)
}

// chunkFile chunks a file into smaller parts.
func chunkFile(f *os.File, p PathItem, idx int, chunks chan ChunkItem) {
	chunker, err := cdc.NewChunker(f, cdc.Options{
		MinSize:              *minChunkSize,
		MaxSize:              *maxChunkSize,
		AverageSize:          *avgChunkSize,
		Normalization:        *normalization,
		DisableNormalization: *disableNormalization,
	})
	if err != nil {
		log.Fatalf("Error creating chunker for file %s: %v", p.FullPath, err)
	}

	for {
		chunk, err := chunker.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("Error reading chunk from file %s: %v", p.FullPath, err)
		}
		chunks <- ChunkItem{Path: p.RelPath, FileIndex: idx, Content: chunk}
	}
}

// fileChunker is responsible for chunking files into smaller parts.
func fileChunker(filenames chan PathItem, chunks chan ChunkItem, chunkWaitGroup *sync.WaitGroup) {
	defer close(chunks)
	defer chunkWaitGroup.Done()

	idx := 0
	for filename := range filenames {
		idx += 1
		f, err := os.Open(filename.FullPath)
		if err != nil {
			log.Fatalf("Error opening file %s: %v", filename.FullPath, err)
		}
		defer f.Close()

		info, err := f.Stat()
		if err != nil {
			log.Fatalf("Error getting file info for %s: %v", filename.FullPath, err)
		}

		if info.Size() <= int64(*minChunkSize) {
			// The file is too small to chunk, so we just generate a checksum for the whole file
			// and submit it as a chunk.
			data, err := io.ReadAll(f)
			if err != nil {
				log.Fatalf("Error reading file %s: %v", filename.FullPath, err)
			}
			chunks <- ChunkItem{
				Path:      filename.RelPath,
				FileIndex: idx,
				Content: cdc.Chunk{
					Offset:      0,
					Length:      int(info.Size()),
					Fingerprint: uint64(adler32.Checksum(data)),
					Data:        data,
				},
			}
		} else {
			chunkFile(f, filename, idx, chunks)
		}
	}
}
