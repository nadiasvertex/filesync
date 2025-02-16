package main

import (
	"fmt"
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
		chunker, err := cdc.NewChunker(f, cdc.Options{
			MinSize:              *minChunkSize,
			MaxSize:              *maxChunkSize,
			AverageSize:          *avgChunkSize,
			Normalization:        *normalization,
			DisableNormalization: *disableNormalization,
		})
		if err != nil {
			log.Fatalf("Error creating chunker for file %s: %v", filename.FullPath, err)
		}

		for {
			chunk, err := chunker.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalf("Error reading chunk from file %s: %v", filename.FullPath, err)
			}
			chunks <- ChunkItem{Path: filename.RelPath, FileIndex: idx, Content: chunk}
		}
	}
}
