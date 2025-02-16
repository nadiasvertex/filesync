package main

import (
	"log"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/nadiasvertex/filesync/syncstream"
	"google.golang.org/protobuf/proto"
)

// indexUpdateStreamer is responsible for streaming chunk fingerprints to the index. It
// receives chunk items from the channel and writes them to the index database. The
// index can later be used to retrieve the fingerprints of chunks that are present
// in the file and compare them with the fingerprints of chunks in the target file.
func indexUpdateStreamer(chunks chan ChunkItem, chunkWaitGroup *sync.WaitGroup) {
	defer chunkWaitGroup.Done()

	opt := badger.DefaultOptions(*indexDir)
	db, err := badger.Open(opt)
	if err != nil {
		log.Fatalf("Error opening index database '%s': %v", *indexDir, err)
	}
	defer db.Close()

	for chunk := range chunks {
		if err = db.Update(func(txn *badger.Txn) error {
			key := chunkToKey(chunk)
			data, err := proto.Marshal(&syncstream.Chunk{
				Offset:      uint64(chunk.Content.Offset),
				Length:      uint64(chunk.Content.Length),
				Fingerprint: chunk.Content.Fingerprint,
			})
			if err != nil {
				return err
			}
			return txn.Set(key, data)
		}); err != nil {
			log.Fatalf("Error writing index for file '%s': %v", chunk.Path, err)
		}
	}
}
