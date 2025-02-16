package main

import (
	"log"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/nadiasvertex/filesync/syncstream"
	"google.golang.org/protobuf/proto"
)

// indexCompareStreamer compares the fingerprints of chunks in the source file with
// the fingerprints of chunks in the target file and sends the missing chunks to the
// chunkSender.
func indexCompareStreamer(sourceChunks chan ChunkItem, missingChunks chan ChunkItem, chunkWaitGroup *sync.WaitGroup) {
	defer chunkWaitGroup.Done()
	defer close(missingChunks)

	opt := badger.DefaultOptions(*indexDir)
	db, err := badger.Open(opt)
	if err != nil {
		log.Fatalf("Error opening index database '%s': %v", *indexDir, err)
	}
	defer db.Close()

	for chunk := range sourceChunks {
		key := chunkToKey(chunk)
		var data []byte
		if err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				data = val
				return nil
			})
		}); err != nil {
			if err == badger.ErrKeyNotFound {
				missingChunks <- chunk
				continue
			} else {
				log.Fatalf("Error reading index for file '%s': %v", chunk.Path, err)
			}
		}

		var targetChunk syncstream.Chunk
		if err := proto.Unmarshal(data, &targetChunk); err != nil {
			log.Fatalf("Error unmarshaling index for file '%s': %v", chunk.Path, err)
		}

		if targetChunk.Fingerprint != chunk.Content.Fingerprint {
			missingChunks <- chunk
		}
	}
}
