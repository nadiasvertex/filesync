package main

import (
	"archive/zip"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nadiasvertex/filesync/syncstream"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
)

// chunkStreamer is responsible for streaming chunks to the output.
func chunkStreamer(out io.Writer, chunks chan ChunkItem, chunkWaitGroup *sync.WaitGroup) {
	defer chunkWaitGroup.Done()

	currentIndex := 0
	for chunk := range chunks {
		if chunk.FileIndex != currentIndex {
			if currentIndex > 0 {
				binary.Write(out, binary.BigEndian, ChunkFrameKindEnd)
			}
			binary.Write(out, binary.BigEndian, ChunkFrameKindStart)
			binary.Write(out, binary.BigEndian, chunk.Path)
			fmt.Println(chunk.Path)
			currentIndex = chunk.FileIndex
		}

		binary.Write(out, binary.BigEndian, ChunkFrameKindData)
		binary.Write(out, binary.BigEndian, ChunkDataFrame{
			Offset:      uint64(chunk.Content.Offset),
			Length:      uint64(chunk.Content.Length),
			Fingerprint: uint64(chunk.Content.Fingerprint),
		})
	}
}

// chunkSender sends missing chunks into the output archive.
func chunkSender(chunks chan ChunkItem, chunkWaitGroup *sync.WaitGroup) {
	defer chunkWaitGroup.Done()

	archivePath := *sendArchivePath
	if filepath.Ext(archivePath) != ".zip" {
		archivePath += ".zip"
	}

	// The send file is just a zip archive with the chunks and a file index mapping.
	archiveFile, err := os.Create(archivePath)
	if err != nil {
		log.Fatalf("Error creating archive file '%s': %v", archivePath, err)
	}
	defer archiveFile.Close()

	archive := zip.NewWriter(archiveFile)
	defer archive.Close()

	chunkStorage, err := os.CreateTemp(os.TempDir(), "chunks.*.bin")
	if err != nil {
		log.Fatalf("Error creating temporary file for changed chunks: %v", err)
	}
	defer chunkStorage.Close()

	fileMap, err := os.CreateTemp(os.TempDir(), "map.*.bin")
	if err != nil {
		log.Fatalf("Error creating temporary file for file index mapping: %v", err)
	}
	defer fileMap.Close()

	out := getCompressor(chunkStorage)
	defer out.Close()

	mapOut := getCompressor(fileMap)
	defer mapOut.Close()

	fileIndexMap := make(map[string]uint64)

	for chunk := range chunks {
		_, exists := fileIndexMap[chunk.Path]
		if !exists {
			fileIndexMap[chunk.Path] = uint64(chunk.FileIndex)
			protodelim.MarshalTo(mapOut, &syncstream.FileIndexMapping{
				Path:      chunk.Path,
				FileIndex: uint64(chunk.FileIndex),
			})
		}

		if *verbose {
			log.Printf("%s offset=%d length=%d fingerprint=%x fileIndex=%d", chunk.Path, chunk.Content.Offset, chunk.Content.Length, chunk.Content.Fingerprint, chunk.FileIndex)
		}

		protodelim.MarshalTo(out, &syncstream.Chunk{
			Offset:      uint64(chunk.Content.Offset),
			Length:      uint64(chunk.Content.Length),
			Fingerprint: chunk.Content.Fingerprint,
			FileIndex:   uint64(chunk.FileIndex),
			Data:        chunk.Content.Data,
		})
	}

	// Reset the file pointers to the beginning
	mapOut.Close()
	if _, err := fileMap.Seek(0, io.SeekStart); err != nil {
		log.Fatalf("Error seeking file map: %v", err)
	}

	// Add the mapping file to the archive.
	if w, err := archive.CreateHeader(&zip.FileHeader{
		Name:     "file_index_map",
		Method:   zip.Store,
		Modified: time.Now(),
	}); err != nil {
		log.Fatalf("Error creating file index map in archive '%s': %v", *sendArchivePath, err)
	} else {
		if _, err := io.Copy(w, fileMap); err != nil {
			log.Fatalf("Error writing file index map to archive: %v", err)
		}
	}

	out.Close()
	if _, err := chunkStorage.Seek(0, io.SeekStart); err != nil {
		log.Fatalf("Error seeking chunk storage: %v", err)
	}

	// Add the chunk file to the archive.
	if w, err := archive.CreateHeader(&zip.FileHeader{
		Name:     "chunks",
		Method:   zip.Store,
		Modified: time.Now(),
	}); err != nil {
		log.Fatalf("Error creating chunk storage in archive '%s': %v", *sendArchivePath, err)
	} else {
		if _, err := io.Copy(w, chunkStorage); err != nil {
			log.Fatalf("Error writing chunk storage to archive: %v", err)
		}
	}

	// Add the settings file to the archive.
	if w, err := archive.CreateHeader(&zip.FileHeader{
		Name:     "settings",
		Method:   zip.Store,
		Modified: time.Now(),
	}); err != nil {
		log.Fatalf("Error creating settings file in archive '%s': %v", *sendArchivePath, err)
	} else {
		data, err := proto.Marshal(&syncstream.Settings{
			Compression: *compression,
		})
		if err != nil {
			log.Fatalf("Error marshaling settings: %v", err)
		}
		if _, err := w.Write(data); err != nil {
			log.Fatalf("Error writing settings to archive: %v", err)
		}
	}
}
