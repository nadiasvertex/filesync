package main

import (
	"archive/zip"
	"bufio"
	"io"
	"log"
	"os"

	"github.com/nadiasvertex/filesync/syncstream"
	"google.golang.org/protobuf/encoding/protodelim"
)

// chunkReader reads chunks from the archive and sends them to the receiver for patching.
func chunkReader() {
	archiveFile, err := os.Open(*sendArchivePath)
	if err != nil {
		log.Fatalf("Error opening archive file '%s': %v", *sendArchivePath, err)
	}
	defer archiveFile.Close()

	info, err := archiveFile.Stat()
	if err != nil {
		log.Fatalf("Error getting file info for '%s': %v", *sendArchivePath, err)
	}

	archive, err := zip.NewReader(archiveFile, info.Size())
	if err != nil {
		log.Fatalf("Error opening archive file '%s': %v", *sendArchivePath, err)
	}

	// Load settings from archive and update the compression flag
	settings := readSettings(archive)
	if settings.Compression != *compression {
		*compression = settings.Compression
	}

	// Load the file index map from the archive
	fileMap := readFileMap(archive)

	chunks, err := archive.Open("chunks")
	if err != nil {
		log.Fatalf("Error opening chunks file in archive: %v", err)
	}

	in := bufio.NewReader(getDecompressor(chunks))

	for {
		var chunk syncstream.Chunk
		if err := protodelim.UnmarshalFrom(in, &chunk); err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("Error reading chunk from archive: %v", err)
		}

		path := fileMap[chunk.FileIndex]
		if *verbose {
			log.Printf("'%s' offset:%d length:%d fingerprint:%d", path, chunk.Offset, chunk.Length, chunk.Fingerprint)
		}
	}
}
