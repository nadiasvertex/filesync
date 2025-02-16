package main

import (
	"archive/zip"
	"bufio"
	"io"
	"log"

	"github.com/nadiasvertex/filesync/syncstream"
	"google.golang.org/protobuf/encoding/protodelim"
)

// Read filemap from the archive.
func readFileMap(archive *zip.Reader) map[uint64]string {
	filemap, err := archive.Open("file_index_map")
	if err != nil {
		log.Fatalf("error opening file index map in archive: %v", err)
	}
	defer filemap.Close()

	in := bufio.NewReader(getDecompressor(filemap))
	indexMap := make(map[uint64]string)

	for {
		var fileIndexMap syncstream.FileIndexMapping
		if err := protodelim.UnmarshalFrom(in, &fileIndexMap); err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("Error reading chunk from archive: %v", err)
		}
		indexMap[fileIndexMap.FileIndex] = fileIndexMap.Path

		if *verbose {
			log.Printf("%+v", &fileIndexMap)
		}
	}

	return indexMap
}
