package main

//go:generate protoc --go_out=. --go_opt=paths=source_relative --proto_path=. syncstream/syncstream.proto

import (
	"fmt"
	"log"
	"os"
	"sync"

	flag "github.com/spf13/pflag"
)

const kiB = 1024
const miB = 1024 * kiB

var verbose = flag.Bool("verbose", false, "Enable verbose logging")
var avgChunkSize = flag.Int("avg-chunk-size", 1*miB, "Set the average chunk size. (default 1 MiB)")
var minChunkSize = flag.Int("min-chunk-size", 0, "Set the minimum chunk size. (default avg / 4)")
var maxChunkSize = flag.Int("max-chunk-size", 0, "Set the maximum chunk size. (default avg * 4)")
var normalization = flag.Int("normalization", 0, "Set the normalization level (default 2)")
var disableNormalization = flag.Bool("no-normalization", false, "Disable normalization (default false)")
var compression = flag.String("compression", "lz4", "Sets the kind of compression to use. Options are lz4, gzip, flate, and none. (default lz4)")
var mode = flag.String("mode", "index", "Sets the execution mode. Options are send, recv, sync, and index. (default index)")
var indexDir = flag.String("index-dir", "", "Required if mode=index or mode=send. Sets the folder for the chunk index.")
var sendArchivePath = flag.String("send-archive-path", "", "The path to the archive to create which contains the chunks missing from the remote system. (default is stdout)")

// main is the entry point of the program.
func main() {
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Println("Usage: file-sync [options] <folder>\n\nOptions:")
		flag.PrintDefaults()
		os.Exit(1)
	}

	inputPath := flag.Arg(0)

	var info os.FileInfo
	var err error

	if info, err = os.Stat(inputPath); os.IsNotExist(err) {
		fmt.Printf("Input path '%s' does not exist\n", inputPath)
		os.Exit(1)
	}

	if *mode == "recv" {
		chunkReceiver()
		os.Exit(0)
	}

	var chunkWaitGroup sync.WaitGroup
	chunkWaitGroup.Add(2)

	pathChannel := make(chan PathItem)
	chunkChannel := make(chan ChunkItem)
	missingChunkChannel := make(chan ChunkItem)
	go pathWalker(inputPath, info, pathChannel, &chunkWaitGroup)
	go fileChunker(pathChannel, chunkChannel, &chunkWaitGroup)

	switch *mode {
	case "index":
		chunkWaitGroup.Add(1)
		go indexUpdateStreamer(chunkChannel, &chunkWaitGroup)
	case "send":
		chunkWaitGroup.Add(2)
		go indexCompareStreamer(chunkChannel, missingChunkChannel, &chunkWaitGroup)
		go chunkSender(missingChunkChannel, &chunkWaitGroup)
	default:
		log.Fatalf("Unknown mode '%s'", *mode)
	}

	chunkWaitGroup.Wait()
}
