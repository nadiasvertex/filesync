package main

//go:generate protoc --go_out=. --go_opt=paths=source_relative --proto_path=. syncstream/syncstream.proto

import (
	"archive/zip"
	"bufio"
	"compress/flate"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	cdc "github.com/jotfs/fastcdc-go"
	"github.com/nadiasvertex/filesync/syncstream"
	"github.com/pierrec/lz4"
	flag "github.com/spf13/pflag"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
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

// PathItem represents a file path item.
type PathItem struct {
	BaseDir  string
	FullPath string
	RelPath  string
}

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

// getCompressor returns a WriteCloser for the specified compression format.
func getCompressor(outputF *os.File) io.WriteCloser {
	switch *compression {
	case "none":
		return outputF
	case "flate":
		outStream, err := flate.NewWriter(outputF, flate.BestCompression)
		if err != nil {
			log.Fatalf("Error creating deflate writer: %v", err)
		}
		return outStream
	case "gzip":
		return gzip.NewWriter(outputF)
	case "lz4":
		return lz4.NewWriter(outputF)
	default:
		fmt.Printf("Unsupported compression format: %s\n", *compression)
		os.Exit(1)
	}

	return nil
}

// getDecompressor returns a Reader for the specified compression format.
func getDecompressor(in io.Reader) io.Reader {
	switch *compression {
	case "none":
		return in
	case "flate":
		return flate.NewReader(in)
	case "gzip":
		reader, err := gzip.NewReader(in)
		if err != nil {
			log.Fatalf("Error creating gzip reader: %v", err)
		}
		return reader
	case "lz4":
		return lz4.NewReader(in)
	default:
		fmt.Printf("Unsupported compression format: %s\n", *compression)
		os.Exit(1)
	}

	return nil
}

// chunkToKey converts a ChunkItem to a key string.
func chunkToKey(chunk ChunkItem) []byte {
	key := fmt.Sprintf("%s:%d", chunk.Path, chunk.Content.Offset)
	return []byte(key)
}

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

// Read settings from the archive.
func readSettings(archive *zip.Reader) *syncstream.Settings {
	settings, err := archive.Open("settings")
	if err != nil {
		log.Fatalf("error opening settings file in archive: %v", err)
	}
	defer settings.Close()

	settingsData, err := io.ReadAll(settings)
	if err != nil {
		log.Fatalf("error reading settings file in archive: %v", err)
	}

	var settingsInfo syncstream.Settings
	if err := proto.Unmarshal(settingsData, &settingsInfo); err != nil {
		log.Fatalf("error unmarshaling settings: %v", err)
	}

	if *verbose {
		log.Printf("Settings read from archive: %+v", &settingsInfo)
	}

	return &settingsInfo
}

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

// chunkReceiver reads chunks from the archive and sends them to the receiver for patching.
func chunkReceiver() {
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
