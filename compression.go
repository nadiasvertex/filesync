package main

import (
	"compress/flate"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/pierrec/lz4"
)

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
