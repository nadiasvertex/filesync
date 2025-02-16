package main

import (
	"archive/zip"
	"io"
	"log"

	"github.com/nadiasvertex/filesync/syncstream"
	"google.golang.org/protobuf/proto"
)

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
