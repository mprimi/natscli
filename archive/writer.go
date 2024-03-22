// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package archive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"archive/zip"
)

type Writer struct {
	path        string
	fileWriter  *os.File
	zipWriter   *zip.Writer
	manifestMap map[string][]*Tag
}

func (w *Writer) Close() error {
	// Add manifest file to archive before closing it
	if w.zipWriter != nil && w.fileWriter != nil {
		err := w.Add(w.manifestMap, internalTagManifest())
		if err != nil {
			return fmt.Errorf("failed to add manifest")
		}
	}

	// Close and null the zip writer
	if w.zipWriter != nil {
		err := w.zipWriter.Close()
		w.zipWriter = nil
		if err != nil {
			return fmt.Errorf("failed to close archive zip writer: %w", err)
		}
	}

	// Close and null the file writer
	if w.fileWriter != nil {
		err := w.fileWriter.Close()
		w.fileWriter = nil
		if err != nil {
			return fmt.Errorf("failed to close archive file writer: %w", err)
		}
	}

	return nil
}

// addArtifact is a low-level API that adds bytes without adding to the index.
// In most cases, don't use this and use Add instead.
func (w *Writer) addArtifact(name string, content *bytes.Reader) error {
	f, err := w.zipWriter.Create(name)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, content)
	if err != nil {
		return err
	}

	return nil
}

// Add serializes the given artifact and adds it to the archive, it creates a file name based on the provided tags
// and ensures uniqueness. The artifact is also added to the manifest for indexing, enabling tag-based querying
// in the reader
func (w *Writer) Add(artifact any, tags ...*Tag) error {
	// Encode the artifact as (pretty-formatted) JSON
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetIndent("", "  ")
	err := encoder.Encode(artifact)
	if err != nil {
		return fmt.Errorf("failed to encode: %w", err)
	}

	return w.AddObject(bytes.NewReader(buf.Bytes()), tags...)
}

// AddObject adds the given artifact bytes as-is
func (w *Writer) AddObject(reader *bytes.Reader, tags ...*Tag) error {
	if w.zipWriter == nil {
		return fmt.Errorf("attempting to write into a closed writer")
	}

	// Create filename based on tags
	name, err := createFilenameFromTags(tags)
	if err != nil {
		return fmt.Errorf("failed to create artifact name: %w", err)
	}

	// Ensure file is unique
	_, exists := w.manifestMap[name]
	if exists {
		return fmt.Errorf("artifact %s with identical tags is already present", name)
	}

	// Open a zip writer
	f, err := w.zipWriter.Create(name)
	if err != nil {
		return fmt.Errorf("failed to create file in archive: %w", err)
	}

	_, err = io.Copy(f, reader)
	if err != nil {
		return fmt.Errorf("failed to copy content: %w", err)
	}

	// Add file and its tags to the manifest
	w.manifestMap[name] = tags

	return nil
}

func (w *Writer) AddCaptureLog(reader *bytes.Reader) error {
	return w.addArtifact(captureLogName, reader)
}

func (w *Writer) AddCaptureMetadata(metadata any) error {
	encoded, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}
	return w.addArtifact(metadataName, bytes.NewReader(encoded))
}

func NewWriter(archivePath string) (*Writer, error) {
	fileWriter, err := os.Create(archivePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create archive: %w", err)
	}

	zipWriter := zip.NewWriter(fileWriter)

	return &Writer{
		path:        archivePath,
		fileWriter:  fileWriter,
		zipWriter:   zipWriter,
		manifestMap: make(map[string][]*Tag),
	}, nil
}
