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

	err := w.Add(w.manifestMap, internalTagManifest())
	if err != nil {
		return fmt.Errorf("failed to add manifest")
	}

	if w.zipWriter != nil {
		err := w.zipWriter.Close()
		w.zipWriter = nil
		if err != nil {
			return fmt.Errorf("failed to close archive zip writer: %w", err)
		}
	}

	if w.fileWriter != nil {
		err := w.fileWriter.Close()
		w.fileWriter = nil
		if err != nil {
			return fmt.Errorf("failed to close archive file writer: %w", err)
		}
	}

	// TODO add index

	return nil
}

func (w *Writer) AddArtifact(name string, content *bytes.Reader) error {
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

func (w *Writer) Add(artifact any, tags ...*Tag) error {
	name, err := createFilenameFromTags(tags)
	if err != nil {
		return fmt.Errorf("failed to create artifact name: %w", err)
	}

	_, exists := w.manifestMap[name]
	if exists {
		return fmt.Errorf("artifact with identical tags is already present")
	}

	f, err := w.zipWriter.Create(name)
	if err != nil {
		return fmt.Errorf("failed to create file in archive: %w", err)
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(artifact)
	if err != nil {
		return fmt.Errorf("failed to encode: %w", err)
	}

	w.manifestMap[name] = tags

	return nil
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
