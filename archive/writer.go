package archive

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"archive/zip"
)

type Writer struct {
	path       string
	fileWriter *os.File
	zipWriter  *zip.Writer
}

func (w *Writer) Close() error {

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

func NewWriter(archivePath string) (*Writer, error) {
	fileWriter, err := os.Create(archivePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create archive: %w", err)
	}

	zipWriter := zip.NewWriter(fileWriter)

	return &Writer{
		path:       archivePath,
		fileWriter: fileWriter,
		zipWriter:  zipWriter,
	}, nil
}
