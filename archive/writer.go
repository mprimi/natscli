package archive

import (
	"fmt"
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

func (w *Writer) AddArtifact(name string, content []byte) error {
	f, err := w.zipWriter.Create(name)
	if err != nil {
		return err
	}

	written, err := f.Write(content)
	if err != nil {
		return err
	} else if written != len(content) {
		return fmt.Errorf("partial write %d/%dB", written, len(content))
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
