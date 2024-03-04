package archive

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
)

type Reader struct {
	archiveReader *zip.ReadCloser
	path          string
	filesMap      map[string]*zip.File
}

func (r *Reader) artifactsCount() int {
	return len(r.archiveReader.File)
}

func (r *Reader) Close() error {
	if r.archiveReader != nil {
		err := r.archiveReader.Close()
		r.archiveReader = nil
		return err
	}
	return nil
}

func (r *Reader) GetFile(name string) (io.ReadCloser, uint64, error) {
	f, exists := r.filesMap[name]
	if !exists {
		return nil, 0, os.ErrNotExist
	}
	reader, err := f.Open()
	if err != nil {
		return nil, 0, err
	}
	return reader, f.UncompressedSize64, nil
}

func NewReader(archivePath string) (*Reader, error) {

	archiveReader, err := zip.OpenReader(archivePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open archive: %w", err)
	}

	filesMap := make(map[string]*zip.File, len(archiveReader.File))
	for _, f := range archiveReader.File {
		filesMap[f.Name] = f
	}

	return &Reader{
		path:          archivePath,
		archiveReader: archiveReader,
		filesMap:      filesMap,
	}, nil
}
