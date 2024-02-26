package archive

import (
	"archive/zip"
	"fmt"
)

type Reader struct {
	archiveReader *zip.ReadCloser
	path          string
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

func NewReader(archivePath string) (*Reader, error) {

	archiveReader, err := zip.OpenReader(archivePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open archive: %w", err)
	}

	return &Reader{
		path:          archivePath,
		archiveReader: archiveReader,
	}, nil
}
