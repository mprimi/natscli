package archive

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type Reader struct {
	archiveReader *zip.ReadCloser
	path          string
	filesMap      map[string]*zip.File
	manifestMap   map[string][]Tag
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

func (r *Reader) Get(name string, v any) error {
	f, _, err := r.GetFile(name)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(f)
	err = decoder.Decode(v)
	if err != nil {
		return fmt.Errorf("failed to decode: %w", err)
	}
	return nil
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

	manifestMap := make(map[string][]Tag, len(filesMap))
	manifestFileName, err := createFilenameFromTags([]*Tag{internalTagManifest()})
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}

	manifestFile, exists := filesMap[manifestFileName]
	if !exists {
		return nil, fmt.Errorf("manifest file not found in archive")
	}

	manifestFileReader, err := manifestFile.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open manifest: %w", err)
	}

	err = json.NewDecoder(manifestFileReader).Decode(&manifestMap)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}

	for fileName, _ := range manifestMap {
		_, present := filesMap[fileName]
		if !present {
			return nil, fmt.Errorf("file %s is in manifest, but not present in archive", fileName)
		}
	}

	for fileName, _ := range filesMap {
		if fileName == ManifestFileName {
			// Manifest is not present in manifest
			continue
		}

		_, present := manifestMap[fileName]
		if !present {
			fmt.Printf("Warning: archive file %s is not present in manifest\n", fileName)
		}
	}

	return &Reader{
		path:          archivePath,
		archiveReader: archiveReader,
		filesMap:      filesMap,
		manifestMap:   manifestMap,
	}, nil
}
