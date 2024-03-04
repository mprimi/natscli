package archive

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestArchiveCreateThenRead(t *testing.T) {
	const SEED = 123456
	rng := rand.New(rand.NewSource(SEED))

	archivePath := filepath.Join(t.TempDir(), "archive.zip")
	aw, err := NewWriter(archivePath)
	if err != nil {
		t.Fatalf("Failed to create archive: %s", err)
	}

	files := map[string][]byte{
		"empty_file.txt": make([]byte, 0),
		"2KB_file.bin":   make([]byte, 2048),
		"2MB_file.bin":   make([]byte, 2048*1024),
	}

	for fileName, fileContent := range files {
		_, err = rng.Read(fileContent)
		if err != nil {
			t.Fatalf("Failed to generate random file contents: %s", err)
		}
		err = aw.AddArtifact(fileName, bytes.NewReader(fileContent))
		if err != nil {
			t.Fatalf("Failed to add file '%s': %s", fileName, err)
		}
	}

	err = aw.Close()
	if err != nil {
		t.Fatalf("Error closing writer: %s", err)
	}

	fileInfo, err := os.Stat(archivePath)
	if err != nil {
		t.Fatalf("Failed to get archive stats: %s", err)
	}
	t.Logf("Archive file size: %d KiB", fileInfo.Size()/1024)

	ar, err := NewReader(archivePath)
	defer ar.Close()
	if err != nil {
		t.Fatalf("Failed to create archive: %s", err)
	}

	expectedArtifactsCount := len(files)
	if expectedArtifactsCount != ar.artifactsCount() {
		t.Fatalf("Wrong number of artifacts. Expected: %d actual: %d", expectedArtifactsCount, ar.artifactsCount())
	}

	for fileName, fileContent := range files {

		fileReader, size, err := ar.GetFile(fileName)
		if err != nil {
			t.Fatalf("Failed to get file: %s: %s", fileName, err)
		}
		defer fileReader.Close()

		if uint64(len(fileContent)) != size {
			t.Fatalf("File %s size mismatch: %d vs. %d", fileName, len(fileContent), size)
		}

		buf, err := io.ReadAll(fileReader)
		if err != nil {
			t.Fatalf("Failed to read content of %s: %s", fileName, err)
		}

		if !bytes.Equal(fileContent, buf) {
			t.Fatalf("File %s content mismatch", fileName)
		}

		t.Logf("Verified file %s, uncompressed size: %dB", fileName, size)
	}
}

// TODO test writer overwrites existing file
// TODO test creation in non-existing directory fails
// TODO test adding twice a file with the same namew
