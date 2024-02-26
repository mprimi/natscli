package archive

import (
	"path/filepath"
	"testing"
)

func TestArchiveCreateThenRead(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "archive.zip")
	aw, err := NewWriter(archivePath)
	if err != nil {
		t.Fatalf("Failed to create archive: %s", err)
	}

	// TODO add some artifacts...

	files := map[string][]byte{
		"empty_file.txt": make([]byte, 0),            // Zero-length file
		"small_file.bin": make([]byte, 0, 2048),      // 2 KiB
		"2MB_file.bin":   make([]byte, 0, 2048*1024), // 2 MiB
	}

	for fileName, fileContent := range files {
		err = aw.AddArtifact(fileName, fileContent)
		if err != nil {
			t.Fatalf("Failed to add file '%s': %s", fileName, err)
		}
	}

	err = aw.Close()
	if err != nil {
		t.Fatalf("Error closing writer: %s", err)
	}

	ar, err := NewReader(archivePath)
	defer ar.Close()
	if err != nil {
		t.Fatalf("Failed to create archive: %s", err)
	}

	expectedArtifactsCount := len(files)
	if expectedArtifactsCount != ar.artifactsCount() {
		t.Fatalf("Wrong number of artifacts. Expected: %d actual: %d", expectedArtifactsCount, ar.artifactsCount())
	}
}

// TODO test writer overwrites existing file
// TODO test creation in non-existing directory fails
// TODO test adding twice a file with the same namew
