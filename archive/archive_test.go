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

func TestArchiveCreateThenReadWithTags(t *testing.T) {
	const SEED = 123456
	rng := rand.New(rand.NewSource(SEED))

	archivePath := filepath.Join(t.TempDir(), "archive.zip")
	aw, err := NewWriter(archivePath)
	if err != nil {
		t.Fatalf("Failed to create archive: %s", err)
	}

	clusters := map[string][]string{
		"C1": {
			"S1",
			"S2",
			"S3",
		},
		"C2": {
			"S1",
			"S2",
			"S3",
			"S4",
			"S5",
		},
	}

	type DummyRecord struct {
		FooString string
		BarInt    int
		BazBytes  []byte
	}

	type DummyHealthStats DummyRecord
	type DummyClusterInfo DummyRecord
	type DummyServerInfo DummyRecord

	for clusterName, clusterServers := range clusters {

		var err error
		// Add one (dummy) cluster info for each cluster
		ci := &DummyClusterInfo{
			FooString: clusterName,
			BarInt:    rng.Int(),
			BazBytes:  make([]byte, 100),
		}
		rng.Read(ci.BazBytes)
		err = aw.Add(ci, ClusterTag(clusterName), ClusterInfoTag())
		if err != nil {
			t.Fatalf("Failed to add cluster info: %s", err)
		}

		for _, serverName := range clusterServers {

			// Add one (dummy) health stats for each server
			hs := &DummyHealthStats{
				FooString: serverName,
				BarInt:    rng.Int(),
				BazBytes:  make([]byte, 50),
			}
			rng.Read(hs.BazBytes)

			err = aw.Add(hs, ClusterTag(clusterName), ServerTag(serverName), ServerHealthTag())
			if err != nil {
				t.Fatalf("Failed to add server health: %s", err)
			}

			// Add one (dummy) server info for each server
			si := &DummyServerInfo{
				FooString: serverName,
				BarInt:    rng.Int(),
				BazBytes:  make([]byte, 50),
			}
			rng.Read(si.BazBytes)

			err = aw.Add(si, ClusterTag(clusterName), ServerTag(serverName), ServerInfoTag())
			if err != nil {
				t.Fatalf("Failed to add server health: %s", err)
			}
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

	expectedFilesList := []string{
		"artifact__cluster_C1__server_S1__health.json",
		"artifact__cluster_C1__server_S2__health.json",
		"artifact__cluster_C1__server_S3__health.json",
		"artifact__cluster_C2__server_S1__health.json",
		"artifact__cluster_C2__server_S2__health.json",
		"artifact__cluster_C2__server_S3__health.json",
		"artifact__cluster_C2__server_S4__health.json",
		"artifact__cluster_C2__server_S5__health.json",

		"artifact__cluster_C1__server_S1__server_info.json",
		"artifact__cluster_C1__server_S2__server_info.json",
		"artifact__cluster_C1__server_S3__server_info.json",
		"artifact__cluster_C2__server_S1__server_info.json",
		"artifact__cluster_C2__server_S2__server_info.json",
		"artifact__cluster_C2__server_S3__server_info.json",
		"artifact__cluster_C2__server_S4__server_info.json",
		"artifact__cluster_C2__server_S5__server_info.json",

		"artifact__cluster_C1__cluster_info.json",
		"artifact__cluster_C2__cluster_info.json",
	}
	expectedArtifactsCount := len(expectedFilesList)
	if len(expectedFilesList) != ar.artifactsCount() {
		t.Fatalf("Wrong number of artifacts. Expected: %d actual: %d", expectedArtifactsCount, ar.artifactsCount())
	}

	t.Logf("Listing archive contents:")
	for fileName, _ := range ar.filesMap {
		t.Logf(" - %s", fileName)
	}

	for _, fileName := range expectedFilesList {
		var r DummyRecord
		err := ar.Get(fileName, &r)
		if err != nil {
			t.Fatalf("Failed to load artifact: %s: %s", fileName, err)
		}
		//t.Logf("%s: %+v", fileName, r)
		if r.FooString == "" {
			t.Fatalf("Unexpected empty structure field for file %s", fileName)
		}
	}
}

// TODO test writer overwrites existing file
// TODO test creation in non-existing directory fails
// TODO test adding twice a file with the same namew
