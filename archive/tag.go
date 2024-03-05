package archive

import (
	"fmt"
)

type TagLabel string

type Tag struct {
	Name  TagLabel
	Value string
}

const (
	serverTagLabel  TagLabel = "server"
	clusterTagLabel TagLabel = "cluster"
	accountTagLabel TagLabel = "account"
	typeTagLabel    TagLabel = "artifact_type"
)

const (
	healtzArtifactType   string = "health"
	varzArtifactType     string = "variables"
	connzArtifactType    string = "connections"
	routezArtifactType   string = "routes"
	gatewayzArtifactType string = "gateways"
	leafzArtifactType    string = "leafs"
	subzArtifactType     string = "subs"
	jszArtifactType      string = "jetstream"
	accountzArtifactType string = "accounts"
	manifestArtifactType string = "manifest"
)

const (
	ManifestFileName string = "manifest.json"
)

func createFilenameFromTags(tags []*Tag) (string, error) {

	// Special files whose name is handled differently
	if len(tags) == 1 {
		tag := tags[0]

		// Manifest file
		if tag.Name == typeTagLabel && tag.Value == manifestArtifactType {
			return ManifestFileName, nil
		}
	}

	var accountTag, clusterTag, serverTag, typeTag *Tag
	for _, tag := range tags {

		if tag.Name == typeTagLabel && tag.Value == manifestArtifactType {
			return "", fmt.Errorf("cannot use internal manifest tag combined with other tags")
		}

		if tag.Name == accountTagLabel {
			if accountTag != nil {
				return "", fmt.Errorf("duplicate acount tag (values: %s and %s)", tag.Value, accountTag.Value)
			}
			accountTag = tag
		}
		if tag.Name == clusterTagLabel {
			if clusterTag != nil {
				return "", fmt.Errorf("duplicate cluster tag (values: %s and %s)", tag.Value, clusterTag.Value)
			}
			clusterTag = tag
		} else if tag.Name == serverTagLabel {
			if serverTag != nil {
				return "", fmt.Errorf("duplicate server tag (values: %s and %s)", tag.Value, serverTag.Value)
			}
			serverTag = tag
		} else if tag.Name == typeTagLabel {
			if typeTag != nil {
				return "", fmt.Errorf("duplicate artifact type tag (values: %s and %s)", tag.Value, typeTag.Value)
			}
			typeTag = tag
		}
	}

	label := "artifact"
	if accountTag != nil {
		label = fmt.Sprintf("%s__account_%s", label, accountTag.Value)
	}
	if clusterTag != nil {
		label = fmt.Sprintf("%s__cluster_%s", label, clusterTag.Value)
	}
	if serverTag != nil {
		label = fmt.Sprintf("%s__server_%s", label, serverTag.Value)
	}
	if typeTag != nil {
		label = fmt.Sprintf("%s__%s", label, typeTag.Value)
	}

	label = label + ".json"

	return label, nil
}

func TagArtifactType(artifactType string) *Tag {
	return &Tag{
		Name:  typeTagLabel,
		Value: artifactType,
	}
}

func TagHealth() *Tag {
	return TagArtifactType(healtzArtifactType)
}

func TagServerVars() *Tag {
	return TagArtifactType(varzArtifactType)
}

func TagConnections() *Tag {
	return TagArtifactType(connzArtifactType)
}

func TagRoutes() *Tag {
	return TagArtifactType(routezArtifactType)
}

func TagGateways() *Tag {
	return TagArtifactType(gatewayzArtifactType)
}

func TagLeafs() *Tag {
	return TagArtifactType(leafzArtifactType)
}

func TagSubs() *Tag {
	return TagArtifactType(subzArtifactType)
}

func TagJetStream() *Tag {
	return TagArtifactType(jszArtifactType)
}

func TagAccounts() *Tag {
	return TagArtifactType(accountzArtifactType)
}

func internalTagManifest() *Tag {
	return TagArtifactType(manifestArtifactType)
}

func TagServer(serverName string) *Tag {
	return &Tag{
		Name:  serverTagLabel,
		Value: serverName,
	}
}

func TagCluster(clusterName string) *Tag {
	return &Tag{
		Name:  clusterTagLabel,
		Value: clusterName,
	}
}

func TagAccount(accountName string) *Tag {
	return &Tag{
		Name:  accountTagLabel,
		Value: accountName,
	}
}
