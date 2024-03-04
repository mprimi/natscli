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
	typeTagLabel    TagLabel = "artifact_type"
)

func createFilenameFromTags(tags []*Tag) (string, error) {
	var clusterTag, serverTag, typeTag *Tag
	for _, tag := range tags {
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

func ClusterInfoTag() *Tag {
	return &Tag{
		Name:  typeTagLabel,
		Value: "cluster_info",
	}
}

func ServerInfoTag() *Tag {
	return &Tag{
		Name:  typeTagLabel,
		Value: "server_info",
	}
}

func ServerHealthTag() *Tag {
	return &Tag{
		Name:  typeTagLabel,
		Value: "health",
	}
}

func ServerTag(serverName string) *Tag {
	return &Tag{
		Name:  serverTagLabel,
		Value: serverName,
	}
}

func ClusterTag(clusterName string) *Tag {
	return &Tag{
		Name:  clusterTagLabel,
		Value: clusterName,
	}
}
