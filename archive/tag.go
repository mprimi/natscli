// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	serverTagLabel      TagLabel = "server"
	clusterTagLabel     TagLabel = "cluster"
	accountTagLabel     TagLabel = "account"
	streamTagLabel      TagLabel = "stream"
	typeTagLabel        TagLabel = "artifact_type"
	profileNameTagLabel TagLabel = "profile_name"
)

const (
	// Server artifacts
	healtzArtifactType   = "health"
	varzArtifactType     = "variables"
	connzArtifactType    = "connections"
	routezArtifactType   = "routes"
	gatewayzArtifactType = "gateways"
	leafzArtifactType    = "leafs"
	subzArtifactType     = "subs"
	jszArtifactType      = "jetstream_info"
	accountzArtifactType = "accounts"
	// Account artifacts
	accountConnectionsArtifactType = "account_connections"
	accountLeafsArtifactType       = "account_leafs"
	accountSubsArtifactType        = "account_subs"
	accountJetStreamArtifactType   = "account_jetstream_info"
	accountInfoArtifactType        = "account_info"
	streamDetailsArtifactType      = "stream_info"
	// Other artifacts
	manifestArtifactType = "manifest"
	profileArtifactType  = "profile"
)

const (
	ManifestFileName = "capture/manifest.json"
	NoCluster        = "unclustered"
)

const rootPrefix = "capture/"
const separator = "__"
const captureLogName = rootPrefix + "capture.log"
const metadataName = rootPrefix + "capture_info.json"

// Special tag that result in a special file path
var specialFilesTagMap = map[Tag]string{
	*internalTagManifest(): rootPrefix + "manifest.json",
}

// Special tags that get composed and combined in the filename
var dimensionTagsNames = map[TagLabel]interface{}{
	accountTagLabel:     nil,
	clusterTagLabel:     nil,
	serverTagLabel:      nil,
	streamTagLabel:      nil,
	typeTagLabel:        nil,
	profileNameTagLabel: nil,
}

func createFilenameFromTags(tags []*Tag) (string, error) {
	if len(tags) < 1 {
		return "", fmt.Errorf("at least one tag is required")
	} else if len(tags) == 1 {
		// Single tag provided, is it one that has a special handling?
		tag := tags[0]
		fileName, isSpecialTag := specialFilesTagMap[*tag]
		if isSpecialTag {
			// Short-circuit and return the matching filename
			return fileName, nil
		}
	}

	// "Dimension" tags are special:
	// - Can have at most one value
	// - They get combined to produce the file path
	dimensionTagsMap := make(map[TagLabel]*Tag, len(tags))

	// Capture non-dimension tags here (unused for now)
	otherTags := make([]*Tag, 0, len(tags))

	for _, tag := range tags {

		// The 'special' tags should not be mixed with the rest
		if _, present := specialFilesTagMap[*tag]; present {
			return "", fmt.Errorf("tag '%s' is special and should not be combined with other tags", tag.Name)
		}

		// Save dimension tags and other tags
		_, isDimensionTag := dimensionTagsNames[tag.Name]
		_, isDuplicateDimensionTag := dimensionTagsMap[tag.Name]
		if isDimensionTag && isDuplicateDimensionTag {
			return "", fmt.Errorf("multiple values not allowed for tag '%s'", tag.Name)
		} else if isDimensionTag {
			dimensionTagsMap[tag.Name] = tag
		} else {
			otherTags = append(otherTags, tag)
		}
	}

	if len(otherTags) > 0 {
		// TODO for the moment, the gather command is the only user, and it is not custom tags.
		// If we ever open the archiving API beyond, we may need to address this.
		panic(fmt.Sprintf("Unhandled tags: %+v", otherTags))
	}

	accountTag, hasAccountTag := dimensionTagsMap[accountTagLabel], dimensionTagsMap[accountTagLabel] != nil
	clusterTag, hasClusterTag := dimensionTagsMap[clusterTagLabel], dimensionTagsMap[clusterTagLabel] != nil
	serverTag, hasServerTag := dimensionTagsMap[serverTagLabel], dimensionTagsMap[serverTagLabel] != nil
	streamTag, hasStreamTag := dimensionTagsMap[streamTagLabel], dimensionTagsMap[streamTagLabel] != nil
	typeTag, hasTypeTag := dimensionTagsMap[typeTagLabel], dimensionTagsMap[typeTagLabel] != nil
	profileNameTag, hasProfileNameTag := dimensionTagsMap[profileNameTagLabel], dimensionTagsMap[profileNameTagLabel] != nil

	var name string

	// All artifacts must have a type, source server and source cluster (or "un-clustered")
	for requiredTagName, hasRequiredTag := range map[string]bool{
		"artifact type":  hasTypeTag,
		"source cluster": hasClusterTag,
		"source server":  hasServerTag,
	} {
		if !hasRequiredTag {
			return "", fmt.Errorf("missing required tag: %s", requiredTagName)
		}
	}

	fileExtension := ".json"

	if hasStreamTag {
		// Stream artifact must have account and cluster tag
		if !hasClusterTag || !hasAccountTag {
			return "", fmt.Errorf("stream artifact is missing cluster or account tags")
		}

		name = fmt.Sprintf("accounts/%s/streams/%s/replicas/%s__%s/%s", accountTag.Value, streamTag.Value, clusterTag.Value, serverTag.Value, typeTag.Value)

	} else if hasAccountTag {
		// Account artifact (but not a stream)
		if !hasClusterTag {
			return "", fmt.Errorf("account artifact is missing cluster tag")
		}
		name = fmt.Sprintf("accounts/%s/servers/%s__%s/%s", accountTag.Value, clusterTag.Value, serverTag.Value, typeTag.Value)

	} else if hasServerTag {
		// Server artifact
		clusterName := NoCluster
		if hasClusterTag {
			clusterName = clusterTag.Value
		}

		// Handle certain types differently
		switch typeTag.Value {
		case profileArtifactType:
			if !hasProfileNameTag {
				return "", fmt.Errorf("profile artifact is missing profile name")
			}
			fileExtension = ".prof"
			name = fmt.Sprintf("profiles/%s/%s__%s", clusterName, serverTag.Value, profileNameTag.Value)

		default:
			name = fmt.Sprintf("clusters/%s/%s/%s", clusterName, serverTag.Value, typeTag.Value)
		}

	} else {
		// TODO may add more cases later, for now bomb if none of the above applies
		panic(fmt.Sprintf("Unhandled tags combination: %+v", dimensionTagsMap))
	}

	name = rootPrefix + name + fileExtension

	return name, nil
}

func TagArtifactType(artifactType string) *Tag {
	return &Tag{
		Name:  typeTagLabel,
		Value: artifactType,
	}
}

func TagServerHealth() *Tag {
	return TagArtifactType(healtzArtifactType)
}
func TagServerVars() *Tag {
	return TagArtifactType(varzArtifactType)
}

func TagServerConnections() *Tag {
	return TagArtifactType(connzArtifactType)
}

func TagServerRoutes() *Tag {
	return TagArtifactType(routezArtifactType)
}

func TagServerGateways() *Tag {
	return TagArtifactType(gatewayzArtifactType)
}

func TagServerLeafs() *Tag {
	return TagArtifactType(leafzArtifactType)
}

func TagServerSubs() *Tag {
	return TagArtifactType(subzArtifactType)
}

func TagServerJetStream() *Tag {
	return TagArtifactType(jszArtifactType)
}

func TagServerAccounts() *Tag {
	return TagArtifactType(accountzArtifactType)
}

func TagAccountConnections() *Tag {
	return TagArtifactType(accountConnectionsArtifactType)
}

func TagAccountLeafs() *Tag {
	return TagArtifactType(accountLeafsArtifactType)
}

func TagAccountSubs() *Tag {
	return TagArtifactType(accountSubsArtifactType)
}

func TagAccountJetStream() *Tag {
	return TagArtifactType(accountJetStreamArtifactType)
}

func TagAccountInfo() *Tag {
	return TagArtifactType(accountInfoArtifactType)
}

func TagStreamInfo() *Tag { return TagArtifactType(streamDetailsArtifactType) }

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

func TagNoCluster() *Tag {
	return &Tag{
		Name:  clusterTagLabel,
		Value: NoCluster,
	}
}

func TagAccount(accountName string) *Tag {
	return &Tag{
		Name:  accountTagLabel,
		Value: accountName,
	}
}

func TagStream(streamName string) *Tag {
	return &Tag{
		Name:  streamTagLabel,
		Value: streamName,
	}
}

func TagServerProfile() *Tag {
	return TagArtifactType(profileArtifactType)
}

func TagProfileName(profileType string) *Tag {
	return &Tag{
		Name:  profileNameTagLabel,
		Value: profileType,
	}
}
