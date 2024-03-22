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
	"testing"
)

func Test_CreateFilenameFromTags(t *testing.T) {

	tests := []struct {
		name    string
		tags    []*Tag
		want    string
		wantErr bool
	}{
		{
			"server health",
			[]*Tag{TagCluster("C1"), TagServer("S1"), TagHealth()},
			"capture/clusters/C1/server_S1__health.json",
			false,
		},
		{
			"server info",
			[]*Tag{TagCluster("C1"), TagServer("S1"), TagServerVars()},
			"capture/clusters/C1/server_S1__variables.json",
			false,
		},
		{
			"server profile",
			[]*Tag{TagCluster("C1"), TagServer("S1"), TagServerProfile(), TagProfileName("foo")},
			"capture/clusters/C1/profiles/server_S1__profile_foo.prof",
			false,
		},
		{
			"server profile with missing name",
			[]*Tag{TagCluster("C1"), TagServer("S1"), TagServerProfile()},
			"",
			true,
		},
		{
			"cluster info",
			[]*Tag{TagCluster("C1"), TagServer("S1"), TagArtifactType("cluster_info")},
			"capture/clusters/C1/server_S1__cluster_info.json",
			false,
		},
		{
			"account connections",
			[]*Tag{TagAccount("A1"), TagServer("S1"), TagConnections()},
			"capture/accounts/A1/server_S1__connections.json",
			false,
		},
		{
			"account connections without source server",
			[]*Tag{TagAccount("A1"), TagConnections()},
			"",
			true,
		},
		{
			"stream info",
			[]*Tag{TagAccount("A1"), TagStream("Foo"), TagServer("S1"), TagCluster("C1"), TagArtifactType("stream_info")},
			"capture/accounts/A1/streams/Foo/server_S1__stream_info.json",
			false,
		},
		{
			"stream info without type",
			[]*Tag{TagAccount("A1"), TagStream("Foo"), TagCluster("C1"), TagServer("S1")},
			"",
			true,
		},
		{
			"stream info without source server",
			[]*Tag{TagAccount("A1"), TagStream("Foo"), TagCluster("C1"), TagArtifactType("stream_info")},
			"",
			true,
		},
		{
			"stream info without account server",
			[]*Tag{TagServer("S1"), TagStream("Foo"), TagNoCluster(), TagArtifactType("stream_info")},
			"",
			true,
		},
		{
			"manifest",
			[]*Tag{internalTagManifest()},
			"capture/manifest.json",
			false,
		},
		{
			"manifest with other tag",
			[]*Tag{internalTagManifest(), TagServer("foo")},
			"",
			true,
		},
		{
			"no tags",
			[]*Tag{},
			"",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := createFilenameFromTags(tt.tags)
			if (err != nil) != tt.wantErr {
				t.Errorf("createFilenameFromTags() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("createFilenameFromTags() got = %v, want %v", got, tt.want)
			}
		})
	}
}
