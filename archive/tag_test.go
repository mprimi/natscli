package archive

import (
	"testing"
)

func Test_createFilenameFromTags(t *testing.T) {

	tests := []struct {
		name    string
		tags    []*Tag
		want    string
		wantErr bool
	}{
		{
			"server health",
			[]*Tag{ClusterTag("C1"), ServerTag("S1"), ServerHealthTag()},
			"artifact__cluster_C1__server_S1__health.json",
			false,
		},
		{
			"server info",
			[]*Tag{ClusterTag("C1"), ServerTag("S1"), ServerInfoTag()},
			"artifact__cluster_C1__server_S1__server_info.json",
			false,
		},
		{
			"cluster info",
			[]*Tag{ClusterTag("C1"), ClusterInfoTag()},
			"artifact__cluster_C1__cluster_info.json",
			false,
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
