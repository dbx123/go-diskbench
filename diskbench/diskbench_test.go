package diskbench

import (
	"testing"
)

func TestBenchDisk(t *testing.T) {

	tests := map[string]struct {
		bench     DiskBench
		expectErr bool
	}{
		"2 second bench": {
			bench: DiskBench{
				Folder:  "./bench",
				Seconds: 2,
			},
		},
		"empty duration": {
			expectErr: true,
		},
	}

	for name, tt := range tests {

		_, err := BenchDisk(tt.bench)

		if tt.expectErr && err == nil {
			t.Fatalf("%v: expected error, got %v", name, err)
		}
	}
}
