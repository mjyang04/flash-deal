package shardkey

import "testing"

func TestDBIndex_Stable(t *testing.T) {
	cases := []struct {
		userID  int64
		dbCount int
		want    int
	}{
		{0, 4, 0},
		{1, 4, 1},
		{4, 4, 0},
		{99, 4, 3},
		{0, 0, 0}, // defensive
	}
	for _, tc := range cases {
		got := DBIndex(tc.userID, tc.dbCount)
		if got != tc.want {
			t.Errorf("DBIndex(%d, %d) = %d, want %d", tc.userID, tc.dbCount, got, tc.want)
		}
	}
}

func TestTableIndex_Stable(t *testing.T) {
	got := TableIndex(99, 4, 16)
	want := int((99 / 4) % 16)
	if got != want {
		t.Errorf("TableIndex(99,4,16) = %d, want %d", got, want)
	}
}
