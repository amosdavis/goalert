package deltatable

import (
	"testing"
	"time"
)

func BenchmarkNormalize(b *testing.B) {
	rt, err := NewRowType[testRow]("alerts")
	if err != nil {
		b.Fatal(err)
	}
	owner := "bob"
	row := testRow{ID: 1, Summary: "disk full on db-1", Active: true, Created: time.Now(), Owner: &owner, Tags: []string{"db"}}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := rt.Normalize(row); err != nil {
			b.Fatal(err)
		}
	}
}
