package kernel

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza"
)

func TestCompactWorkGuardsPreservesExactCounts(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "compact-work-guards")
	defer s.Close()
	one, two, zero := int64(1), int64(2), int64(0)
	guards := []rhiza.SQLStatement{
		{SQL: `SELECT ? UNION ALL SELECT ?`, Args: []any{1, 2}, WantRows: true, ExpectedReturnedRows: &two},
		{SQL: `SELECT 1 WHERE 0`, WantRows: true, ExpectedReturnedRows: &zero},
	}
	if err := s.ApplyStatements(ctx, "compact-pass", compactWorkGuards(guards)); err != nil {
		t.Fatal(err)
	}
	guards[0].ExpectedReturnedRows = &one
	if err := s.ApplyStatements(ctx, "compact-fail", compactWorkGuards(guards)); err == nil {
		t.Fatal("row count guard was weakened to existence")
	}
}
