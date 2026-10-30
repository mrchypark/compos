package kernel

import (
	"strings"

	"github.com/mrchypark/rhiza"
)

// Atomic Work child planners read pre-submit state. Pack their exact row-count
// assertions into a single statement without relaxing any guard or splitting
// the native transaction. Output-producing statements are never moved.
func compactWorkGuards(statements []rhiza.SQLStatement) []rhiza.SQLStatement {
	for _, statement := range statements {
		if len(statement.OutputRefs) > 0 {
			return statements
		}
	}
	var predicates []string
	var args []any
	mutations := make([]rhiza.SQLStatement, 0, len(statements))
	seen := map[string]bool{}
	for _, statement := range statements {
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement.SQL)), "SELECT") || !statement.WantRows || statement.ExpectedReturnedRows == nil || len(statement.OutputRefs) > 0 {
			mutations = append(mutations, statement)
			continue
		}
		key := canonical(statement)
		if seen[key] {
			continue
		}
		seen[key] = true
		predicates = append(predicates, `(SELECT COUNT(*) FROM (`+statement.SQL+`)) = ?`)
		args = append(args, statement.Args...)
		args = append(args, *statement.ExpectedReturnedRows)
	}
	if len(predicates) == 0 {
		return statements
	}
	one := int64(1)
	return append([]rhiza.SQLStatement{{SQL: `SELECT 1 WHERE ` + strings.Join(predicates, " AND "), Args: args, WantRows: true, ExpectedReturnedRows: &one}}, mutations...)
}
