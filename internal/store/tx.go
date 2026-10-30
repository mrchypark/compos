package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza"
)

var (
	ErrInteractiveTransactionUnsupported = errors.New("interactive SQL transactions are unsupported by Rhiza")
	ErrMutationPreconditionFailed        = errors.New("mutation precondition failed")
	ErrMutationExecutionFailed           = errors.New("mutation execution failed")
)

// Tx is the SQL surface used by Compos transactional domain logic.
type Tx interface {
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) Transact(ctx context.Context, requestID string, fn func(Tx) error) error {
	if s.rhiza != nil {
		return ErrInteractiveTransactionUnsupported
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ApplyStatements(ctx context.Context, requestID string, statements []rhiza.SQLStatement) error {
	if s.rhiza == nil {
		return errors.New("replicated SQL statements require Rhiza storage")
	}
	response, err := s.rhiza.Execute(ctx, rhiza.ExecuteRequest{
		RequestID: compactSQLRequestID(requestID), Statements: statements,
	})
	if err != nil {
		return err
	}
	if response.Status == "committed" {
		return nil
	}
	if response.ErrorCode == rhiza.MutationErrorCodePreconditionFailed {
		return fmt.Errorf("%w: %s", ErrMutationPreconditionFailed, response.ErrorCode)
	}
	if response.ErrorCode == rhiza.MutationErrorCodeExecutionFailed {
		return fmt.Errorf("%w: %s", ErrMutationExecutionFailed, response.ErrorCode)
	}
	return fmt.Errorf("commit Rhiza statement batch rejected: status=%s code=%s", response.Status, response.ErrorCode)
}

func (s *Store) Exec(ctx context.Context, requestID, query string, args ...any) error {
	_, err := s.ExecAffected(ctx, requestID, query, args...)
	return err
}

// ExecAffected executes one write through the canonical store boundary.
func (s *Store) ExecAffected(ctx context.Context, requestID, query string, args ...any) (int64, error) {
	if s.rhiza != nil {
		response, err := s.rhiza.Execute(ctx, rhiza.ExecuteRequest{
			RequestID: compactSQLRequestID(requestID), SQL: query, Args: args,
		})
		if err != nil {
			return 0, err
		}
		if response.Status != "committed" {
			return 0, fmt.Errorf("commit rhiza statement rejected: status=%s code=%s", response.Status, response.ErrorCode)
		}
		return response.RowsAffected, nil
	}
	var rowsAffected int64
	err := s.Transact(ctx, requestID, func(tx Tx) error {
		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
		rowsAffected, err = result.RowsAffected()
		return err
	})
	return rowsAffected, err
}
