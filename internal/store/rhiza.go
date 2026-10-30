package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mrchypark/rhiza"
)

// OpenRhiza starts an embedded Rhiza SQL node. No public database listener is opened.
func OpenRhiza(ctx context.Context, config rhiza.Config) (*Store, error) {
	config = withComposGraphIndexes(config)
	cluster, err := rhiza.Open(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open embedded rhiza: %w", err)
	}
	s := &Store{path: config.DataDir, kind: "rhiza", cluster: config.ClusterID, rhiza: cluster}
	s.close = cluster.Close
	for {
		err = s.initializeRhiza(ctx)
		if err == nil {
			break
		}
		if len(config.Members) <= 1 || (!errors.Is(err, rhiza.ErrNotReady) &&
			!errors.Is(err, rhiza.ErrQuorumUnavailable) &&
			!errors.Is(err, rhiza.ErrDurabilityUnavailable) &&
			!errors.Is(err, rhiza.ErrCommitUnknown)) {
			s.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			s.Close()
			return nil, errors.Join(err, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	return s, nil
}

func withComposGraphIndexes(config rhiza.Config) rhiza.Config {
	indexes := append([]rhiza.GraphNodePropertyIndex(nil), config.LocalGraphNodePropertyIndexes...)
	seen := make(map[rhiza.GraphNodePropertyIndex]struct{}, len(indexes)+3)
	for _, index := range indexes {
		seen[index] = struct{}{}
	}
	for _, label := range []string{"ComposConcept", "ComposEntityDefinition", "ComposProjectionState"} {
		index := rhiza.GraphNodePropertyIndex{Label: label, Property: "projection_key"}
		if _, ok := seen[index]; ok {
			continue
		}
		indexes = append(indexes, index)
		seen[index] = struct{}{}
	}
	config.LocalGraphNodePropertyIndexes = indexes
	return config
}

func (s *Store) initializeRhiza(ctx context.Context) error {
	var identity string
	err := s.ReadRow(ctx, ReadLinearizable, `SELECT identity FROM schema_metadata WHERE id = 1`).Scan(&identity)
	if strings.Contains(fmt.Sprint(err), "no such table") {
		migrations, splitErr := rhizaSchemaMigrations()
		if splitErr != nil {
			return fmt.Errorf("split Rhiza schema: %w", splitErr)
		}
		if err := s.rhiza.Migrate(ctx, migrations); err != nil {
			return fmt.Errorf("initialize Rhiza schema: %w", err)
		}
		err = s.ReadRow(ctx, ReadLinearizable, `SELECT identity FROM schema_metadata WHERE id = 1`).Scan(&identity)
	}
	if errors.Is(err, sql.ErrNoRows) || strings.Contains(fmt.Sprint(err), "no such table") {
		return fmt.Errorf("refusing legacy or partially initialized Rhiza database without schema identity: %w", err)
	}
	if err != nil {
		return fmt.Errorf("read Rhiza schema identity: %w", err)
	}
	if identity != SchemaIdentity {
		return fmt.Errorf("unsupported Rhiza schema identity %q", identity)
	}
	return nil
}

func rhizaSchemaMigrations() ([]rhiza.Migration, error) {
	statements, err := splitSchema()
	if err != nil {
		return nil, err
	}
	if len(statements) == 0 || !strings.HasPrefix(statements[0], "CREATE TABLE schema_metadata (") {
		return nil, errors.New("schema must start with schema_metadata")
	}
	var migrations []rhiza.Migration
	for start := 1; start < len(statements); start += 64 {
		var batch []rhiza.SQLStatement
		for _, statement := range statements[start:min(start+64, len(statements))] {
			batch = append(batch, rhiza.SQLStatement{SQL: statement})
		}
		migrations = append(migrations, rhiza.Migration{
			Version: int64(len(migrations) + 1), Name: fmt.Sprintf("compos-schema-v1-%d", len(migrations)+1), Statements: batch,
		})
	}
	// Publish readiness only after all DDL, with table and identity in one transaction.
	migrations = append(migrations, rhiza.Migration{
		Version: int64(len(migrations) + 1), Name: fmt.Sprintf("compos-schema-v1-%d", len(migrations)+1),
		Statements: []rhiza.SQLStatement{
			{SQL: statements[0]},
			{SQL: `INSERT INTO schema_metadata(id, identity, applied_at) VALUES (1, '` + SchemaIdentity + `', '1970-01-01T00:00:00Z')`},
		},
	})
	return migrations, nil
}

func (s *Store) GraphAvailable() bool { return s.rhiza != nil }

func (s *Store) GraphExecute(ctx context.Context, requestID, cypher string, args map[string]any) error {
	if s.rhiza == nil {
		return errors.New("embedded Rhiza graph is unavailable")
	}
	response, err := s.rhiza.GraphExecute(ctx, rhiza.GraphCommand{RequestID: compactSQLRequestID(requestID), Cypher: cypher, Args: args})
	if err != nil {
		return err
	}
	if response.Status != "committed" {
		return fmt.Errorf("commit rhiza graph mutation rejected: status=%s code=%s", response.Status, response.ErrorCode)
	}
	return nil
}

func (s *Store) GraphQuery(ctx context.Context, cypher string, args map[string]any) (rhiza.GraphResult, error) {
	if s.rhiza == nil {
		return rhiza.GraphResult{}, errors.New("embedded Rhiza graph is unavailable")
	}
	return s.rhiza.GraphQuery(ctx, rhiza.GraphQueryRequest{Cypher: cypher, Args: args, Consistency: rhiza.ConsistencyLocal})
}

func (s *Store) GraphReachable(ctx context.Context, request rhiza.GraphReachableRequest) (rhiza.GraphReachableResult, error) {
	if s.rhiza == nil {
		return rhiza.GraphReachableResult{}, errors.New("embedded Rhiza graph is unavailable")
	}
	return s.rhiza.GraphReachable(ctx, request)
}

func compactSQLRequestID(requestID string) string {
	if requestID == "" {
		requestID = uuid.NewString()
	}
	if len(requestID) <= 64 {
		return requestID
	}
	sum := sha256.Sum256([]byte(requestID))
	return hex.EncodeToString(sum[:])
}
