package marketplace

import (
	"context"
	"encoding/json"
	"fmt"
)

// Projection is one committed Postgres result awaiting a Cassandra update.
type Projection struct {
	ID      int64
	JobID   string
	Kind    string
	Payload json.RawMessage
}

// NextProjection always returns the oldest pending row. The deployment has
// one replica so a later update cannot overtake an earlier one.
func (s *Store) NextProjection(ctx context.Context) (Projection, error) {
	var p Projection
	err := s.db.QueryRowContext(ctx, `
		SELECT id, job_id, kind, payload
		FROM projection_outbox WHERE delivered_at IS NULL
		ORDER BY id LIMIT 1`,
	).Scan(&p.ID, &p.JobID, &p.Kind, &p.Payload)
	return p, err
}

func (s *Store) MarkProjectionDelivered(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE projection_outbox SET delivered_at = CURRENT_TIMESTAMP
		WHERE id = $1 AND delivered_at IS NULL`, id)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("projection %d was not pending", id)
	}
	return nil
}
