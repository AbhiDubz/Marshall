package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// PayloadStore holds the command a job executes, keyed by job ID. It
// is separate from Store because pkg/types.Job is specified exactly by
// the brief and carries no payload.
type PayloadStore interface {
	PutPayload(ctx context.Context, jobID, cmd string) error
	GetPayload(ctx context.Context, jobID string) (string, error)
}

func (m *MemStore) PutPayload(_ context.Context, jobID, cmd string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[jobID]; !ok {
		return fmt.Errorf("job %s: %w", jobID, ErrNotFound)
	}
	if m.payloads == nil {
		m.payloads = make(map[string]string)
	}
	m.payloads[jobID] = cmd
	return nil
}

func (m *MemStore) GetPayload(_ context.Context, jobID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cmd, ok := m.payloads[jobID]
	if !ok {
		return "", fmt.Errorf("payload for job %s: %w", jobID, ErrNotFound)
	}
	return cmd, nil
}

func (s *PGStore) PutPayload(ctx context.Context, jobID, cmd string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO job_payloads (job_id, cmd) VALUES ($1,$2)
		ON CONFLICT (job_id) DO UPDATE SET cmd=EXCLUDED.cmd`, jobID, cmd)
	return err
}

func (s *PGStore) GetPayload(ctx context.Context, jobID string) (string, error) {
	var cmd string
	err := s.pool.QueryRow(ctx, `SELECT cmd FROM job_payloads WHERE job_id=$1`, jobID).Scan(&cmd)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("payload for job %s: %w", jobID, ErrNotFound)
	}
	return cmd, err
}
