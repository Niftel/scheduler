package core

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// enqueueWorkflowNotifications snapshots one logical delivery per matching
// policy. It deliberately reads no notification target configuration: only the
// notification worker may decrypt and use destination credentials.
//
// The caller owns tx so the workflow transition (or its exactly-once watermark)
// and every delivery row commit or roll back together.
func (s *Scheduler) enqueueWorkflowNotifications(
	ctx context.Context,
	tx *sqlx.Tx,
	wjID int64,
	event string,
	occurrenceType string,
	occurrenceID string,
	subjectKind string,
) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO notification_deliveries (
			idempotency_key,
			organization_id,
			team_id,
			notification_policy_id,
			notification_template_id,
			target_name,
			target_type,
			resource_type,
			resource_id,
			event,
			occurrence_type,
			occurrence_id,
			subject_id,
			subject_name,
			subject_kind
		)
		SELECT
			LEFT(
				'workflow:' || $3 || ':' || $4 || ':' || $2 || ':policy:' || np.id::text,
				255
			),
			np.organization_id,
			np.team_id,
			np.id,
			nt.id,
			nt.name,
			nt.notification_type,
			np.resource_type,
			np.resource_id,
			np.event,
			$3,
			LEFT($4, 255),
			wj.id,
			LEFT(wt.name, 255),
			$5
		FROM workflow_jobs wj
		JOIN workflow_templates wt ON wt.id = wj.workflow_template_id
		JOIN notification_policies np
		  ON np.resource_type = 'workflow_template'
		 AND np.resource_id = wt.id
		 AND np.event = $2
		JOIN notification_templates nt ON nt.id = np.notification_template_id
		WHERE wj.id = $1
		  AND (
		    ($2 IN ('approval','approved','denied','timeout') AND np.team_id = wj.approval_team_id)
		    OR
		    ($2 NOT IN ('approval','approved','denied','timeout') AND np.team_id IS NULL)
		  )
		ON CONFLICT (idempotency_key) DO NOTHING`,
		wjID, event, occurrenceType, occurrenceID, subjectKind)
	if err != nil {
		return fmt.Errorf("enqueue workflow %s notification: %w", event, err)
	}
	return nil
}

func (s *Scheduler) markWorkflowStarted(ctx context.Context, wjID int64) error {
	tx, err := s.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin workflow started transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx,
		`UPDATE workflow_jobs SET started_notified=true WHERE id=$1 AND started_notified=false`,
		wjID)
	if err != nil {
		return fmt.Errorf("claim workflow started transition: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read workflow started transition result: %w", err)
	}
	if changed == 1 {
		occurrenceID := fmt.Sprintf("%d:started", wjID)
		if err := s.enqueueWorkflowNotifications(
			ctx, tx, wjID, "started", "workflow_job", occurrenceID, "workflow",
		); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit workflow started transition: %w", err)
	}
	return nil
}

func (s *Scheduler) beginApproval(ctx context.Context, wjID, nodeID int64) (bool, error) {
	tx, err := s.DB.BeginTxx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin approval transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx,
		`UPDATE workflow_job_nodes SET status='awaiting_approval' WHERE id=$1 AND workflow_job_id=$2 AND status='pending'`,
		nodeID, wjID)
	if err != nil {
		return false, fmt.Errorf("start approval transition: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read approval transition result: %w", err)
	}
	if changed == 0 {
		return false, nil
	}
	occurrenceID := fmt.Sprintf("%d:approval", nodeID)
	if err := s.enqueueWorkflowNotifications(
		ctx, tx, wjID, "approval", "workflow_node", occurrenceID, "workflow approval",
	); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit approval transition: %w", err)
	}
	return true, nil
}

func (s *Scheduler) expireApproval(ctx context.Context, wjID, nodeID int64) (string, bool, error) {
	tx, err := s.DB.BeginTxx(ctx, nil)
	if err != nil {
		return "", false, fmt.Errorf("begin approval timeout transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var status string
	err = tx.GetContext(ctx, &status, `
		UPDATE workflow_job_nodes
		SET status=approval_timeout_action, timed_out=true, outcome_notified=true
		WHERE id=$1 AND workflow_job_id=$2 AND status='awaiting_approval'
		  AND awaiting_since + make_interval(secs => approval_timeout_seconds) <= now()
		RETURNING status`, nodeID, wjID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("expire approval transition: %w", err)
	}
	occurrenceID := fmt.Sprintf("%d:timeout", nodeID)
	if err := s.enqueueWorkflowNotifications(
		ctx, tx, wjID, "timeout", "workflow_node", occurrenceID, "workflow approval",
	); err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, fmt.Errorf("commit approval timeout transition: %w", err)
	}
	return status, true, nil
}

func (s *Scheduler) markApprovalOutcome(
	ctx context.Context,
	wjID int64,
	nodeID int64,
	event string,
) (bool, error) {
	tx, err := s.DB.BeginTxx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin approval outcome transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx, `
		UPDATE workflow_job_nodes
		SET outcome_notified=true
		WHERE id=$1 AND workflow_job_id=$2 AND outcome_notified=false
		  AND status IN ('approved','rejected')`, nodeID, wjID)
	if err != nil {
		return false, fmt.Errorf("claim approval outcome transition: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read approval outcome transition result: %w", err)
	}
	if changed == 0 {
		return false, nil
	}
	occurrenceID := fmt.Sprintf("%d:%s", nodeID, event)
	if err := s.enqueueWorkflowNotifications(
		ctx, tx, wjID, event, "workflow_node", occurrenceID, "workflow approval",
	); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit approval outcome transition: %w", err)
	}
	return true, nil
}

func (s *Scheduler) finishWorkflow(ctx context.Context, wjID int64, status string) (bool, error) {
	tx, err := s.DB.BeginTxx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin workflow terminal transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx, `
		UPDATE workflow_jobs
		SET status=$1, finished_at=now()
		WHERE id=$2 AND status='running'`, status, wjID)
	if err != nil {
		return false, fmt.Errorf("finish workflow transition: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read workflow terminal transition result: %w", err)
	}
	if changed == 0 {
		return false, nil
	}
	event := "error"
	if status == "successful" {
		event = "success"
	}
	occurrenceID := fmt.Sprintf("%d:%s", wjID, event)
	if err := s.enqueueWorkflowNotifications(
		ctx, tx, wjID, event, "workflow_job", occurrenceID, "workflow",
	); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit workflow terminal transition: %w", err)
	}
	return true, nil
}
