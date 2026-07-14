package core

import (
	"context"
	"encoding/json"
	"time"

	"github.com/praetordev/notify"
)

// notifyWorkflow fires a workflow template's attached notifications for a
// lifecycle event (success | error | approval). Everything is resolved from the
// run id, so callers pass only the workflow_job id, the event, and a human verb.
//
// Workflow notifications are dispatched here (not by the consumer, which projects
// executor job events) because a workflow run finalizes and its approval nodes are
// created in the scheduler. It is exactly-once per transition: each call site is
// the single place that transition happens — the finalize block and approval-node
// start — and both run under advanceWorkflow's per-workflow advisory lock. Sends
// run in the background so an HTTP call can't stall the scheduler tick.
func (s *Scheduler) notifyWorkflow(wjID int64, event, verb string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		type row struct {
			Type         string          `db:"notification_type"`
			Config       json.RawMessage `db:"config"`
			WorkflowName string          `db:"name"`
		}
		var rows []row
		if err := s.DB.SelectContext(ctx, &rows, `
			SELECT nt.notification_type, nt.config, wt.name
			FROM workflow_jobs wj
			JOIN workflow_templates wt ON wt.id = wj.workflow_template_id
			JOIN workflow_template_notifications wtn ON wtn.workflow_template_id = wt.id AND wtn.event = $2
			JOIN notification_templates nt ON nt.id = wtn.notification_template_id
			WHERE wj.id = $1`, wjID, event); err != nil {
			logger.Error("workflow notifier lookup failed", "workflow_id", wjID, "event", event, "err", err)
			return
		}
		// Kind gives the human-facing backends the right noun ("Praetor workflow ..."
		// / "Praetor workflow approval ...") and tags the webhook body.
		kind := "workflow"
		switch event {
		case "approval", "approved", "denied":
			kind = "workflow approval"
		}
		for _, r := range rows {
			if err := notify.SendOne(ctx, r.Type, r.Config, notify.Message{
				JobID: wjID, JobName: r.WorkflowName, Event: event, Status: verb, Kind: kind,
			}); err != nil {
				logger.Error("workflow notifier send failed", "type", r.Type, "workflow_id", wjID, "err", err)
				continue
			}
			logger.Info("workflow notifier sent", "type", r.Type, "workflow_id", wjID, "event", event)
		}
	}()
}
