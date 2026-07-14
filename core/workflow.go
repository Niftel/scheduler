package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/praetordev/launch"
)

// processWorkflows advances every running workflow one step per tick: it reaps
// finished node jobs, launches newly-eligible nodes (or skips them), pauses on
// approval gates, and finalizes the workflow when all nodes are terminal.
func (s *Scheduler) processWorkflows(ctx context.Context) {
	var ids []int64
	if err := s.DB.SelectContext(ctx, &ids, `SELECT id FROM workflow_jobs WHERE status='running'`); err != nil {
		return
	}
	for _, id := range ids {
		s.advanceWorkflowLocked(ctx, id)
	}
}

// wfLockNamespace scopes the workflow advisory locks so their keys can't collide
// with any other pg_advisory_lock user (arbitrary constant, "PW").
const wfLockNamespace = 0x5057

// advanceWorkflowLocked advances one workflow while holding a Postgres advisory
// lock keyed by its id, so multiple schedulers (HA) never advance the same
// workflow concurrently — which would double-launch nodes, since per-node status
// writes are not transactional. The lock is a global named lock (not tied to the
// connection the node writes go through), so it provides mutual exclusion even
// though advanceWorkflow uses the pool. A scheduler that can't acquire it skips
// this workflow; another instance holds it and will advance it.
func (s *Scheduler) advanceWorkflowLocked(ctx context.Context, id int64) {
	conn, err := s.DB.Connx(ctx)
	if err != nil {
		logger.Error("workflow acquire conn failed", "workflow_id", id, "err", err)
		return
	}
	defer conn.Close()

	var got bool
	if err := conn.GetContext(ctx, &got, `SELECT pg_try_advisory_lock($1::int, $2::int)`, wfLockNamespace, id); err != nil {
		logger.Error("workflow advisory lock failed", "workflow_id", id, "err", err)
		return
	}
	if !got {
		return // another scheduler is advancing this workflow
	}
	defer func() {
		if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_unlock($1::int, $2::int)`, wfLockNamespace, id); err != nil {
			logger.Error("workflow advisory unlock failed", "workflow_id", id, "err", err)
		}
	}()

	if err := s.advanceWorkflow(ctx, id); err != nil {
		logger.Error("workflow advance failed", "workflow_id", id, "err", err)
	}
}

type wfNode struct {
	ID            int64  `db:"id"`
	NodeKey       string `db:"node_key"`
	NodeType      string `db:"node_type"`
	Name          string `db:"name"`
	Status        string `db:"status"`
	JobTemplateID *int64 `db:"job_template_id"`
	UnifiedJobID  *int64 `db:"unified_job_id"`
	WebhookURL    string `db:"webhook_url"`  // webhook_out: URL to POST
	WebhookBody   string `db:"webhook_body"` // webhook_out: optional JSON body
	EventToken    string `db:"event_token"`  // webhook_in: per-run callback secret
}

type wfEdge struct {
	ParentKey string `db:"parent_key"`
	ChildKey  string `db:"child_key"`
	EdgeType  string `db:"edge_type"`
}

func wfTerminal(st string) bool {
	switch st {
	case "successful", "failed", "skipped", "approved", "rejected":
		return true
	}
	return false
}

// wfEdgeFires reports whether an edge of edgeType is satisfied by a parent that
// finished in parentState. approved counts as success, rejected as failure.
func wfEdgeFires(edgeType, parentState string) bool {
	switch edgeType {
	case "success":
		return parentState == "successful" || parentState == "approved"
	case "failure":
		return parentState == "failed" || parentState == "rejected"
	case "always":
		return parentState == "successful" || parentState == "failed" || parentState == "approved" || parentState == "rejected"
	}
	return false
}

// nodeStarter starts an eligible workflow node of a specific node_type — updating
// its status and performing any side effect (pause, remote POST, ...). Registered
// in nodeStarters keyed by node_type; a node with no registered starter falls
// through to the default job-launch path in advanceWorkflow. Table-ifying the old
// if/else ladder (B9/#88) makes adding a node type a single map entry.
type nodeStarter func(s *Scheduler, ctx context.Context, wfName string, wjID int64, n *wfNode)

var nodeStarters = map[string]nodeStarter{
	"approval":    (*Scheduler).startApprovalNode,
	"webhook_in":  (*Scheduler).startWebhookInNode,
	"webhook_out": (*Scheduler).startWebhookOutNode,
}

// startApprovalNode pauses the node until a user approves/denies it, and fires the
// workflow template's 'approval' notifications so approvers know to act. The node
// transitions pending->awaiting_approval exactly once, so the notification is sent
// once.
func (s *Scheduler) startApprovalNode(ctx context.Context, _ string, wjID int64, n *wfNode) {
	logExec(ctx, s.DB, `UPDATE workflow_job_nodes SET status='awaiting_approval' WHERE id=$1`, n.ID)
	n.Status = "awaiting_approval"
	s.notifyWorkflow(wjID, "approval", "needs approval")
}

// startWebhookInNode pauses until an external caller hits the node's callback with
// its per-run event_token. The token is minted now so the run detail can surface
// the callback URL for whoever needs to release it.
func (s *Scheduler) startWebhookInNode(ctx context.Context, _ string, wjID int64, n *wfNode) {
	token := newEventToken()
	logExec(ctx, s.DB,
		`UPDATE workflow_job_nodes SET status='awaiting_event', event_token=$1 WHERE id=$2`, token, n.ID)
	n.Status = "awaiting_event"
	n.EventToken = token
	logger.Info("workflow node awaiting remote event", "workflow_id", wjID, "node", n.NodeKey)
}

// startWebhookOutNode POSTs to the configured URL and continues immediately. A 2xx
// (or 3xx) is success; anything else — or a missing/failed request — fails the node
// so its failure edges fire.
func (s *Scheduler) startWebhookOutNode(ctx context.Context, wfName string, wjID int64, n *wfNode) {
	newSt := "successful"
	if !postWorkflowWebhook(n.WebhookURL, n.WebhookBody, wfName, wjID, n.NodeKey) {
		newSt = "failed"
	}
	logExec(ctx, s.DB, `UPDATE workflow_job_nodes SET status=$1 WHERE id=$2`, newSt, n.ID)
	n.Status = newSt
	logger.Info("workflow node webhook_out", "workflow_id", wjID, "node", n.NodeKey, "status", newSt)
}

func (s *Scheduler) advanceWorkflow(ctx context.Context, wjID int64) error {
	var wf struct {
		TemplateID int64           `db:"workflow_template_id"`
		Name       string          `db:"name"`
		LaunchArgs json.RawMessage `db:"launch_args"`
	}
	if err := s.DB.GetContext(ctx, &wf,
		`SELECT wj.workflow_template_id, wt.name, wj.launch_args
		 FROM workflow_jobs wj JOIN workflow_templates wt ON wt.id = wj.workflow_template_id
		 WHERE wj.id=$1`, wjID); err != nil {
		return err
	}
	// Workflow-level launch overrides (a schedule's extra_vars, a webhook payload,
	// an EDA event + --limit) captured at snapshot time. They are overlaid on each
	// node job's template defaults below (launch wins), matching AWX workflow
	// extra_vars semantics.
	wfOpts := launch.ParseArgs(wf.LaunchArgs)

	// Fire the 'started' notification the first time this run is advanced. The
	// atomic claim (started_notified false->true) makes it exactly-once on its own,
	// independent of the advisory lock.
	if res, err := s.DB.ExecContext(ctx,
		`UPDATE workflow_jobs SET started_notified=true WHERE id=$1 AND started_notified=false`, wjID); err == nil {
		if rows, _ := res.RowsAffected(); rows == 1 {
			s.notifyWorkflow(wjID, "started", "started")
		}
	}

	// Read the run's snapshotted graph — not the template — so editing the template
	// never changes a run in flight.
	var nodes []wfNode
	if err := s.DB.SelectContext(ctx, &nodes,
		`SELECT wjn.id, wjn.node_key, wjn.node_type, COALESCE(wjn.name, '') AS name,
		        wjn.status, wjn.job_template_id, wjn.unified_job_id,
		        COALESCE(wjn.webhook_url, '')  AS webhook_url,
		        COALESCE(wjn.webhook_body, '') AS webhook_body,
		        COALESCE(wjn.event_token, '') AS event_token
		 FROM workflow_job_nodes wjn
		 WHERE wjn.workflow_job_id = $1`, wjID); err != nil {
		return err
	}
	byKey := map[string]*wfNode{}
	for i := range nodes {
		byKey[nodes[i].NodeKey] = &nodes[i]
	}

	var edges []wfEdge
	_ = s.DB.SelectContext(ctx, &edges,
		`SELECT parent_key, child_key, edge_type FROM workflow_job_edges WHERE workflow_job_id=$1`, wjID)
	parentsOf := map[string][]wfEdge{}
	for _, e := range edges {
		parentsOf[e.ChildKey] = append(parentsOf[e.ChildKey], e)
	}

	// 1. Reap node jobs that have finished.
	for i := range nodes {
		n := &nodes[i]
		if n.Status == "running" && n.UnifiedJobID != nil {
			var st string
			if err := s.DB.GetContext(ctx, &st, `SELECT status FROM unified_jobs WHERE id=$1`, *n.UnifiedJobID); err == nil {
				if st == "successful" || st == "failed" || st == "error" {
					newSt := "failed"
					if st == "successful" {
						newSt = "successful"
					}
					logExec(ctx, s.DB, `UPDATE workflow_job_nodes SET status=$1 WHERE id=$2`, newSt, n.ID)
					n.Status = newSt
				}
			}
		}
	}

	// 1b. Fire approval-outcome notifications once. A user approves/denies an
	// approval node via the API (status -> approved/rejected); the scheduler
	// observes the outcome here and notifies, claiming outcome_notified so it fires
	// exactly once.
	for i := range nodes {
		n := &nodes[i]
		if n.NodeType != "approval" || (n.Status != "approved" && n.Status != "rejected") {
			continue
		}
		res, err := s.DB.ExecContext(ctx,
			`UPDATE workflow_job_nodes SET outcome_notified=true WHERE id=$1 AND outcome_notified=false`, n.ID)
		if err != nil {
			continue
		}
		if rows, _ := res.RowsAffected(); rows == 1 {
			if n.Status == "approved" {
				s.notifyWorkflow(wjID, "approved", "approved")
			} else {
				s.notifyWorkflow(wjID, "denied", "denied")
			}
		}
	}

	// 2. Evaluate pending nodes whose parents have all finished.
	for i := range nodes {
		n := &nodes[i]
		if n.Status != "pending" {
			continue
		}
		parents := parentsOf[n.NodeKey]
		allParentsTerminal := true
		for _, e := range parents {
			if p := byKey[e.ParentKey]; p == nil || !wfTerminal(p.Status) {
				allParentsTerminal = false
				break
			}
		}
		if !allParentsTerminal {
			continue
		}

		fired := len(parents) == 0 // a root node runs unconditionally
		for _, e := range parents {
			if p := byKey[e.ParentKey]; p != nil && wfEdgeFires(e.EdgeType, p.Status) {
				fired = true
				break
			}
		}
		if !fired {
			logExec(ctx, s.DB, `UPDATE workflow_job_nodes SET status='skipped' WHERE id=$1`, n.ID)
			n.Status = "skipped"
			continue
		}

		// Start the node by its type. approval/webhook_in/webhook_out have
		// registered starters (nodeStarters); anything else falls through to the
		// default job-launch path below. Adding a node type is one map entry.
		if start, ok := nodeStarters[n.NodeType]; ok {
			start(s, ctx, wf.Name, wjID, n)
			continue
		}

		if n.JobTemplateID == nil {
			logExec(ctx, s.DB, `UPDATE workflow_job_nodes SET status='skipped' WHERE id=$1`, n.ID)
			n.Status = "skipped"
			continue
		}

		// Launch the node's job template as an ordinary unified_job.
		var ujtID int64
		if err := s.DB.GetContext(ctx, &ujtID, `SELECT unified_job_template_id FROM job_templates WHERE id=$1`, *n.JobTemplateID); err != nil {
			logExec(ctx, s.DB, `UPDATE workflow_job_nodes SET status='failed' WHERE id=$1`, n.ID)
			n.Status = "failed"
			continue
		}
		// Name each node job uniquely per run so identical workflows/nodes don't
		// collide in the Jobs list: "<workflow> #<run> / <node>".
		nodeLabel := n.Name
		if nodeLabel == "" {
			nodeLabel = n.NodeKey
		}
		jobName := fmt.Sprintf("%s #%d / %s", wf.Name, wjID, nodeLabel)
		// Workflow nodes launch through the single job-creation site, carrying the
		// workflow-level launch Options (schedule/webhook/EDA context) which the
		// scheduler's manifest build overlays on the node template's defaults
		// (launch wins). Per-node overrides are a future change that now has one
		// place to land.
		jobID, err := launch.Job(ctx, s.DB, jobName, &ujtID, wfOpts)
		if err != nil {
			logExec(ctx, s.DB, `UPDATE workflow_job_nodes SET status='failed' WHERE id=$1`, n.ID)
			n.Status = "failed"
			continue
		}
		logExec(ctx, s.DB, `UPDATE workflow_job_nodes SET status='running', unified_job_id=$1 WHERE id=$2`, jobID, n.ID)
		n.Status = "running"
		jid := jobID
		n.UnifiedJobID = &jid
		logger.Info("workflow node launched as job", "workflow_id", wjID, "node", n.NodeKey, "job_id", jobID)
	}

	// 3. Finalize the workflow when every node is terminal.
	allTerminal := true
	for i := range nodes {
		if !wfTerminal(nodes[i].Status) {
			allTerminal = false
			break
		}
	}
	if allTerminal {
		anyFail := false
		for i := range nodes {
			if nodes[i].Status == "failed" || nodes[i].Status == "rejected" {
				anyFail = true
			}
		}
		status := "successful"
		if anyFail {
			status = "failed"
		}
		logExec(ctx, s.DB, `UPDATE workflow_jobs SET status=$1, finished_at=now() WHERE id=$2`, status, wjID)
		logger.Info("workflow finished", "workflow_id", wjID, "status", status)
		// Fire the workflow template's terminal-state notifications. This block runs
		// exactly once per run (processWorkflows only picks up status='running'), so
		// there is no dedup to do.
		if status == "successful" {
			s.notifyWorkflow(wjID, "success", "succeeded")
		} else {
			s.notifyWorkflow(wjID, "error", "failed")
		}
	}
	return nil
}

// newEventToken mints a random secret a webhook_in node's callback must present.
func newEventToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// postWorkflowWebhook POSTs a webhook_out node's payload to its URL and reports
// whether the call succeeded (2xx/3xx). A missing URL is a failure. With no body
// configured it sends a small JSON describing the workflow/node.
func postWorkflowWebhook(url, body, workflowName string, wjID int64, nodeKey string) bool {
	if url == "" {
		logger.Warn("workflow node webhook_out has no URL", "workflow_id", wjID, "node", nodeKey)
		return false
	}
	if body == "" {
		b, _ := json.Marshal(map[string]interface{}{
			"workflow": workflowName, "workflow_job_id": wjID, "node": nodeKey,
		})
		body = string(b)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte(body)))
	if err != nil {
		logger.Error("workflow node webhook_out bad request", "workflow_id", wjID, "node", nodeKey, "err", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("workflow node webhook_out POST failed", "workflow_id", wjID, "node", nodeKey, "err", err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}
