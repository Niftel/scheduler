package core

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/praetordev/events"
	"github.com/praetordev/launch"
	"github.com/praetordev/models"
)

// manifest.go owns turning a claimed unified_job into an execution manifest —
// resolving the template, project, inventory, runner host, credential and pack.
// It was extracted out of the scheduler tick (processPendingJobs) so manifest
// assembly is one testable unit, and every read uses an EXPLICIT column list
// (never SELECT *) so adding a DB column can't break the dispatch path with a
// runtime scan error. The builders are read-only: they return the ids the caller
// snapshots onto execution_runs, keeping run-scoped writes in one place.
// See docs/coupling-decomposition-plan.md (B4).

// buildSyncManifest resolves an inventory-source sync job (no job template) into
// its manifest. Returns the manifest and the credential id to snapshot on the
// run (0 = none). The executor runs `ansible-inventory --list` for the source and
// upserts the result into the referenced inventory.
func (s *Scheduler) buildSyncManifest(ctx context.Context, tx *sqlx.Tx, sourceID int64) (events.JobManifest, int64, error) {
	var src struct {
		InventoryID  int64  `db:"inventory_id"`
		Source       string `db:"source"`
		Kind         string `db:"source_kind"`
		CredentialID *int64 `db:"credential_id"`
	}
	if err := tx.GetContext(ctx, &src,
		`SELECT inventory_id, source, source_kind, credential_id FROM inventory_sources WHERE id = $1`, sourceID); err != nil {
		return events.JobManifest{}, 0, fmt.Errorf("inventory sync source %d not found: %w", sourceID, err)
	}
	m := events.JobManifest{
		InventorySync:       true,
		InventorySource:     src.Source,
		InventorySourceKind: src.Kind,
		SyncInventoryID:     src.InventoryID,
		APIURL:              s.APIURL,
	}
	var credID int64
	if src.CredentialID != nil {
		// Reference only: the executor resolves the source's cloud credential at
		// dispatch from ingestion (no plaintext at rest). The caller snapshots the
		// id on the run so resolution is run-scoped.
		m.CredentialID = *src.CredentialID
		credID = *src.CredentialID
	}
	return m, credID, nil
}

// buildJobManifest resolves a job template into its execution manifest. Returns
// the manifest plus the runner-host and credential ids the caller snapshots on
// the run (0 = none). Inline playbooks are disabled — playbooks come only from an
// SCM project.
func (s *Scheduler) buildJobManifest(ctx context.Context, tx *sqlx.Tx, job models.UnifiedJob) (events.JobManifest, int64, int64, error) {
	var template models.JobTemplate
	if err := tx.GetContext(ctx, &template, `
		SELECT id, organization_id, name, inventory_id, project_id, playbook,
		       credential_id, execution_pack_id, forks, verbosity, extra_vars,
		       job_limit, use_fact_cache
		FROM job_templates WHERE id = $1`, *job.UnifiedJobTemplateID); err != nil {
		return events.JobManifest{}, 0, 0, fmt.Errorf("find template %d: %w", *job.UnifiedJobTemplateID, err)
	}

	// Project (SCM URL for the playbook), if the template has one.
	var projectURL string
	if template.ProjectID != nil {
		var project models.Project
		if err := tx.GetContext(ctx, &project,
			`SELECT id, name, scm_url FROM projects WHERE id = $1`, *template.ProjectID); err != nil {
			return events.JobManifest{}, 0, 0, fmt.Errorf("find project %d for template %q: %w", *template.ProjectID, template.Name, err)
		}
		projectURL = project.SCMURL
		logger.Info("using project for job", "project", project.Name, "scm_url", project.SCMURL, "job_id", job.ID)
	} else {
		logger.Info("template has no project - using default/inline logic", "template", template.Name, "job_id", job.ID)
	}

	// Inventory travels by reference (id only): the executor fetches the rendered
	// INI from ingestion at dispatch. We only confirm it exists here.
	var inventoryID int64
	if template.InventoryID != nil {
		var exists int64
		if err := tx.GetContext(ctx, &exists,
			`SELECT id FROM inventories WHERE id = $1`, *template.InventoryID); err != nil {
			return events.JobManifest{}, 0, 0, fmt.Errorf("find inventory %d for template %q: %w", *template.InventoryID, template.Name, err)
		}
		inventoryID = *template.InventoryID
	} else {
		logger.Info("template has no inventory - using default localhost", "template", template.Name, "job_id", job.ID)
	}

	// Runner host: the target the executor SSHes into to run the play. Prefer the
	// designated runner host; fall back to the first enabled host in the inventory.
	var runnerHostName string
	var runnerHostID int64
	if template.InventoryID != nil {
		var h models.Host
		err := tx.GetContext(ctx, &h,
			`SELECT id, name FROM hosts WHERE inventory_id = $1 AND is_runner_host = true AND enabled = true LIMIT 1`,
			*template.InventoryID)
		if err != nil {
			err = tx.GetContext(ctx, &h,
				`SELECT id, name FROM hosts WHERE inventory_id = $1 AND enabled = true ORDER BY id LIMIT 1`,
				*template.InventoryID)
			if err == nil {
				logger.Info("no runner host set - using first host", "host", h.Name, "host_id", h.ID, "job_id", job.ID)
			}
		} else {
			logger.Info("using runner host", "host", h.Name, "host_id", h.ID, "job_id", job.ID)
		}
		if err == nil {
			runnerHostName = h.Name
			runnerHostID = h.ID
		}
	}

	// Effective vars/limit: template defaults overlaid by the launch overrides
	// (already gated by the template's ask_* flags at launch time).
	jobOpts := launch.ParseArgs(job.JobArgs)
	m := events.JobManifest{
		InventoryID:     inventoryID, // executor fetches + fills Inventory + CachedFacts at dispatch (#13/#48)
		ProjectURL:      projectURL,
		Playbook:        template.Playbook,
		PlaybookContent: "", // inline playbooks disabled — SCM projects only
		ExtraVars:       jobOpts.MergeExtraVars(template.ExtraVars),
		Limit:           jobOpts.EffectiveLimit(template.JobLimit),
		Verbosity:       template.Verbosity, // #78
		Forks:           template.Forks,     // #78
		UseFactCache:    template.UseFactCache,
		RunnerHost:      runnerHostName,
		RunnerHostID:    runnerHostID,
		APIURL:          s.APIURL,
		GalaxyServers:   s.resolveGalaxyServers(ctx, template.OrganizationID),
	}

	// Machine credential (by reference; executor resolves injectors at dispatch).
	var credID int64
	if template.CredentialID != nil {
		m.CredentialID = *template.CredentialID
		credID = *template.CredentialID
	}

	// Execution Pack: which self-contained runtime to push. Empty = default pack.
	if template.ExecutionPackID != nil {
		var packName string
		if err := tx.GetContext(ctx, &packName,
			`SELECT name FROM execution_packs WHERE id = $1`, *template.ExecutionPackID); err == nil {
			m.ExecutionPack = packName
		}
	}

	return m, runnerHostID, credID, nil
}
