package core

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/praetordev/crypto"
	"github.com/praetordev/events"
)

// resolveGalaxyServers returns an organization's configured Ansible Galaxy /
// Automation Hub servers, in order, with each credential's API token decrypted —
// ready to embed in a job manifest. Returns nil when the org has none.
func (s *Scheduler) resolveGalaxyServers(ctx context.Context, orgID int64) []events.GalaxyServer {
	rows, err := s.DB.QueryxContext(ctx, `
		SELECT c.name, c.inputs
		FROM organization_galaxy_credentials ogc
		JOIN credentials c ON c.id = ogc.credential_id
		WHERE ogc.organization_id = $1
		ORDER BY ogc.position, ogc.id`, orgID)
	if err != nil {
		logger.Error("galaxy resolve for org failed", "org_id", orgID, "err", err)
		return nil
	}
	defer rows.Close()

	var servers []events.GalaxyServer
	for rows.Next() {
		var name string
		var inputs []byte
		if err := rows.Scan(&name, &inputs); err != nil {
			continue
		}
		var f struct {
			URL     string `json:"url"`
			AuthURL string `json:"auth_url"`
			Token   string `json:"token"`
		}
		if json.Unmarshal(inputs, &f) != nil || f.URL == "" {
			continue
		}
		token := f.Token
		if token != "" {
			if dec, err := crypto.DecryptSecret(token); err == nil {
				token = dec // stored encrypted; fall back to as-is if not
			}
		}
		servers = append(servers, events.GalaxyServer{
			Name:    galaxyServerName(name),
			URL:     f.URL,
			AuthURL: f.AuthURL,
			Token:   token,
		})
	}
	return servers
}

// galaxyServerName sanitizes a credential name into an ansible-galaxy server id
// (it becomes part of an env var key: ANSIBLE_GALAXY_SERVER_<NAME>_URL).
func galaxyServerName(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if s := strings.Trim(b.String(), "_"); s != "" {
		return s
	}
	return "galaxy"
}
