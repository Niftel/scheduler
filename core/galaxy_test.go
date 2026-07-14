package core

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/praetordev/crypto"
)

// TestResolveGalaxyServers verifies the scheduler turns an org's attached Galaxy
// credentials into manifest servers, decrypting the stored token. Requires
// TEST_DATABASE_URL migrated through 000019.
func TestResolveGalaxyServers(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping galaxy resolution test")
	}
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		t.Skipf("cannot reach TEST_DATABASE_URL: %v", err)
	}
	defer db.Close()

	// Encryption and the resolver's decryption both read PRAETOR_SECRET_KEY, so
	// pin it for this process to keep them symmetric.
	t.Setenv("PRAETOR_SECRET_KEY", "0123456789abcdef0123456789abcdef")

	ctx := context.Background()
	uniq := time.Now().UnixNano()

	var ctID int64
	if err := db.QueryRow(
		`INSERT INTO credential_types (name, description, inputs, injectors) VALUES ($1,'','{}','{}') RETURNING id`,
		fmt.Sprintf("galaxy-type-%d", uniq)).Scan(&ctID); err != nil {
		t.Fatalf("credential_type: %v", err)
	}
	var orgID int64
	if err := db.QueryRow(`INSERT INTO organizations (name) VALUES ($1) RETURNING id`, fmt.Sprintf("gx-org-%d", uniq)).Scan(&orgID); err != nil {
		t.Fatalf("org: %v", err)
	}

	// Token is stored encrypted, exactly as the credentials handler writes it.
	encToken, err := crypto.EncryptSecret("s3cr3t-token")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	inputs := fmt.Sprintf(`{"url":"https://hub.example/api/","auth_url":"https://sso.example/token","token":%q}`, encToken)
	var credID int64
	if err := db.QueryRow(
		`INSERT INTO credentials (organization_id, credential_type_id, name, inputs) VALUES ($1,$2,$3,$4::jsonb) RETURNING id`,
		orgID, ctID, "My Private Hub", inputs).Scan(&credID); err != nil {
		t.Fatalf("credential: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO organization_galaxy_credentials (organization_id, credential_id, position) VALUES ($1,$2,0)`, orgID, credID); err != nil {
		t.Fatalf("attach: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM organizations WHERE id=$1`, orgID)
		_, _ = db.Exec(`DELETE FROM credential_types WHERE id=$1`, ctID)
	})

	sched := NewScheduler(db, time.Second, nil)
	servers := sched.resolveGalaxyServers(ctx, orgID)

	if len(servers) != 1 {
		t.Fatalf("expected 1 galaxy server, got %d", len(servers))
	}
	s := servers[0]
	if s.Name != "my_private_hub" {
		t.Errorf("server name = %q, want sanitized my_private_hub", s.Name)
	}
	if s.URL != "https://hub.example/api/" {
		t.Errorf("server url = %q", s.URL)
	}
	if s.AuthURL != "https://sso.example/token" {
		t.Errorf("server auth_url = %q", s.AuthURL)
	}
	if s.Token != "s3cr3t-token" {
		t.Errorf("token not decrypted: got %q want s3cr3t-token", s.Token)
	}

	// An org with nothing attached resolves to no servers.
	if got := sched.resolveGalaxyServers(ctx, 999999999); len(got) != 0 {
		t.Errorf("unattached org should yield no servers, got %d", len(got))
	}
}
