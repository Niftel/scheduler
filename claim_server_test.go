package main

import (
	"strings"
	"testing"
)

func TestClaimServerDisabledWithoutSecretsURL(t *testing.T) {
	server, secrets, err := newClaimServer(nil, claimServerConfig{})
	if err != nil {
		t.Fatalf("newClaimServer: %v", err)
	}
	if server != nil {
		t.Fatal("disabled claim server must be nil")
	}
	if secrets != nil {
		t.Fatal("disabled secrets client must be nil")
	}
}

func TestClaimServerRejectsPartialConfiguration(t *testing.T) {
	_, _, err := newClaimServer(nil, claimServerConfig{SecretsURL: "https://secrets.internal"})
	if err == nil {
		t.Fatal("expected partial configuration to fail")
	}
	if !strings.Contains(err.Error(), "is required when PRAETOR_SECRETS_URL is set") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClaimServerValidationRequiresEveryTrustBoundary(t *testing.T) {
	config := claimServerConfig{
		SecretsURL: "https://secrets.internal", SecretsCAFile: "secrets-ca.pem",
		SecretsCertificateFile: "scheduler.pem", SecretsPrivateKeyFile: "scheduler-key.pem",
		TrustDomain: "praetor.internal", ServerCertificateFile: "claim.pem",
		ServerPrivateKeyFile: "claim-key.pem", ExecutorCAFile: "executors-ca.pem",
	}
	if err := config.validate(); err != nil {
		t.Fatalf("complete configuration rejected: %v", err)
	}
}
