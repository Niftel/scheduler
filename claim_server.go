package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	secretsclient "github.com/Niftel/praetor-secrets/client"
	secretstransport "github.com/Niftel/praetor-secrets/transport"
	"github.com/jmoiron/sqlx"
	"github.com/praetordev/scheduler/claim"
)

type claimServerConfig struct {
	SecretsURL             string
	SecretsCAFile          string
	SecretsCertificateFile string
	SecretsPrivateKeyFile  string
	TrustDomain            string
	Address                string
	ServerCertificateFile  string
	ServerPrivateKeyFile   string
	ExecutorCAFile         string
}

func (config claimServerConfig) enabled() bool {
	return config.SecretsURL != ""
}

func (config claimServerConfig) validate() error {
	if !config.enabled() {
		return nil
	}
	required := map[string]string{
		"PRAETOR_SECRETS_CA_FILE":      config.SecretsCAFile,
		"PRAETOR_SECRETS_CERT_FILE":    config.SecretsCertificateFile,
		"PRAETOR_SECRETS_KEY_FILE":     config.SecretsPrivateKeyFile,
		"PRAETOR_SECRETS_TRUST_DOMAIN": config.TrustDomain,
		"PRAETOR_CLAIM_CERT_FILE":      config.ServerCertificateFile,
		"PRAETOR_CLAIM_KEY_FILE":       config.ServerPrivateKeyFile,
		"PRAETOR_CLAIM_CLIENT_CA_FILE": config.ExecutorCAFile,
	}
	for name, value := range required {
		if value == "" {
			return fmt.Errorf("%s is required when PRAETOR_SECRETS_URL is set", name)
		}
	}
	return nil
}

func newClaimServer(database *sqlx.DB, config claimServerConfig) (*http.Server, *secretsclient.Client, error) {
	if err := config.validate(); err != nil {
		return nil, nil, err
	}
	if !config.enabled() {
		return nil, nil, nil
	}
	if database == nil {
		return nil, nil, errors.New("claim server database is required")
	}
	secrets, err := secretsclient.New(secretsclient.Config{
		BaseURL:         config.SecretsURL,
		CAFile:          config.SecretsCAFile,
		CertificateFile: config.SecretsCertificateFile,
		PrivateKeyFile:  config.SecretsPrivateKeyFile,
		Timeout:         10 * time.Second,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("configure secrets client: %w", err)
	}
	serverCertificate, err := tls.LoadX509KeyPair(config.ServerCertificateFile, config.ServerPrivateKeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load claim server certificate: %w", err)
	}
	caPEM, err := os.ReadFile(config.ExecutorCAFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read claim client CA: %w", err)
	}
	executorCAs := x509.NewCertPool()
	if !executorCAs.AppendCertsFromPEM(caPEM) {
		return nil, nil, errors.New("claim client CA contains no certificates")
	}
	tlsConfig, err := secretstransport.TLSConfig(serverCertificate, executorCAs)
	if err != nil {
		return nil, nil, fmt.Errorf("configure claim server TLS: %w", err)
	}
	handler, err := claim.NewHandler(claim.NewManager(database, secrets), config.TrustDomain)
	if err != nil {
		return nil, nil, fmt.Errorf("configure claim handler: %w", err)
	}
	address := config.Address
	if address == "" {
		address = ":8443"
	}
	return &http.Server{
		Addr:              address,
		Handler:           handler,
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}, secrets, nil
}
