package claim

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/Niftel/praetor-secrets/credential"
	"github.com/Niftel/praetor-secrets/transport"
	"github.com/google/uuid"
)

type ClaimService interface {
	Claim(context.Context, uuid.UUID, uuid.UUID, string) error
}

type Handler struct {
	service ClaimService
	mapper  transport.SPIFFEMapper
}

func NewHandler(service ClaimService, trustDomain string) (*Handler, error) {
	if service == nil || trustDomain == "" {
		return nil, ErrConflict
	}
	return &Handler{service: service, mapper: transport.SPIFFEMapper{TrustDomain: trustDomain}}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/internal/v1/execution-runs/") || !strings.HasSuffix(r.URL.Path, "/claim") {
		h.problem(w, http.StatusNotFound, "resource_not_found")
		return
	}
	identity, err := h.identity(r)
	if err != nil || identity.Role != credential.RoleExecutor {
		h.problem(w, http.StatusUnauthorized, "workload_authentication_failed")
		return
	}
	runText := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/internal/v1/execution-runs/"), "/claim")
	if strings.Contains(runText, "/") {
		h.problem(w, http.StatusNotFound, "resource_not_found")
		return
	}
	runID, err := uuid.Parse(runText)
	if err != nil {
		h.problem(w, http.StatusBadRequest, "invalid_request")
		return
	}
	var body struct {
		DispatchID uuid.UUID `json:"dispatch_id"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&body) != nil || body.DispatchID == uuid.Nil {
		h.problem(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		h.problem(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if err := h.service.Claim(r.Context(), runID, body.DispatchID, identity.Subject); err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			h.problem(w, http.StatusNotFound, "resource_not_found")
		case errors.Is(err, ErrConflict):
			h.problem(w, http.StatusConflict, "claim_conflict")
		default:
			h.problem(w, http.StatusServiceUnavailable, "service_unavailable")
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"claimed":true}`))
}

func (h *Handler) identity(r *http.Request) (credential.WorkloadIdentity, error) {
	if r.TLS == nil || !r.TLS.HandshakeComplete || r.TLS.Version != tls.VersionTLS13 || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return credential.WorkloadIdentity{}, transport.ErrWorkloadIdentity
	}
	return h.mapper.Identity(r.TLS.VerifiedChains[0][0])
}

func (h *Handler) problem(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "code": code})
}
