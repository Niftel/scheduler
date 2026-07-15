package claim

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type fakeClaimer struct {
	run, dispatch uuid.UUID
	executor      string
	err           error
}

func (f *fakeClaimer) Claim(_ context.Context, run, dispatch uuid.UUID, executor string) error {
	f.run, f.dispatch, f.executor = run, dispatch, executor
	return f.err
}

func claimRequest(t *testing.T, identity string) (*http.Request, uuid.UUID, uuid.UUID) {
	t.Helper()
	runID, dispatchID := uuid.New(), uuid.New()
	u, _ := url.Parse(identity)
	cert := &x509.Certificate{URIs: []*url.URL{u}}
	r := httptest.NewRequest(http.MethodPost, "/internal/v1/execution-runs/"+runID.String()+"/claim", strings.NewReader(`{"dispatch_id":"`+dispatchID.String()+`"}`))
	r.TLS = &tls.ConnectionState{Version: tls.VersionTLS13, HandshakeComplete: true, VerifiedChains: [][]*x509.Certificate{{cert}}}
	return r, runID, dispatchID
}

func TestClaimUsesCertificateDerivedExecutorIdentity(t *testing.T) {
	service := &fakeClaimer{}
	h, err := NewHandler(service, "praetor.local")
	if err != nil {
		t.Fatal(err)
	}
	r, runID, dispatchID := claimRequest(t, "spiffe://praetor.local/workload/praetor-executor/worker-7")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || service.run != runID || service.dispatch != dispatchID || service.executor != "praetor-executor:worker-7" {
		t.Fatalf("status=%d run=%s dispatch=%s executor=%q", w.Code, service.run, service.dispatch, service.executor)
	}
}

func TestClaimRejectsSchedulerAndUnverifiedIdentity(t *testing.T) {
	h, _ := NewHandler(&fakeClaimer{}, "praetor.local")
	r, _, _ := claimRequest(t, "spiffe://praetor.local/workload/praetor-scheduler")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("scheduler status=%d", w.Code)
	}
	r.TLS.VerifiedChains = nil
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unverified status=%d", w.Code)
	}
}
