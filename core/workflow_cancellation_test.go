package core

import "testing"

func TestWorkflowCancellationStateSemantics(t *testing.T) {
	if !wfTerminal("canceled") {
		t.Fatal("canceled workflow node must be terminal")
	}
	if wfEdgeFires("success", "canceled") {
		t.Fatal("success edge must not fire for cancellation")
	}
	if !wfEdgeFires("failure", "canceled") {
		t.Fatal("failure edge must fire for cancellation")
	}
	if !wfEdgeFires("always", "canceled") {
		t.Fatal("always edge must fire for cancellation")
	}
}
