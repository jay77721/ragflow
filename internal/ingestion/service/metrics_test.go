//
//  Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestIngestorStats_ExportPrometheusText(t *testing.T) {
	stats := IngestorStats{
		IdleSlots:           3,
		ReservedSlots:       1,
		HandlingSlots:       2,
		ActivePulls:         1,
		PullExpiryCount:     5,
		PullErrorCount:      2,
		MaxWaitingRejects:   1,
		HeartbeatFailures:   0,
		AckCount:            42,
		NackCount:           3,
		DuplicateClaims:     4,
		SlotInvariantErrors: 0,
	}

	text := stats.ExportPrometheusText("ragflow_ingestor", "worker-node-1")

	expectedSubstrings := []string{
		`# HELP ragflow_ingestor_idle_slots Current number of idle worker slots`,
		`# TYPE ragflow_ingestor_idle_slots gauge`,
		`ragflow_ingestor_idle_slots{ingestor_id="worker-node-1"} 3`,
		`# HELP ragflow_ingestor_reserved_slots Current number of reserved worker slots awaiting messages`,
		`ragflow_ingestor_reserved_slots{ingestor_id="worker-node-1"} 1`,
		`# HELP ragflow_ingestor_handling_slots Current number of worker slots currently handling tasks`,
		`ragflow_ingestor_handling_slots{ingestor_id="worker-node-1"} 2`,
		`# HELP ragflow_ingestor_active_pulls Current number of active in-flight stream pull operations`,
		`ragflow_ingestor_active_pulls{ingestor_id="worker-node-1"} 1`,
		`# HELP ragflow_ingestor_pull_expiries_total Total count of empty pull expirations`,
		`# TYPE ragflow_ingestor_pull_expiries_total counter`,
		`ragflow_ingestor_pull_expiries_total{ingestor_id="worker-node-1"} 5`,
		`# HELP ragflow_ingestor_pull_errors_total Total count of pull errors`,
		`ragflow_ingestor_pull_errors_total{ingestor_id="worker-node-1"} 2`,
		`# HELP ragflow_ingestor_max_waiting_rejects_total Total count of pull requests rejected due to MaxWaiting capacity`,
		`ragflow_ingestor_max_waiting_rejects_total{ingestor_id="worker-node-1"} 1`,
		`# HELP ragflow_ingestor_heartbeat_failures_total Total count of task heartbeat renewal failures`,
		`ragflow_ingestor_heartbeat_failures_total{ingestor_id="worker-node-1"} 0`,
		`# HELP ragflow_ingestor_acks_total Total count of successfully acknowledged task messages`,
		`ragflow_ingestor_acks_total{ingestor_id="worker-node-1"} 42`,
		`# HELP ragflow_ingestor_nacks_total Total count of negatively acknowledged task messages`,
		`ragflow_ingestor_nacks_total{ingestor_id="worker-node-1"} 3`,
		`# HELP ragflow_ingestor_duplicate_claims_total Total count of duplicate task claims rejected`,
		`ragflow_ingestor_duplicate_claims_total{ingestor_id="worker-node-1"} 4`,
		`# HELP ragflow_ingestor_slot_invariant_errors_total Total count of slot conservation invariant violations`,
		`ragflow_ingestor_slot_invariant_errors_total{ingestor_id="worker-node-1"} 0`,
	}

	for _, substr := range expectedSubstrings {
		if !strings.Contains(text, substr) {
			t.Errorf("expected ExportPrometheusText to contain %q, got:\n%s", substr, text)
		}
	}
}

func TestIngestor_PrometheusHandler(t *testing.T) {
	ingestor := newUnitIngestor("metrics-test-ingestor", 2, []string{"pdf"})
	ingestor.idleSlotsCount.Store(2)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	handler := ingestor.PrometheusHandler()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/plain") {
		t.Errorf("expected text/plain Content-Type, got %q", contentType)
	}

	body := rec.Body.String()
	expectedIdle := fmt.Sprintf(`ragflow_ingestor_idle_slots{ingestor_id="%s"} 2`, ingestor.ID())
	if !strings.Contains(body, expectedIdle) {
		t.Errorf("expected %s, got:\n%s", expectedIdle, body)
	}
}

func TestIngestor_RegisterOTelMetrics(t *testing.T) {
	ingestor := newUnitIngestor("otel-test-ingestor", 4, []string{"pdf"})

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := provider.Meter("ragflow/ingestion-test")

	if err := ingestor.RegisterOTelMetrics(meter); err != nil {
		t.Fatalf("RegisterOTelMetrics failed: %v", err)
	}

	// Collect metrics from the manual reader to trigger the callback
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect failed: %v", err)
	}

	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("expected ScopeMetrics from OTel reader after collection")
	}

	metrics := rm.ScopeMetrics[0].Metrics
	if len(metrics) == 0 {
		t.Fatal("expected registered metrics in ScopeMetrics")
	}

	foundIdle := false
	for _, m := range metrics {
		if m.Name == "ragflow.ingestor.idle_slots" {
			foundIdle = true
			break
		}
	}
	if !foundIdle {
		t.Errorf("expected metric ragflow.ingestor.idle_slots in collected metrics")
	}
}

func TestActiveIngestorRegistry(t *testing.T) {
	ing1 := newUnitIngestor("cluster-ing-1", 1, []string{"pdf"})
	ing2 := newUnitIngestor("cluster-ing-2", 2, []string{"pdf"})

	RegisterActiveIngestor(ing1)
	RegisterActiveIngestor(ing2)

	text := ExportAllPrometheusText()
	if !strings.Contains(text, fmt.Sprintf(`ingestor_id="%s"`, ing1.ID())) || !strings.Contains(text, fmt.Sprintf(`ingestor_id="%s"`, ing2.ID())) {
		t.Fatalf("expected text to contain both ingestor IDs, got:\n%s", text)
	}

	UnregisterActiveIngestor(ing1.ID())
	UnregisterActiveIngestor(ing2.ID())

	textAfter := ExportAllPrometheusText()
	if strings.Contains(textAfter, fmt.Sprintf(`ingestor_id="%s"`, ing1.ID())) {
		t.Fatalf("expected cluster-ing-1 removed after unregister, got:\n%s", textAfter)
	}
}
