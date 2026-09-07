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
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	activeIngestorsMu sync.RWMutex
	activeIngestors   = make(map[string]*Ingestor)
)

// RegisterActiveIngestor tracks a running Ingestor instance for cluster metrics scraping.
func RegisterActiveIngestor(ing *Ingestor) {
	activeIngestorsMu.Lock()
	defer activeIngestorsMu.Unlock()
	activeIngestors[ing.id] = ing
}

// UnregisterActiveIngestor removes a stopped Ingestor instance from active tracking.
func UnregisterActiveIngestor(id string) {
	activeIngestorsMu.Lock()
	defer activeIngestorsMu.Unlock()
	delete(activeIngestors, id)
}

// ExportAllPrometheusText exports Prometheus metrics for all active Ingestors.
func ExportAllPrometheusText() string {
	activeIngestorsMu.RLock()
	defer activeIngestorsMu.RUnlock()
	var b strings.Builder
	for _, ing := range activeIngestors {
		b.WriteString(ing.ExportPrometheusText())
		b.WriteString("\n")
	}
	return b.String()
}

// ExportPrometheusText formats IngestorStats as Prometheus-compatible text (RP §5.3).
func (s IngestorStats) ExportPrometheusText(namespace string, ingestorID string) string {
	ns := strings.TrimSuffix(namespace, "_")
	if ns == "" {
		ns = "ragflow_ingestor"
	}

	var labels string
	if ingestorID != "" {
		labels = fmt.Sprintf("{ingestor_id=%q}", ingestorID)
	}

	var b strings.Builder

	// Slot occupancy and pull metrics (gauges)
	fmt.Fprintf(&b, "# HELP %s_idle_slots Current number of idle worker slots\n", ns)
	fmt.Fprintf(&b, "# TYPE %s_idle_slots gauge\n", ns)
	fmt.Fprintf(&b, "%s_idle_slots%s %d\n", ns, labels, s.IdleSlots)

	fmt.Fprintf(&b, "# HELP %s_reserved_slots Current number of reserved worker slots awaiting messages\n", ns)
	fmt.Fprintf(&b, "# TYPE %s_reserved_slots gauge\n", ns)
	fmt.Fprintf(&b, "%s_reserved_slots%s %d\n", ns, labels, s.ReservedSlots)

	fmt.Fprintf(&b, "# HELP %s_handling_slots Current number of worker slots currently handling tasks\n", ns)
	fmt.Fprintf(&b, "# TYPE %s_handling_slots gauge\n", ns)
	fmt.Fprintf(&b, "%s_handling_slots%s %d\n", ns, labels, s.HandlingSlots)

	fmt.Fprintf(&b, "# HELP %s_active_pulls Current number of active in-flight stream pull operations\n", ns)
	fmt.Fprintf(&b, "# TYPE %s_active_pulls gauge\n", ns)
	fmt.Fprintf(&b, "%s_active_pulls%s %d\n", ns, labels, s.ActivePulls)

	// Ingestion operational counters
	fmt.Fprintf(&b, "# HELP %s_pull_expiries_total Total count of empty pull expirations\n", ns)
	fmt.Fprintf(&b, "# TYPE %s_pull_expiries_total counter\n", ns)
	fmt.Fprintf(&b, "%s_pull_expiries_total%s %d\n", ns, labels, s.PullExpiryCount)

	fmt.Fprintf(&b, "# HELP %s_pull_errors_total Total count of pull errors\n", ns)
	fmt.Fprintf(&b, "# TYPE %s_pull_errors_total counter\n", ns)
	fmt.Fprintf(&b, "%s_pull_errors_total%s %d\n", ns, labels, s.PullErrorCount)

	fmt.Fprintf(&b, "# HELP %s_max_waiting_rejects_total Total count of pull requests rejected due to MaxWaiting capacity\n", ns)
	fmt.Fprintf(&b, "# TYPE %s_max_waiting_rejects_total counter\n", ns)
	fmt.Fprintf(&b, "%s_max_waiting_rejects_total%s %d\n", ns, labels, s.MaxWaitingRejects)

	fmt.Fprintf(&b, "# HELP %s_heartbeat_failures_total Total count of task heartbeat renewal failures\n", ns)
	fmt.Fprintf(&b, "# TYPE %s_heartbeat_failures_total counter\n", ns)
	fmt.Fprintf(&b, "%s_heartbeat_failures_total%s %d\n", ns, labels, s.HeartbeatFailures)

	fmt.Fprintf(&b, "# HELP %s_acks_total Total count of successfully acknowledged task messages\n", ns)
	fmt.Fprintf(&b, "# TYPE %s_acks_total counter\n", ns)
	fmt.Fprintf(&b, "%s_acks_total%s %d\n", ns, labels, s.AckCount)

	fmt.Fprintf(&b, "# HELP %s_nacks_total Total count of negatively acknowledged task messages\n", ns)
	fmt.Fprintf(&b, "# TYPE %s_nacks_total counter\n", ns)
	fmt.Fprintf(&b, "%s_nacks_total%s %d\n", ns, labels, s.NackCount)

	fmt.Fprintf(&b, "# HELP %s_duplicate_claims_total Total count of duplicate task claims rejected\n", ns)
	fmt.Fprintf(&b, "# TYPE %s_duplicate_claims_total counter\n", ns)
	fmt.Fprintf(&b, "%s_duplicate_claims_total%s %d\n", ns, labels, s.DuplicateClaims)

	fmt.Fprintf(&b, "# HELP %s_slot_invariant_errors_total Total count of slot conservation invariant violations\n", ns)
	fmt.Fprintf(&b, "# TYPE %s_slot_invariant_errors_total counter\n", ns)
	fmt.Fprintf(&b, "%s_slot_invariant_errors_total%s %d\n", ns, labels, s.SlotInvariantErrors)

	return b.String()
}

// ExportPrometheusText formats the ingestor's live stats as Prometheus exposition text.
func (e *Ingestor) ExportPrometheusText() string {
	return e.Stats().ExportPrometheusText("ragflow_ingestor", e.id)
}

// PrometheusHandler returns an http.HandlerFunc exporting the ingestor's live Prometheus metrics.
func (e *Ingestor) PrometheusHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(e.ExportPrometheusText()))
	}
}

// RegisterOTelMetrics registers dynamic OpenTelemetry observable gauges and counters
// with the given OTel Meter, observing live IngestorStats snapshots on collection.
func (e *Ingestor) RegisterOTelMetrics(meter metric.Meter) error {
	idleGauge, err := meter.Int64ObservableGauge(
		"ragflow.ingestor.idle_slots",
		metric.WithDescription("Current number of idle worker slots"),
		metric.WithUnit("{slot}"),
	)
	if err != nil {
		return err
	}
	reservedGauge, err := meter.Int64ObservableGauge(
		"ragflow.ingestor.reserved_slots",
		metric.WithDescription("Current number of reserved worker slots awaiting messages"),
		metric.WithUnit("{slot}"),
	)
	if err != nil {
		return err
	}
	handlingGauge, err := meter.Int64ObservableGauge(
		"ragflow.ingestor.handling_slots",
		metric.WithDescription("Current number of worker slots currently handling tasks"),
		metric.WithUnit("{slot}"),
	)
	if err != nil {
		return err
	}
	activePullsGauge, err := meter.Int64ObservableGauge(
		"ragflow.ingestor.active_pulls",
		metric.WithDescription("Current number of active in-flight stream pull operations"),
		metric.WithUnit("{pull}"),
	)
	if err != nil {
		return err
	}
	pullExpiryCounter, err := meter.Int64ObservableCounter(
		"ragflow.ingestor.pull_expiries_total",
		metric.WithDescription("Total count of empty pull expirations"),
	)
	if err != nil {
		return err
	}
	pullErrorCounter, err := meter.Int64ObservableCounter(
		"ragflow.ingestor.pull_errors_total",
		metric.WithDescription("Total count of pull errors"),
	)
	if err != nil {
		return err
	}
	maxWaitingRejectsCounter, err := meter.Int64ObservableCounter(
		"ragflow.ingestor.max_waiting_rejects_total",
		metric.WithDescription("Total count of pull requests rejected due to MaxWaiting capacity"),
	)
	if err != nil {
		return err
	}
	heartbeatFailuresCounter, err := meter.Int64ObservableCounter(
		"ragflow.ingestor.heartbeat_failures_total",
		metric.WithDescription("Total count of task heartbeat renewal failures"),
	)
	if err != nil {
		return err
	}
	ackCounter, err := meter.Int64ObservableCounter(
		"ragflow.ingestor.acks_total",
		metric.WithDescription("Total count of successfully acknowledged task messages"),
	)
	if err != nil {
		return err
	}
	nackCounter, err := meter.Int64ObservableCounter(
		"ragflow.ingestor.nacks_total",
		metric.WithDescription("Total count of negatively acknowledged task messages"),
	)
	if err != nil {
		return err
	}
	dupClaimsCounter, err := meter.Int64ObservableCounter(
		"ragflow.ingestor.duplicate_claims_total",
		metric.WithDescription("Total count of duplicate task claims rejected"),
	)
	if err != nil {
		return err
	}
	slotInvariantCounter, err := meter.Int64ObservableCounter(
		"ragflow.ingestor.slot_invariant_errors_total",
		metric.WithDescription("Total count of slot conservation invariant violations"),
	)
	if err != nil {
		return err
	}

	attrs := metric.WithAttributes(attribute.String("ingestor_id", e.id))

	_, err = meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			stats := e.Stats()
			o.ObserveInt64(idleGauge, int64(stats.IdleSlots), attrs)
			o.ObserveInt64(reservedGauge, int64(stats.ReservedSlots), attrs)
			o.ObserveInt64(handlingGauge, int64(stats.HandlingSlots), attrs)
			o.ObserveInt64(activePullsGauge, int64(stats.ActivePulls), attrs)
			o.ObserveInt64(pullExpiryCounter, stats.PullExpiryCount, attrs)
			o.ObserveInt64(pullErrorCounter, stats.PullErrorCount, attrs)
			o.ObserveInt64(maxWaitingRejectsCounter, stats.MaxWaitingRejects, attrs)
			o.ObserveInt64(heartbeatFailuresCounter, stats.HeartbeatFailures, attrs)
			o.ObserveInt64(ackCounter, stats.AckCount, attrs)
			o.ObserveInt64(nackCounter, stats.NackCount, attrs)
			o.ObserveInt64(dupClaimsCounter, stats.DuplicateClaims, attrs)
			o.ObserveInt64(slotInvariantCounter, stats.SlotInvariantErrors, attrs)
			return nil
		},
		idleGauge,
		reservedGauge,
		handlingGauge,
		activePullsGauge,
		pullExpiryCounter,
		pullErrorCounter,
		maxWaitingRejectsCounter,
		heartbeatFailuresCounter,
		ackCounter,
		nackCounter,
		dupClaimsCounter,
		slotInvariantCounter,
	)
	return err
}
