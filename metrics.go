package dtm

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// DtmMetrics defines DTM-related monitoring metrics for production observability.
type DtmMetrics struct {
	transactionsTotal      *prometheus.CounterVec
	transactionsActive     prometheus.Gauge
	transactionDuration    *prometheus.HistogramVec
	barrierOperationsTotal prometheus.Counter
	healthCheckTotal       *prometheus.CounterVec
	gidRequestsTotal       *prometheus.CounterVec
}

var (
	dtmMetrics     *DtmMetrics
	dtmMetricsOnce sync.Once
)

// getMetrics returns the metrics instance (lazy init). Returns nil when disabled.
func (d *DTMClient) getMetrics() *DtmMetrics {
	if !d.IsEnabled() {
		return nil
	}
	dtmMetricsOnce.Do(func() {
		dtmMetrics = newDtmMetrics()
	})
	return dtmMetrics
}

func newDtmMetrics() *DtmMetrics {
	return &DtmMetrics{
		transactionsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lynx",
				Subsystem: "dtm",
				Name:      "transactions_total",
				Help:      "Total number of DTM transactions",
			},
			[]string{"type", "status"},
		),
		transactionsActive: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "lynx",
				Subsystem: "dtm",
				Name:      "active_transactions",
				Help:      "Number of active DTM transactions",
			},
		),
		transactionDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "lynx",
				Subsystem: "dtm",
				Name:      "transaction_duration_seconds",
				Help:      "Duration of DTM transactions",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"type", "status"},
		),
		barrierOperationsTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "lynx",
				Subsystem: "dtm",
				Name:      "barrier_operations_total",
				Help:      "Total number of DTM barrier operations",
			},
		),
		healthCheckTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lynx",
				Subsystem: "dtm",
				Name:      "health_check_total",
				Help:      "Total number of DTM health checks",
			},
			[]string{"status"},
		),
		gidRequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lynx",
				Subsystem: "dtm",
				Name:      "gid_requests_total",
				Help:      "Total number of GID generation requests",
			},
			[]string{"status"},
		),
	}
}

// RecordTransaction records a transaction completion.
func (m *DtmMetrics) RecordTransaction(txType, status string) {
	if m != nil && m.transactionsTotal != nil {
		m.transactionsTotal.WithLabelValues(txType, status).Inc()
	}
}

// RecordTransactionDuration records transaction duration.
func (m *DtmMetrics) RecordTransactionDuration(txType, status string, duration float64) {
	if m != nil && m.transactionDuration != nil {
		m.transactionDuration.WithLabelValues(txType, status).Observe(duration)
	}
}

// IncActiveTransactions increments active transaction count.
func (m *DtmMetrics) IncActiveTransactions() {
	if m != nil && m.transactionsActive != nil {
		m.transactionsActive.Inc()
	}
}

// DecActiveTransactions decrements active transaction count.
func (m *DtmMetrics) DecActiveTransactions() {
	if m != nil && m.transactionsActive != nil {
		m.transactionsActive.Dec()
	}
}

// IncBarrierOperations increments barrier operation count.
func (m *DtmMetrics) IncBarrierOperations() {
	if m != nil && m.barrierOperationsTotal != nil {
		m.barrierOperationsTotal.Inc()
	}
}

// RecordHealthCheck records a health check.
func (m *DtmMetrics) RecordHealthCheck(status string) {
	if m != nil && m.healthCheckTotal != nil {
		m.healthCheckTotal.WithLabelValues(status).Inc()
	}
}

// RecordGidRequest records a GID generation request.
func (m *DtmMetrics) RecordGidRequest(status string) {
	if m != nil && m.gidRequestsTotal != nil {
		m.gidRequestsTotal.WithLabelValues(status).Inc()
	}
}

// GetDtmMetrics returns the DTM metrics instance for external use.
func GetDtmMetrics() *DtmMetrics {
	return dtmMetrics
}
