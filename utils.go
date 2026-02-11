package dtm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dtm-labs/client/dtmcli"
	"github.com/dtm-labs/client/dtmgrpc"
	"github.com/go-lynx/lynx/log"
	"github.com/go-resty/resty/v2"
	"google.golang.org/grpc/metadata"
)

// TransactionType transaction type
type TransactionType string

const (
	// TransTypeSAGA SAGA transaction type
	TransTypeSAGA TransactionType = "saga"
	// TransTypeTCC TCC transaction type
	TransTypeTCC TransactionType = "tcc"
	// TransTypeMsg 2-phase message transaction type
	TransTypeMsg TransactionType = "msg"
	// TransTypeXA XA transaction type
	TransTypeXA TransactionType = "xa"
)

// TransactionOptions transaction options
type TransactionOptions struct {
	// Transaction timeout (seconds)
	TimeoutToFail int64
	// Branch timeout (seconds)
	BranchTimeout int64
	// Retry interval (seconds)
	RetryInterval int64
	// Custom request headers
	CustomHeaders map[string]string
	// Whether to wait for result
	WaitResult bool
	// Concurrent execution of branches
	Concurrent bool
}

// ExecuteXA execute XA transaction
func (h *TransactionHelper) ExecuteXA(ctx context.Context, gid string, branches []XABranch, opts *TransactionOptions) error {
	if opts == nil {
		opts = DefaultTransactionOptions()
	}

	start := time.Now()
	if m := h.client.getMetrics(); m != nil {
		m.IncActiveTransactions()
	}
	defer func() {
		if m := h.client.getMetrics(); m != nil {
			m.DecActiveTransactions()
		}
	}()

	// Use XA global transaction API
	err := dtmcli.XaGlobalTransaction(h.client.GetServerURL(), gid, func(xa *dtmcli.Xa) (*resty.Response, error) {
		// Set transaction options
		xa.TimeoutToFail = opts.TimeoutToFail
		xa.RequestTimeout = opts.BranchTimeout
		xa.RetryInterval = opts.RetryInterval

		// Call all XA branches
		for _, branch := range branches {
			_, err := xa.CallBranch(branch.Action, branch.Data)
			if err != nil {
				log.Errorf("XA branch failed: gid=%s, action=%s, error=%v", gid, branch.Action, err)
				return nil, err
			}
		}
		return nil, nil
	})

	if err != nil {
		log.Errorf("XA transaction failed: gid=%s, error=%v", gid, err)
		recordTransactionMetrics(h.client, "xa", "failed", start)
		return err
	}

	log.Infof("XA transaction submitted successfully: gid=%s", gid)

	// If need to wait for result
	if opts.WaitResult {
		err = h.waitTransactionResult(ctx, gid, TransTypeXA)
		recordTransactionMetrics(h.client, "xa", statusFromErr(err), start)
		return err
	}
	recordTransactionMetrics(h.client, "xa", "success", start)
	return nil
}

// DefaultTransactionOptions returns default transaction options
func DefaultTransactionOptions() *TransactionOptions {
	return &TransactionOptions{
		TimeoutToFail: 60,
		BranchTimeout: 30,
		RetryInterval: 10,
		WaitResult:    false,
		Concurrent:    false,
	}
}

// statusFromErr maps error to metrics status
func statusFromErr(err error) string {
	if err == nil {
		return "success"
	}
	if strings.Contains(err.Error(), "timeout") {
		return "timeout"
	}
	return "failed"
}

// recordTransactionMetrics records transaction metrics
func recordTransactionMetrics(client *DTMClient, txType, status string, start time.Time) {
	if m := client.getMetrics(); m != nil {
		m.RecordTransaction(txType, status)
		m.RecordTransactionDuration(txType, status, time.Since(start).Seconds())
	}
}

// TransactionHelper transaction helper tool
type TransactionHelper struct {
	client *DTMClient
}

// NewTransactionHelper create transaction helper tool
func NewTransactionHelper(client *DTMClient) *TransactionHelper {
	return &TransactionHelper{
		client: client,
	}
}

// ExecuteSAGA execute SAGA transaction
func (h *TransactionHelper) ExecuteSAGA(ctx context.Context, gid string, branches []SAGABranch, opts *TransactionOptions) error {
	if opts == nil {
		opts = DefaultTransactionOptions()
	}

	start := time.Now()
	if m := h.client.getMetrics(); m != nil {
		m.IncActiveTransactions()
	}
	defer func() {
		if m := h.client.getMetrics(); m != nil {
			m.DecActiveTransactions()
		}
	}()

	saga := h.client.NewSaga(gid)
	if saga == nil {
		recordTransactionMetrics(h.client, "saga", "failed", start)
		return fmt.Errorf("failed to create SAGA transaction")
	}

	saga.TimeoutToFail = opts.TimeoutToFail
	saga.RequestTimeout = opts.BranchTimeout
	saga.RetryInterval = opts.RetryInterval
	saga.Concurrent = opts.Concurrent

	// Add request headers (pass_through from config + custom)
	passThrough := []string(nil)
	if cfg := h.client.GetConfig(); cfg != nil {
		passThrough = cfg.GetPassThroughHeaders()
	}
	if headers := mergeBranchHeaders(passThrough, opts.CustomHeaders); len(headers) > 0 {
		saga.BranchHeaders = headers
	}

	// Add all branches
	for _, branch := range branches {
		saga.Add(branch.Action, branch.Compensate, branch.Data)
	}

	// Submit transaction
	err := saga.Submit()
	if err != nil {
		log.Errorf("SAGA transaction failed: gid=%s, error=%v", gid, err)
		recordTransactionMetrics(h.client, "saga", "failed", start)
		return err
	}

	log.Infof("SAGA transaction submitted successfully: gid=%s", gid)

	// If need to wait for result
	if opts.WaitResult {
		err = h.waitTransactionResult(ctx, gid, TransTypeSAGA)
		recordTransactionMetrics(h.client, "saga", statusFromErr(err), start)
		return err
	}
	recordTransactionMetrics(h.client, "saga", "success", start)
	return nil
}

// ExecuteTCC execute TCC transaction
func (h *TransactionHelper) ExecuteTCC(ctx context.Context, gid string, branches []TCCBranch, opts *TransactionOptions) error {
	if opts == nil {
		opts = DefaultTransactionOptions()
	}

	start := time.Now()
	if m := h.client.getMetrics(); m != nil {
		m.IncActiveTransactions()
	}
	defer func() {
		if m := h.client.getMetrics(); m != nil {
			m.DecActiveTransactions()
		}
	}()

	// Use the new TCC global transaction API
	err := dtmcli.TccGlobalTransaction(h.client.GetServerURL(), gid, func(tcc *dtmcli.Tcc) (*resty.Response, error) {
		// Set transaction options
		tcc.TimeoutToFail = opts.TimeoutToFail
		tcc.RequestTimeout = opts.BranchTimeout
		tcc.RetryInterval = opts.RetryInterval

		// Add request headers (pass_through from config + custom)
		passThrough := []string(nil)
		if cfg := h.client.GetConfig(); cfg != nil {
			passThrough = cfg.GetPassThroughHeaders()
		}
		if headers := mergeBranchHeaders(passThrough, opts.CustomHeaders); len(headers) > 0 {
			tcc.BranchHeaders = headers
		}

		// Call all Try branches
		for _, branch := range branches {
			_, err := tcc.CallBranch(branch.Data, branch.Try, branch.Confirm, branch.Cancel)
			if err != nil {
				log.Errorf("TCC branch failed: gid=%s, try=%s, error=%v", gid, branch.Try, err)
				return nil, err
			}
		}

		return nil, nil
	})

	if err != nil {
		log.Errorf("TCC transaction failed: gid=%s, error=%v", gid, err)
		recordTransactionMetrics(h.client, "tcc", "failed", start)
		return err
	}

	log.Infof("TCC transaction submitted successfully: gid=%s", gid)

	// If need to wait for result
	if opts.WaitResult {
		err = h.waitTransactionResult(ctx, gid, TransTypeTCC)
		recordTransactionMetrics(h.client, "tcc", statusFromErr(err), start)
		return err
	}
	recordTransactionMetrics(h.client, "tcc", "success", start)
	return nil
}

// ExecuteMsg execute 2-phase message transaction
func (h *TransactionHelper) ExecuteMsg(ctx context.Context, gid string, queryPrepared string, branches []MsgBranch, opts *TransactionOptions) error {
	if opts == nil {
		opts = DefaultTransactionOptions()
	}

	start := time.Now()
	if m := h.client.getMetrics(); m != nil {
		m.IncActiveTransactions()
	}
	defer func() {
		if m := h.client.getMetrics(); m != nil {
			m.DecActiveTransactions()
		}
	}()

	msg := h.client.NewMsg(gid)
	if msg == nil {
		recordTransactionMetrics(h.client, "msg", "failed", start)
		return fmt.Errorf("failed to create MSG transaction")
	}

	msg.TimeoutToFail = opts.TimeoutToFail
	msg.RequestTimeout = opts.BranchTimeout
	msg.RetryInterval = opts.RetryInterval

	// Add request headers (pass_through from config + custom)
	passThrough := []string(nil)
	if cfg := h.client.GetConfig(); cfg != nil {
		passThrough = cfg.GetPassThroughHeaders()
	}
	if headers := mergeBranchHeaders(passThrough, opts.CustomHeaders); len(headers) > 0 {
		msg.BranchHeaders = headers
	}

	// Add all branches
	for _, branch := range branches {
		msg.Add(branch.Action, branch.Data)
	}

	// Prepare message
	err := msg.Prepare(queryPrepared)
	if err != nil {
		log.Errorf("MSG prepare failed: gid=%s, error=%v", gid, err)
		recordTransactionMetrics(h.client, "msg", "failed", start)
		return err
	}

	// Submit transaction
	err = msg.Submit()
	if err != nil {
		log.Errorf("MSG transaction failed: gid=%s, error=%v", gid, err)
		recordTransactionMetrics(h.client, "msg", "failed", start)
		return err
	}

	log.Infof("MSG transaction submitted successfully: gid=%s", gid)

	// If need to wait for result
	if opts.WaitResult {
		err = h.waitTransactionResult(ctx, gid, TransTypeMsg)
		recordTransactionMetrics(h.client, "msg", statusFromErr(err), start)
		return err
	}
	recordTransactionMetrics(h.client, "msg", "success", start)
	return nil
}

// queryResult represents DTM server query API response (supports multiple formats)
type queryResult struct {
	Status      string `json:"status,omitempty"`
	DtmResult   string `json:"dtm_result,omitempty"`
	Transaction *struct {
		Status string `json:"status,omitempty"`
	} `json:"transaction,omitempty"`
}

// waitTransactionResult waits for transaction to complete by polling DTM query API
func (h *TransactionHelper) waitTransactionResult(ctx context.Context, gid string, transType TransactionType) error {
	timeout := 5 * time.Minute
	interval := 2 * time.Second
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		status, err := h.CheckTransactionStatus(gid)
		if err != nil {
			if errors.Is(err, ErrNotImplemented) {
				return fmt.Errorf("WaitResult requires CheckTransactionStatus: %w", err)
			}
			log.Debugf("CheckTransactionStatus failed, retrying: gid=%s, err=%v", gid, err)
		} else {
			switch strings.ToLower(status) {
			case "succeed", "success":
				return nil
			case "failed", "aborting":
				return fmt.Errorf("transaction %s: gid=%s, type=%s", status, gid, transType)
			}
			// prepared, submitted, ongoing - keep polling
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("transaction timeout: gid=%s, type=%s", gid, transType)
			}
		}
	}
}

// SAGABranch SAGA branch definition
type SAGABranch struct {
	Action     string      // Forward operation URL
	Compensate string      // Compensation operation URL
	Data       interface{} // Request data
}

// TCCBranch TCC branch definition
type TCCBranch struct {
	Try     string      // Try phase URL
	Confirm string      // Confirm phase URL
	Cancel  string      // Cancel phase URL
	Data    interface{} // Request data
}

// MsgBranch message branch definition
type MsgBranch struct {
	Action string      // Operation URL
	Data   interface{} // Request data
}

// XABranch XA branch definition
type XABranch struct {
	Action string // Operation URL
	Data   string // Request data as serialized string (e.g., JSON)
}

// CreateGrpcContext create gRPC Context containing transaction information
func CreateGrpcContext(ctx context.Context, gid string, transType string, branchID string, op string) context.Context {
	md := metadata.New(map[string]string{
		"dtm-gid":        gid,
		"dtm-trans-type": transType,
		"dtm-branch-id":  branchID,
		"dtm-op":         op,
	})
	return metadata.NewOutgoingContext(ctx, md)
}

// ExtractGrpcTransInfo extract transaction information from gRPC Context
func ExtractGrpcTransInfo(ctx context.Context) (*dtmcli.BranchBarrier, error) {
	// Use the built-in function from dtmgrpc package
	return dtmgrpc.BarrierFromGrpc(ctx)
}

// MustGenGid generates a global transaction ID. Returns empty string if generation fails
// (e.g. DTM server unreachable). Caller should check for empty and handle accordingly.
func (h *TransactionHelper) MustGenGid() string {
	gid := h.client.GenerateGid()
	if gid == "" {
		log.Errorf("Failed to generate transaction gid (DTM server may be unreachable)")
		return ""
	}
	return gid
}

// GenGid generates a transaction GID with error handling
func (h *TransactionHelper) GenGid() (string, error) {
	gid := h.client.GenerateGid()
	if gid == "" {
		return "", fmt.Errorf("failed to generate transaction gid")
	}
	return gid, nil
}

// CheckTransactionStatus queries DTM server for transaction status.
// Returns status: "succeed", "failed", "prepared", "submitted", "aborting", or "ongoing".
// Returns ErrNotImplemented if DTM query API is unavailable or returns unexpected format.
func (h *TransactionHelper) CheckTransactionStatus(gid string) (string, error) {
	baseURL := h.client.GetServerURL()
	if baseURL == "" {
		return "", fmt.Errorf("DTM server URL is not configured")
	}
	queryURL := strings.TrimSuffix(baseURL, "/") + "/query?gid=" + url.QueryEscape(gid)

	client := dtmcli.GetRestyClient()
	resp, err := client.R().Get(queryURL)
	if err != nil {
		return "", fmt.Errorf("query transaction status: %w", err)
	}
	if resp.StatusCode() == http.StatusNotFound {
		return "", fmt.Errorf("%w: DTM query API not found (HTTP 404), server may not support /query", ErrNotImplemented)
	}
	if resp.StatusCode() != http.StatusOK {
		return "", fmt.Errorf("query transaction status: HTTP %d", resp.StatusCode())
	}

	body := resp.String()
	bodyLower := strings.ToLower(body)

	// Try JSON parse first
	var qr queryResult
	if json.Unmarshal(resp.Body(), &qr) == nil {
		if s := qr.Status; s != "" {
			return strings.ToLower(s), nil
		}
		if s := qr.DtmResult; s != "" {
			return strings.ToLower(s), nil
		}
		if qr.Transaction != nil && qr.Transaction.Status != "" {
			return strings.ToLower(qr.Transaction.Status), nil
		}
	}

	// Fallback: parse plain text status (DTM may return raw status string)
	for _, s := range []string{"succeed", "success", "failed", "prepared", "submitted", "aborting", "ongoing"} {
		if strings.Contains(bodyLower, s) {
			return s, nil
		}
	}

	return "", fmt.Errorf("%w: DTM query returned unrecognized response: %s", ErrNotImplemented, body)
}

// RegisterGrpcService registers gRPC service to DTM. Not yet implemented.
func (h *TransactionHelper) RegisterGrpcService(serviceName string, endpoint string) error {
	if h.client.GetGRPCServer() == "" {
		return fmt.Errorf("gRPC server is not configured")
	}
	return fmt.Errorf("%w: RegisterGrpcService (name=%s, endpoint=%s)", ErrNotImplemented, serviceName, endpoint)
}
