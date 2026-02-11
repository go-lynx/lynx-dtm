package dtm

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/go-lynx/lynx-dtm/conf"
)

// ValidateConfig validates DTM configuration. Returns error if invalid.
func ValidateConfig(c *conf.DTM) error {
	if c == nil {
		return fmt.Errorf("DTM configuration is nil")
	}

	if c.GetEnabled() {
		if c.GetServerUrl() == "" {
			return fmt.Errorf("server_url is required when enabled")
		}
		u, err := url.Parse(c.GetServerUrl())
		if err != nil {
			return fmt.Errorf("invalid server_url: %w", err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("server_url must use http or https scheme")
		}
	}

	if c.GetTimeout() < 0 {
		return fmt.Errorf("timeout cannot be negative")
	}
	if c.GetRetryInterval() < 0 {
		return fmt.Errorf("retry_interval cannot be negative")
	}
	if c.GetTransactionTimeout() < 0 {
		return fmt.Errorf("transaction_timeout cannot be negative")
	}
	if c.GetBranchTimeout() < 0 {
		return fmt.Errorf("branch_timeout cannot be negative")
	}
	if c.GetTimeout() > 0 && c.GetTimeout() < 1 {
		return fmt.Errorf("timeout should be at least 1 second")
	}
	if c.GetTransactionTimeout() > 0 && c.GetTransactionTimeout() < 1 {
		return fmt.Errorf("transaction_timeout should be at least 1 second")
	}

	if c.GetGrpcTlsEnabled() {
		if c.GetGrpcCertFile() == "" || c.GetGrpcKeyFile() == "" {
			return fmt.Errorf("grpc_cert_file and grpc_key_file are required when grpc_tls_enabled is true")
		}
	}

	if c.GetMaxConnectionRetries() < 0 {
		return fmt.Errorf("max_connection_retries cannot be negative")
	}

	return nil
}

// mergeBranchHeaders merges pass_through_headers from config with custom headers.
// Custom headers take precedence. Callers should populate custom from request context for pass-through.
func mergeBranchHeaders(passThrough []string, custom map[string]string) map[string]string {
	if len(passThrough) == 0 && len(custom) == 0 {
		return nil
	}
	out := make(map[string]string)
	for _, h := range passThrough {
		h = strings.TrimSpace(h)
		if h != "" {
			out[h] = "" // Value from request context when available
		}
	}
	for k, v := range custom {
		out[k] = v
	}
	return out
}
