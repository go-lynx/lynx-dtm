package dtm

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dtm-labs/client/dtmcli"
	"github.com/go-lynx/lynx/log"
)

// CheckHealth performs a health check of the DTM client.
// When disabled, returns nil. When enabled, probes DTM server via /newGid or /query.
func (d *DTMClient) CheckHealth() error {
	if !d.IsEnabled() {
		return nil
	}

	if d.conf == nil {
		return fmt.Errorf("DTM configuration is nil")
	}

	if d.serverURL == "" {
		return fmt.Errorf("DTM server URL is not configured")
	}

	// Probe DTM server: try newGid (lightweight, validates HTTP connectivity)
	baseURL := strings.TrimSuffix(d.serverURL, "/")
	client := dtmcli.GetRestyClient()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.R().SetContext(ctx).Get(baseURL + "/newGid")
	if err != nil {
		if m := d.getMetrics(); m != nil {
			m.RecordHealthCheck("error")
		}
		return fmt.Errorf("DTM health check failed (newGid): %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		if m := d.getMetrics(); m != nil {
			m.RecordHealthCheck("error")
		}
		return fmt.Errorf("DTM health check failed: HTTP %d", resp.StatusCode())
	}

	body := strings.TrimSpace(resp.String())
	if body == "" {
		if m := d.getMetrics(); m != nil {
			m.RecordHealthCheck("error")
		}
		return fmt.Errorf("DTM health check failed: empty newGid response")
	}

	if m := d.getMetrics(); m != nil {
		m.RecordHealthCheck("success")
	}

	log.Debugf("DTM client health check passed (server: %s)", d.serverURL)
	return nil
}
