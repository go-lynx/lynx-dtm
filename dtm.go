package dtm

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/dtm-labs/client/dtmcli"
	"github.com/dtm-labs/client/dtmcli/dtmimp"
	"github.com/go-lynx/lynx-dtm/conf"
	"github.com/go-lynx/lynx/log"
	"github.com/go-lynx/lynx/plugins"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Plugin metadata
const (
	// pluginName is the unique identifier for the DTM plugin
	pluginName = "dtm.server"

	// pluginVersion indicates the current version of the DTM plugin
	pluginVersion = "v1.6.0-beta"

	// pluginDescription briefly describes the functionality of the DTM plugin
	pluginDescription = "DTM distributed transaction manager plugin for Lynx framework"

	// confPrefix is the configuration prefix used when loading DTM configuration
	confPrefix = "lynx.dtm"
)

// DTMClient represents the DTM client plugin
type DTMClient struct {
	// Embed base plugin, inherit common properties and methods of the plugin
	*plugins.BasePlugin
	// DTM configuration information
	conf *conf.DTM
	rt   plugins.Runtime
	// DTM server URL for HTTP client
	serverURL string
	// gRPC connection for DTM
	grpcConn *grpc.ClientConn
	// gRPC server address
	grpcServer string
}

// NewDTMClient creates a new DTM plugin instance
func NewDTMClient() *DTMClient {
	return &DTMClient{
		BasePlugin: plugins.NewBasePlugin(
			// Generate unique plugin ID
			plugins.GeneratePluginID("", pluginName, pluginVersion),
			// Plugin name
			pluginName,
			// Plugin description
			pluginDescription,
			// Plugin version
			pluginVersion,
			// Configuration prefix
			confPrefix,
			// Weight
			90,
		),
	}
}

func (d *DTMClient) InitializeContext(ctx context.Context, plugin plugins.Plugin, rt plugins.Runtime) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context canceled before DTM initialize: %w", err)
	}
	return d.BasePlugin.Initialize(plugin, rt)
}

func (d *DTMClient) StartContext(ctx context.Context, _ plugins.Plugin) error {
	return d.startupWithContext(ctx)
}

func (d *DTMClient) StopContext(ctx context.Context, _ plugins.Plugin) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context canceled before DTM stop: %w", err)
	}
	return d.cleanupWithContext(ctx)
}

func (d *DTMClient) IsContextAware() bool {
	return true
}

func (d *DTMClient) PluginProtocol() plugins.PluginProtocol {
	protocol := d.BasePlugin.PluginProtocol()
	protocol.ContextLifecycle = true
	return protocol
}

// InitializeResources method is used to load and initialize the DTM plugin
func (d *DTMClient) InitializeResources(rt plugins.Runtime) error {
	if err := d.BasePlugin.InitializeResources(rt); err != nil {
		return err
	}
	d.rt = rt
	// Initialize an empty configuration structure
	d.conf = &conf.DTM{}

	// Scan and load DTM configuration from runtime configuration
	err := rt.GetConfig().Value(confPrefix).Scan(d.conf)
	if err != nil {
		return err
	}

	// Set default configuration
	if d.conf.ServerUrl == "" {
		d.conf.ServerUrl = "http://localhost:36789/api/dtmsvr"
	}
	if d.conf.Timeout == 0 {
		d.conf.Timeout = 10
	}
	if d.conf.RetryInterval == 0 {
		d.conf.RetryInterval = 10
	}
	if d.conf.TransactionTimeout == 0 {
		d.conf.TransactionTimeout = 60
	}
	if d.conf.BranchTimeout == 0 {
		d.conf.BranchTimeout = 30
	}
	if d.conf.MaxConnectionRetries == 0 {
		d.conf.MaxConnectionRetries = 3
	}

	if err := ValidateConfig(d.conf); err != nil {
		return fmt.Errorf("DTM config validation failed: %w", err)
	}

	return nil
}

// StartupTasks starts the DTM client
func (d *DTMClient) StartupTasks() error {
	return d.startupWithContext(context.Background())
}

func (d *DTMClient) startupWithContext(ctx context.Context) error {
	log.Infof("Initializing DTM client")
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context canceled before DTM startup: %w", err)
	}

	if !d.conf.GetEnabled() {
		log.Infof("DTM client is disabled")
		return nil
	}

	// Store server URL for HTTP client
	if d.conf.ServerUrl != "" {
		d.serverURL = d.conf.ServerUrl
		log.Infof("DTM HTTP client configured with server: %s", d.conf.ServerUrl)
	}

	// Initialize gRPC connection if configured (with retry)
	if d.conf.GrpcServer != "" {
		maxRetries := int(d.conf.MaxConnectionRetries)
		if maxRetries < 1 {
			maxRetries = 1
		}
		var err error
		for attempt := 0; attempt < maxRetries; attempt++ {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("DTM startup canceled during gRPC initialization: %w", err)
			}
			if attempt > 0 {
				backoff := time.Duration(attempt) * 2 * time.Second
				log.Infof("Retrying DTM gRPC connection in %v (attempt %d/%d)", backoff, attempt+1, maxRetries)
				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					return fmt.Errorf("DTM startup canceled during retry backoff: %w", ctx.Err())
				case <-timer.C:
				}
			}
			d.grpcConn, err = d.dialGrpcContext(ctx)
			if err == nil {
				d.grpcServer = d.conf.GrpcServer
				log.Infof("DTM gRPC client initialized with server: %s", d.conf.GrpcServer)
				break
			}
			log.Warnf("DTM gRPC connection attempt %d failed: %v", attempt+1, err)
		}
		if d.grpcConn == nil {
			return fmt.Errorf("failed to connect to DTM gRPC server after %d attempts: %w", maxRetries, err)
		}
	}

	if d.rt != nil {
		if err := d.rt.RegisterSharedResource(pluginName, d); err != nil {
			return fmt.Errorf("failed to register DTM shared resource: %w", err)
		}
		if err := d.rt.RegisterPrivateResource("config", d.conf); err != nil {
			log.Warnf("failed to register DTM private config resource: %v", err)
		}
		if d.serverURL != "" {
			if err := d.rt.RegisterPrivateResource("server_url", d.serverURL); err != nil {
				log.Warnf("failed to register DTM private server URL resource: %v", err)
			}
		}
		if d.grpcConn != nil {
			if err := d.rt.RegisterPrivateResource("grpc_connection", d.grpcConn); err != nil {
				log.Warnf("failed to register DTM private gRPC connection resource: %v", err)
			}
		}
		if d.grpcServer != "" {
			if err := d.rt.RegisterPrivateResource("grpc_server", d.grpcServer); err != nil {
				log.Warnf("failed to register DTM private gRPC server resource: %v", err)
			}
		}
		if metrics := d.getMetrics(); metrics != nil {
			if err := d.rt.RegisterPrivateResource("metrics", metrics); err != nil {
				log.Warnf("failed to register DTM private metrics resource: %v", err)
			}
		}
	}

	log.Infof("DTM client successfully initialized")
	return nil
}

// CleanupTasks cleans up DTM client resources
func (d *DTMClient) CleanupTasks() error {
	return d.cleanupWithContext(context.Background())
}

func (d *DTMClient) cleanupWithContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context canceled before DTM cleanup: %w", err)
	}
	if d.grpcConn == nil {
		return nil
	}

	conn := d.grpcConn
	d.grpcConn = nil
	d.grpcServer = ""

	if err := conn.Close(); err != nil {
		log.Errorf("Failed to close gRPC connection: %v", err)
		return err
	}
	return nil
}

// dialGrpc creates gRPC connection with optional TLS
func (d *DTMClient) dialGrpc() (*grpc.ClientConn, error) {
	return d.dialGrpcContext(context.Background())
}

func (d *DTMClient) dialGrpcContext(ctx context.Context) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100 * 1024 * 1024)),
	}

	if d.conf.GetGrpcTlsEnabled() {
		tlsConfig, err := buildGrpcTLSConfig(
			d.conf.GetGrpcCertFile(),
			d.conf.GetGrpcKeyFile(),
			d.conf.GetGrpcCaFile(),
		)
		if err != nil {
			return nil, fmt.Errorf("build TLS config: %w", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	return grpc.DialContext(ctx, d.conf.GrpcServer, opts...)
}

// buildGrpcTLSConfig builds TLS config from certificate files
func buildGrpcTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	config := &tls.Config{}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
	}

	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA certificate: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("parse CA certificate")
		}
		config.RootCAs = caCertPool
	}

	return config, nil
}

// GetServerURL returns the DTM server URL
func (d *DTMClient) GetServerURL() string {
	return d.serverURL
}

// GetGRPCServer returns the gRPC server address
func (d *DTMClient) GetGRPCServer() string {
	return d.grpcServer
}

// NewSaga creates a new SAGA transaction
func (d *DTMClient) NewSaga(gid string) *dtmcli.Saga {
	if d.serverURL == "" {
		log.Errorf("DTM server URL is not configured")
		return nil
	}
	if d.conf == nil {
		log.Errorf("DTM configuration not initialized")
		return nil
	}
	saga := dtmcli.NewSaga(d.serverURL, gid)
	saga.TimeoutToFail = int64(d.conf.TransactionTimeout)
	saga.RequestTimeout = int64(d.conf.Timeout)
	saga.RetryInterval = int64(d.conf.RetryInterval)
	return saga
}

// NewMsg creates a new 2-phase message transaction
func (d *DTMClient) NewMsg(gid string) *dtmcli.Msg {
	if d.serverURL == "" {
		log.Errorf("DTM server URL is not configured")
		return nil
	}
	if d.conf == nil {
		log.Errorf("DTM configuration not initialized")
		return nil
	}
	msg := dtmcli.NewMsg(d.serverURL, gid)
	msg.TimeoutToFail = int64(d.conf.TransactionTimeout)
	msg.RequestTimeout = int64(d.conf.Timeout)
	msg.RetryInterval = int64(d.conf.RetryInterval)
	return msg
}

// NewTcc creates a new TCC transaction
func (d *DTMClient) NewTcc(gid string) *dtmcli.Tcc {
	if d.serverURL == "" {
		log.Errorf("DTM server URL is not configured")
		return nil
	}
	if d.conf == nil {
		log.Errorf("DTM configuration not initialized")
		return nil
	}
	tcc := &dtmcli.Tcc{TransBase: *dtmimp.NewTransBase(gid, "tcc", d.serverURL, "")}
	tcc.TimeoutToFail = int64(d.conf.TransactionTimeout)
	tcc.RequestTimeout = int64(d.conf.Timeout)
	tcc.RetryInterval = int64(d.conf.RetryInterval)
	return tcc
}

// NewXa creates a new XA transaction
func (d *DTMClient) NewXa(gid string) *dtmcli.Xa {
	if d.serverURL == "" {
		log.Errorf("DTM server URL is not configured")
		return nil
	}
	if d.conf == nil {
		log.Errorf("DTM configuration not initialized")
		return nil
	}
	xa := &dtmcli.Xa{TransBase: *dtmimp.NewTransBase(gid, "xa", d.serverURL, "")}
	xa.TimeoutToFail = int64(d.conf.TransactionTimeout)
	xa.RequestTimeout = int64(d.conf.Timeout)
	xa.RetryInterval = int64(d.conf.RetryInterval)
	return xa
}

// GenerateGid generates a new global transaction ID.
// Returns empty string if DTM server is unreachable (avoids panic from dtmcli.MustGenGid).
func (d *DTMClient) GenerateGid() string {
	if d.serverURL == "" {
		log.Errorf("DTM server URL is not configured")
		return ""
	}
	var gid string
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("GenerateGid panic recovered (DTM server may be unreachable): %v", r)
				if m := d.getMetrics(); m != nil {
					m.RecordGidRequest("failed")
				}
			}
		}()
		gid = dtmcli.MustGenGid(d.serverURL)
		if m := d.getMetrics(); m != nil {
			m.RecordGidRequest("success")
		}
	}()
	return gid
}

// CallBranch is deprecated. It does not perform actual TCC branch calls.
// Use dtmcli.TccGlobalTransaction with tcc.CallBranch, or TransactionHelper.ExecuteTCC instead.
func (d *DTMClient) CallBranch(_ context.Context, _ interface{}, _, _, _ string) (*dtmcli.BranchBarrier, error) {
	return nil, fmt.Errorf("%w: use dtmcli.TccGlobalTransaction or TransactionHelper.ExecuteTCC for TCC branches", ErrNotImplemented)
}

// GetConfig returns the DTM configuration
func (d *DTMClient) GetConfig() *conf.DTM {
	return d.conf
}

// IsEnabled returns whether the DTM plugin is enabled.
func (d *DTMClient) IsEnabled() bool {
	return d.conf != nil && d.conf.GetEnabled()
}

// Configure updates the plugin configuration at runtime
func (d *DTMClient) Configure(c any) error {
	if c == nil {
		return nil
	}
	cfg, ok := c.(*conf.DTM)
	if !ok {
		return fmt.Errorf("invalid configuration type: expected *conf.DTM, got %T", c)
	}
	d.conf = cfg
	if cfg.ServerUrl != "" {
		d.serverURL = cfg.ServerUrl
	}
	return nil
}
