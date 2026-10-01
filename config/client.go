package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Config holds connection parameters for the Temporal Service.
// By default, it reads from environment variables, enabling direct connection
// to Temporal Cloud or any remote managed Temporal cluster without local setup.
type Config struct {
	HostPort  string // e.g. "my-namespace.a1b2c.tmprl.cloud:7233" or "localhost:7233"
	Namespace string // e.g. "my-namespace.a1b2c" or "default"
	CertPath  string // Path to TLS client certificate (.pem / .crt) for mTLS
	KeyPath   string // Path to TLS client private key (.key) for mTLS
	APIKey    string // Temporal Cloud API Key (if using API key auth instead of mTLS)
	Insecure  bool   // Set true only for unencrypted dev endpoints (e.g. remote dev server without TLS)
}

// LoadFromEnv populates Config from standard environment variables.
func LoadFromEnv() Config {
	hostPort := os.Getenv("TEMPORAL_HOST_PORT")
	if hostPort == "" {
		hostPort = "localhost:7233"
	}

	namespace := os.Getenv("TEMPORAL_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	return Config{
		HostPort:  hostPort,
		Namespace: namespace,
		CertPath:  os.Getenv("TEMPORAL_TLS_CERT_PATH"),
		KeyPath:   os.Getenv("TEMPORAL_TLS_KEY_PATH"),
		APIKey:    os.Getenv("TEMPORAL_API_KEY"),
		Insecure:  os.Getenv("TEMPORAL_INSECURE") == "true",
	}
}

// NewTemporalClient initializes a client connected to the Temporal Service.
// It handles:
// 1. Temporal Cloud with mTLS (Cert + Key)
// 2. Temporal Cloud with API Key authentication
// 3. Remote secure Temporal clusters (standard TLS)
// 4. Remote/Local plaintext gRPC connections
func NewTemporalClient(cfg Config) (client.Client, error) {
	clientOptions := client.Options{
		HostPort:  cfg.HostPort,
		Namespace: cfg.Namespace,
	}

	// 1. Handle mTLS authentication (Temporal Cloud standard)
	if cfg.CertPath != "" && cfg.KeyPath != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertPath, cfg.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load mTLS key pair: %w", err)
		}

		serverName := strings.Split(cfg.HostPort, ":")[0]
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			ServerName:   serverName,
			MinVersion:   tls.VersionTLS13,
		}

		clientOptions.ConnectionOptions = client.ConnectionOptions{
			TLS: tlsConfig,
		}
	} else if cfg.APIKey != "" {
		// 2. Handle Temporal Cloud API Key authentication
		serverName := strings.Split(cfg.HostPort, ":")[0]
		tlsConfig := &tls.Config{
			ServerName: serverName,
			MinVersion: tls.VersionTLS12,
		}

		// Attach API key as gRPC metadata interceptor
		clientOptions.ConnectionOptions = client.ConnectionOptions{
			TLS: tlsConfig,
			DialOptions: []grpc.DialOption{
				grpc.WithUnaryInterceptor(func(
					ctx context.Context,
					method string,
					req, reply interface{},
					cc *grpc.ClientConn,
					invoker grpc.UnaryInvoker,
					opts ...grpc.CallOption,
				) error {
					md := metadata.Pairs(
						"temporal-namespace", cfg.Namespace,
						"authorization", "Bearer "+cfg.APIKey,
					)
					ctxWithMD := metadata.NewOutgoingContext(ctx, md)
					return invoker(ctxWithMD, method, req, reply, cc, opts...)
				}),
			},
		}
	} else if !cfg.Insecure && strings.Contains(cfg.HostPort, "tmprl.cloud") {
		// Temporal Cloud endpoint detected without certs or API key
		return nil, fmt.Errorf("temporal cloud host detected (%s) but neither mTLS certs nor TEMPORAL_API_KEY were provided", cfg.HostPort)
	} else if !cfg.Insecure && strings.HasSuffix(cfg.HostPort, ":443") {
		// Remote cluster with standard public TLS
		systemRoots, _ := x509.SystemCertPool()
		serverName := strings.Split(cfg.HostPort, ":")[0]
		clientOptions.ConnectionOptions = client.ConnectionOptions{
			TLS: &tls.Config{
				RootCAs:    systemRoots,
				ServerName: serverName,
			},
		}
	}

	c, err := client.Dial(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to dial Temporal service at %s: %w", cfg.HostPort, err)
	}

	return c, nil
}
