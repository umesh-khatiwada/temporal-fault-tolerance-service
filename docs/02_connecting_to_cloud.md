# Module 02: Connecting to Temporal Service (Cloud & Remote) Without Local Setup

> **Official Documentation Reference:** [https://docs.temporal.io/cloud](https://docs.temporal.io/cloud)

---

## 1. Why You Don't Need Local Temporal Server Setup

Traditionally, running Temporal locally required spinning up:
- Temporal Server binary or Docker Compose
- PostgreSQL or Cassandra for persistence
- ElasticSearch / OpenSearch for advanced visibility
- UI Server

However, for modern development:
1. **You can connect directly to Temporal Cloud (Fully Managed Service)**.
2. **You can connect to a shared staging/remote cluster**.
3. **You can run complete unit & integration tests using the In-Memory SDK Test Environment (`testsuite`)** with zero server, zero Docker, and zero daemons.

This repository is pre-configured to connect to any remote Temporal Service via standard environment variables.

---

## 2. Option A: Connecting to Temporal Cloud

Temporal Cloud is a fully-managed serverless orchestration platform provided by Temporal Technologies.

### Step 1: Get Credentials from Temporal Cloud Console
1. Log in to [https://cloud.temporal.io](https://cloud.temporal.io).
2. Create or select a **Namespace** (e.g. `engineering-dev.a1b2c`).
3. Note your **gRPC Host & Port** (e.g. `engineering-dev.a1b2c.tmprl.cloud:7233`).

### Step 2: Choose Authentication Method

#### Method 1: mTLS (Mutual TLS Certificates - Recommended for Production)
Temporal Cloud uses mTLS by default:
1. Generate your client private key and certificate:
   ```bash
   openssl req -newkey rsa:2048 -nodes -keyout client.key -x509 -days 365 -out client.pem
   ```
2. Upload `client.pem` to your Temporal Cloud Namespace settings under **Certificates**.
3. Configure your environment:
   ```bash
   export TEMPORAL_HOST_PORT="engineering-dev.a1b2c.tmprl.cloud:7233"
   export TEMPORAL_NAMESPACE="engineering-dev.a1b2c"
   export TEMPORAL_TLS_CERT_PATH="/path/to/client.pem"
   export TEMPORAL_TLS_KEY_PATH="/path/to/client.key"
   ```

#### Method 2: API Key Authentication
Temporal Cloud also supports API Keys:
1. Generate an API Key under **Settings > API Keys** in Temporal Cloud.
2. Configure your environment:
   ```bash
   export TEMPORAL_HOST_PORT="engineering-dev.a1b2c.tmprl.cloud:7233"
   export TEMPORAL_NAMESPACE="engineering-dev.a1b2c"
   export TEMPORAL_API_KEY="tmprl_apikey_xxxxxxxxxxxxxxxxxxxxxx"
   ```

---

## 3. Option B: Connecting to an Internal / Remote Company Cluster

If your team or organization has an existing remote Temporal cluster:

```bash
# For a remote cluster with public TLS:
export TEMPORAL_HOST_PORT="temporal.mycompany.internal:7233"
export TEMPORAL_NAMESPACE="default"

# For an internal cluster without TLS:
export TEMPORAL_HOST_PORT="10.0.1.50:7233"
export TEMPORAL_NAMESPACE="default"
export TEMPORAL_INSECURE="true"
```

---

## 4. How the Code Connects: `config/client.go`

In this project, [`config/client.go`](file:///home/umesh-pc/Desktop/workspace/temporal/config/client.go) inspects the environment and dynamically constructs the proper `client.Options`:

```go
func NewTemporalClient(cfg Config) (client.Client, error) {
    clientOptions := client.Options{
        HostPort:  cfg.HostPort,
        Namespace: cfg.Namespace,
    }

    if cfg.CertPath != "" && cfg.KeyPath != "" {
        // Automatically configures mTLS for Temporal Cloud
        cert, _ := tls.LoadX509KeyPair(cfg.CertPath, cfg.KeyPath)
        clientOptions.ConnectionOptions.TLS = &tls.Config{
            Certificates: []tls.Certificate{cert},
            ServerName:   strings.Split(cfg.HostPort, ":")[0],
            MinVersion:   tls.VersionTLS13,
        }
    } else if cfg.APIKey != "" {
        // Automatically injects gRPC Bearer Authorization header
        clientOptions.ConnectionOptions.DialOptions = append(
            clientOptions.ConnectionOptions.DialOptions,
            grpc.WithUnaryInterceptor(apiKeyInterceptor(cfg.APIKey, cfg.Namespace)),
        )
    }

    return client.Dial(clientOptions)
}
```

---

## 5. Running the Worker & Starter Against the Remote Service

### Terminal 1: Run your Worker
```bash
export TEMPORAL_HOST_PORT="engineering-dev.a1b2c.tmprl.cloud:7233"
export TEMPORAL_NAMESPACE="engineering-dev.a1b2c"
export TEMPORAL_TLS_CERT_PATH="./client.pem"
export TEMPORAL_TLS_KEY_PATH="./client.key"

go run ./cmd/worker
```
Output:
```text
=== Starting Temporal Learning Worker ===
Connecting to Temporal Service at: engineering-dev.a1b2c.tmprl.cloud:7233 (Namespace: engineering-dev.a1b2c)
Worker successfully registered all workflows & activities on TaskQueue: 'TEMPORAL_LEARNING_TASK_QUEUE'
Worker is polling for tasks from Temporal Service...
```

### Terminal 2: Trigger Workflows via Starter CLI
```bash
# Trigger Fund Transfer:
go run ./cmd/starter transfer

# Trigger Order Approval with Signals:
go run ./cmd/starter order
go run ./cmd/starter query <order-workflow-id>
go run ./cmd/starter signal <order-workflow-id> yes

# Trigger Distributed Saga:
go run ./cmd/starter saga --fail
```

---

## 6. Zero-Server Local Development with `testsuite`

Don't have a Temporal Cloud account yet? **No problem.**
You can run and explore all workflows immediately using:

```bash
go test -v ./...
```

The Go SDK's `testsuite.WorkflowTestSuite` runs a complete, high-fidelity Temporal state engine in memory. It tests workflow determinism, timers, activity retries, signals, queries, and saga rollbacks with **sub-second execution**!
