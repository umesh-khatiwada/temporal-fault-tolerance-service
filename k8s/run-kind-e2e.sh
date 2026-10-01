#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Master KinD Cluster Script for Temporal End-to-End Orchestration
# ==============================================================================
# Usage:
#   ./k8s/run-kind-e2e.sh          # Full end-to-end: setup cluster, deploy & test
#   ./k8s/run-kind-e2e.sh test     # Re-run end-to-end workflow test Job
#   ./k8s/run-kind-e2e.sh status   # Show cluster resources and UI access info
#   ./k8s/run-kind-e2e.sh destroy  # Teardown the cluster namespaces
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-temporal-cluster}"
WORKER_IMAGE="temporal-worker:local"
STARTER_IMAGE="temporal-starter:local"

# Colors for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    log_info "Verifying prerequisites (docker, kind, kubectl)..."
    for tool in docker kind kubectl; do
        if ! command -v "$tool" &>/dev/null; then
            log_error "Missing required tool: $tool. Please install it before running this script."
            exit 1
        fi
    done
    if ! docker info &>/dev/null; then
        log_error "Docker daemon is not running or current user lacks docker permissions."
        exit 1
    fi
    log_success "All prerequisites are satisfied."
}

setup_kind_cluster() {
    # Check if target cluster already exists
    if kind get clusters 2>/dev/null | grep -wq "^${CLUSTER_NAME}$"; then
        log_info "KinD cluster '${CLUSTER_NAME}' already exists. Reusing it."
    else
        log_info "Attempting to create KinD cluster '${CLUSTER_NAME}'..."
        if ! kind create cluster --name "${CLUSTER_NAME}" --config "${SCRIPT_DIR}/kind-config.yaml" 2>/dev/null; then
            log_warn "Could not create new KinD cluster '${CLUSTER_NAME}' (likely host inotify instance limit: max_user_instances=128)."
            # Discover any existing active KinD cluster
            ACTIVE_CLUSTER=$(docker ps --filter "name=-control-plane" --format "{{.Names}}" | head -n 1 | sed 's/-control-plane//' || true)
            if [ -n "$ACTIVE_CLUSTER" ]; then
                log_info "Discovered active running KinD cluster: '${ACTIVE_CLUSTER}'. Using it for deployment."
                CLUSTER_NAME="${ACTIVE_CLUSTER}"
            else
                log_error "No active KinD cluster available and creation failed."
                exit 1
            fi
        else
            log_success "KinD cluster '${CLUSTER_NAME}' created successfully."
        fi
    fi

    log_info "Setting kubectl context to 'kind-${CLUSTER_NAME}'..."
    kubectl config use-context "kind-${CLUSTER_NAME}"
}

build_and_load_images() {
    log_info "Building static Linux amd64 Go binaries..."
    export PATH="${HOME}/.local/bin:${HOME}/.local/go/bin:${PATH}"

    mkdir -p "${ROOT_DIR}/bin"
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o "${ROOT_DIR}/bin/worker" "${ROOT_DIR}/cmd/worker"
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o "${ROOT_DIR}/bin/starter" "${ROOT_DIR}/cmd/starter"

    log_info "Building Docker images for Worker and Starter CLI..."
    docker build -t "${WORKER_IMAGE}" -f "${SCRIPT_DIR}/Dockerfile.worker" "${ROOT_DIR}"
    docker build -t "${STARTER_IMAGE}" -f "${SCRIPT_DIR}/Dockerfile.starter" "${ROOT_DIR}"

    log_info "Loading application images into KinD cluster '${CLUSTER_NAME}'..."
    kind load docker-image "${WORKER_IMAGE}" --name "${CLUSTER_NAME}"
    kind load docker-image "${STARTER_IMAGE}" --name "${CLUSTER_NAME}"

    # Ensure system images are present in node
    NODE_NAME="${CLUSTER_NAME}-control-plane"
    log_info "Ensuring system images in node '${NODE_NAME}'..."
    docker exec "${NODE_NAME}" crictl pull postgres:15-alpine || true
    docker exec "${NODE_NAME}" crictl pull temporalio/auto-setup:latest || true
    docker exec "${NODE_NAME}" crictl pull temporalio/ui:latest || true

    log_success "All images are loaded and verified in cluster."
}

deploy_temporal_system() {
    log_info "Applying namespaces (temporal and temporal-app)..."
    kubectl apply -f "${SCRIPT_DIR}/00-namespace.yaml"

    log_info "Deploying PostgreSQL and Temporal Server..."
    kubectl apply -f "${SCRIPT_DIR}/01-temporal-server.yaml"

    log_info "Deploying Temporal Web UI..."
    kubectl apply -f "${SCRIPT_DIR}/02-temporal-ui.yaml"

    log_info "Waiting for PostgreSQL pod to be ready..."
    kubectl wait --namespace temporal --for=condition=ready pod -l app.kubernetes.io/name=postgres --timeout=120s

    log_info "Waiting for Temporal Server (schema init & gRPC service) to be ready..."
    kubectl wait --namespace temporal --for=condition=ready pod -l app.kubernetes.io/name=temporal-server --timeout=180s

    log_info "Waiting for Temporal Web UI to be ready..."
    kubectl wait --namespace temporal --for=condition=ready pod -l app.kubernetes.io/name=temporal-ui --timeout=90s

    log_success "Temporal Service is fully operational in namespace 'temporal'."
}

deploy_worker_fleet() {
    log_info "Deploying Temporal Go Worker fleet (2 replicas)..."
    kubectl apply -f "${SCRIPT_DIR}/03-worker-deployment.yaml"

    log_info "Waiting for Worker pods to become ready..."
    kubectl wait --namespace temporal-app --for=condition=ready pod -l app.kubernetes.io/name=temporal-worker --timeout=90s

    log_success "Temporal Workers are up and polling task queue 'TEMPORAL_LEARNING_TASK_QUEUE'."
}

run_e2e_tests() {
    log_info "Launching End-to-End Test Job in Kubernetes..."
    kubectl delete job temporal-e2e-test -n temporal-app --ignore-not-found &>/dev/null || true

    kubectl apply -f "${SCRIPT_DIR}/04-test-job.yaml"

    log_info "Awaiting test job completion (executing Transfer, Saga rollback, and Batch workflows)..."
    if ! kubectl wait --namespace temporal-app --for=condition=complete job/temporal-e2e-test --timeout=120s; then
        log_error "Test Job failed or timed out! Pod logs:"
        kubectl logs -n temporal-app -l app.kubernetes.io/name=temporal-e2e-test --tail=100
        exit 1
    fi

    echo ""
    log_success "=========================================================="
    log_success " E2E TEST JOB LOGS FROM KUBERNETES POD:"
    log_success "=========================================================="
    kubectl logs -n temporal-app -l app.kubernetes.io/name=temporal-e2e-test
    echo ""
}

show_status() {
    echo ""
    echo "=========================================================="
    echo "  TEMPORAL KUBERNETES DEPLOYMENT STATUS"
    echo "=========================================================="
    kubectl get pods -n temporal -o wide
    echo ""
    kubectl get pods -n temporal-app -o wide
    echo ""
    echo "=========================================================="
    echo "  ACCESS INSTRUCTIONS"
    echo "=========================================================="
    echo -e "Port-Forward Web UI:   ${GREEN}kubectl port-forward -n temporal svc/temporal-ui 8233:8080${NC}"
    echo -e "Open in Browser:       ${GREEN}http://localhost:8233${NC}"
    echo ""
    echo -e "Port-Forward gRPC:     ${GREEN}kubectl port-forward -n temporal svc/temporal-frontend 7233:7233${NC}"
    echo ""
    echo "Trigger workflows from your local machine:"
    echo "  TEMPORAL_HOST_PORT=\"localhost:7233\" TEMPORAL_INSECURE=\"true\" go run ./cmd/starter transfer"
    echo "  TEMPORAL_HOST_PORT=\"localhost:7233\" TEMPORAL_INSECURE=\"true\" go run ./cmd/starter saga --fail"
    echo "  TEMPORAL_HOST_PORT=\"localhost:7233\" TEMPORAL_INSECURE=\"true\" go run ./cmd/starter order"
    echo ""
    echo "To teardown Temporal resources:"
    echo "  kubectl delete namespace temporal temporal-app"
    echo "=========================================================="
}

destroy_resources() {
    log_warn "Deleting 'temporal' and 'temporal-app' namespaces..."
    kubectl delete namespace temporal temporal-app --ignore-not-found || true
    log_success "Temporal namespaces cleaned up."
}

# Routing
ACTION="${1:-all}"

case "$ACTION" in
    all)
        check_prerequisites
        setup_kind_cluster
        build_and_load_images
        deploy_temporal_system
        deploy_worker_fleet
        run_e2e_tests
        show_status
        ;;
    test)
        check_prerequisites
        kubectl config use-context "kind-${CLUSTER_NAME}"
        run_e2e_tests
        ;;
    status)
        check_prerequisites
        kubectl config use-context "kind-${CLUSTER_NAME}"
        show_status
        ;;
    destroy)
        destroy_resources
        ;;
    *)
        echo "Usage: $0 [all|test|status|destroy]"
        exit 1
        ;;
esac
