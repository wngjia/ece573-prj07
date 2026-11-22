# ECE 473/573 Project 7: Resilience and Chaos Engineering

Project 7 extends the Project 6 shopping marketplace with a Postgres projection
outbox and a separate worker that updates Cassandra read views. It also installs
Chaos Mesh so you can inject repeatable Kubernetes Pod faults and compare the
system during a fault and after recovery.

## Ubuntu 24.04 VM Setup

This project requires an Ubuntu 24.04 LTS `amd64` VM and uses Go 1.26.
Use a fresh VM because the cluster needs ports 8080 and 8081.

From the `ece573-prj07` repository root, provision the VM with:

```bash
sudo ./setup_vm.sh
newgrp docker
```

The script installs and verifies Go 1.26.6, Git, `jq`, OpenSSL, Python 3,
Docker Engine, Buildx, `kind`, `kubectl`, and Helm. It also sets the host
inotify instance limit needed by the six kind nodes. `newgrp docker` activates
the Docker-group membership in the current terminal.

Verify the environment with:

```bash
go version
docker --version
docker buildx version
kind version
kubectl version --client
helm version --short
python3 --version
```

## Quick Start

From the `ece573-prj07` project root, run:

```bash
./create_cluster.sh
./deploy_apps.sh
./run_baseline.sh
```

`create_cluster.sh` creates one control-plane node and five worker nodes,
deploys Postgres, Cassandra, and Kafka, creates the `marketplace.commands`
topic, and installs Chaos Mesh in the `chaos-mesh` namespace. `deploy_apps.sh`
builds and loads the application images and waits for the Deployments. Both
scripts select the Project 7 `kubectl` context.

| Component | Pods | Role |
| --- | ---: | --- |
| Marketplace service | 1 | Accepts requests at `http://127.0.0.1:8080` |
| Login service | 1 | Serves account registration and login at `http://127.0.0.1:8081` |
| Marketplace worker | 1 | Consumes Kafka commands and commits business results to Postgres |
| Outbox worker | 1 | Projects committed results into Cassandra |
| Postgres | 1 | Stores business state, job outcomes, checkpoints, and outbox rows |
| Cassandra | 1 | Serves listing and account read views |
| Kafka | 1 | Stores commands in a four-partition topic |

The Kafka topic has replication factor 1, `min.insync.replicas=1`, and
seven-day retention. Cluster creation preserves existing database volumes and
consumer checkpoints on repeated runs. `destroy_cluster.sh` deletes the
Project 7 cluster and its data.

The baseline creates unique seller and buyer accounts, grants buyer credit,
lists an item, orders it, checks the account and listing views, and confirms
that a repeated order is rejected. It waits for the Cassandra views to catch
up and saves responses under ignored `out/`. Each run uses new account names;
it does not reset the databases.

## Individual Operations

The host scripts use ports 8080 and 8081, as in Project 6. Listing and order
scripts wait for the worker's result before printing it.

- `./scripts/create-account.sh Alice` creates an account with zero credits.
  It prompts for a password or reads one from stdin.
- `./scripts/login.sh Alice` prints a login response containing a JWT after
  reading Alice's password.
- `./scripts/admin-login.sh` logs in with the generated admin password.
- `./scripts/grant-credits.sh Bob 100` grants Bob 100 credits.
- `./scripts/list.sh Alice "Used textbook" 25` submits a listing and prints the
  completed listing or an error.
- `./scripts/browse.sh` returns available listings. An optional seller ID
  filters the result.
- `./scripts/order.sh listing-1 Bob` submits an order and prints the completed
  order or an error. Use the listing ID returned by `list.sh`.
- `./scripts/account.sh Bob 2000-01-01 2000-01-02` reads Bob's balance and
  activity, optionally within a date range.
- `./scripts/wait-job.sh Alice <job_id>` waits for a particular job result.
- `./deploy_apps.sh` rebuilds and restarts the application Pods after a code
  change while preserving database data.
- `./destroy_cluster.sh` deletes the Project 7 cluster and its data.

When `AUTH_ENABLED=true`, set `TOKEN` to the appropriate Bearer JWT for the
marketplace scripts. Browsing remains public. The supplied deployment sets
`AUTH_ENABLED=false` for the fault runner. Cluster setup generates signing
keys and an admin password under ignored `secrets/`.

## Source Structure

```text
kind-cluster.yaml                  one control-plane node, five workers, host ports
k8s/databases.yaml                Postgres and Cassandra Services and StatefulSets
k8s/kafka.yaml                    one Kafka broker/controller Pod
k8s/apps.yaml                     login, API, business worker, outbox worker
database/schema.sql               business, job, checkpoint, and outbox tables
create_cluster.sh                 create cluster, databases, Kafka, Chaos Mesh
deploy_apps.sh                    build images and deploy applications
run_baseline.sh                   end-to-end marketplace check
run_resilience.py                 one fault experiment and recovery snapshots
chaos/                            supplied PodChaos manifests
scripts/install-chaos-mesh.sh     install Chaos Mesh into its namespace
login-service/                    account registration and login
marketplace-service/              HTTP routes and Kafka producer
marketplace-worker/               Kafka consumer and Postgres business processing
outbox-worker/                    Cassandra projection from Postgres outbox
shared/model/                     request, command, job, and result types
shared/queue/                     Kafka producer and consumer configuration
shared/marketplace/               Postgres business, job, and outbox operations
shared/history/                   Cassandra read views
shared/bootstrap/                 common dependency setup
scripts/                          host-side request helpers
```

## REST API and Jobs

Project 7 keeps the Project 6 request bodies for `POST /listings` and
`POST /orders`. A successful submission returns HTTP 202 with a new `job_id`,
`status: queued`, and a `status_url`. This confirms Kafka accepted the command;
it does not confirm that a listing or order was committed. A producer failure
returns HTTP 503. The worker may later complete or reject an accepted job.

`GET /accounts/{account_id}/jobs/{job_id}` returns a committed job result.
Before the worker commits it, the endpoint may return HTTP 404. The host
`list.sh` and `order.sh` scripts use `wait-job.sh` to retry the status request
and print the eventual result. When authentication is enabled, the token
subject must match the account in the status URL.

`GET /listings` reads Cassandra. `GET /accounts/{account_id}` combines the
Postgres credit balance with Cassandra listing, order, and sales views.
`POST /accounts/{account_id}/credits` writes directly to Postgres. The
marketplace worker does not need Cassandra; the API still connects to it at
startup to serve read views.

## Worker Recovery and the Projection Outbox

The marketplace worker joins a Kafka consumer group and reads each assigned
partition's next offset from Postgres. For each command it commits the
business result, terminal job outcome, next offset, and, for a successful
result, a projection outbox row in one Postgres transaction. Kafka consumer
auto-commit is disabled. A worker that stops before the transaction commits
can replay the command; one that stops afterward resumes from the saved
checkpoint. Business rejections are terminal outcomes and create no outbox
row.

The outbox worker reads pending rows in order, writes the Cassandra views,
then marks each row delivered in Postgres. On a Postgres or Cassandra error,
it exits and Kubernetes restarts it. An undelivered row remains available for
replay. Cassandra writes use stable primary keys so replay is safe if the
worker stops after writing to Cassandra but before marking delivery. The
single outbox-worker replica and ordered reads keep older projections from
overtaking newer ones.

An HTTP 202 promises acceptance into Kafka, not a successful business result
or immediate Cassandra visibility. After a successful result commits in
Postgres, the outbox retains its projection until the outbox worker can apply
it. A temporary fault can delay processing or projection; recovery still
requires the affected component and its dependencies to become available.

## Chaos Mesh and Fault Experiments

Chaos Mesh is installed by `create_cluster.sh`. Verify its Pods separately
from the marketplace Pods:

```bash
kubectl get -n chaos-mesh services,pods
kubectl get services,statefulsets,pods
```

The supplied `chaos/cassandra-pod-failure.yaml` keeps one Cassandra Pod
unavailable for its declared duration. The supplied
`chaos/outbox-worker-pod-kill.yaml` kills one outbox-worker Pod once; Kubernetes
then replaces it. Both are Chaos Mesh `PodChaos` resources selecting Pods by
namespace and `app` label.

Run one manifest at a time with `run_resilience.py`:

```bash
./run_resilience.py chaos/cassandra-pod-failure.yaml
./run_resilience.py chaos/outbox-worker-pod-kill.yaml
```

The runner creates a new seller and submits 100 uniquely named listing
requests at two requests per second by default. It does not retry failed
POSTs. After half the submissions, it applies the manifest. It takes a
`checkpoint` after all HTTP responses or client timeouts finish, deletes the
fault, and waits up to 120 seconds for `after_recovery` convergence. It does
not wait for the business worker before taking the checkpoint. A timed fault
may expire before that snapshot; a longer fault ends when the runner deletes
it.

The terminal shows `accepted_202`, `processed`, `cassandra_listings`, and
`pending_outbox` for both snapshots. `processed` includes completed and
rejected Postgres jobs. The JSON report under `out/resilience-*.json` contains
the manifest path and counts for `checkpoint` and `after_recovery`, including
HTTP failures, unresolved jobs, and missing projections. An unavailable
database count is `null`, not zero. A lost HTTP response is recorded as
`transport_unknown` because Kafka may have accepted the command.

For a new experiment, copy a supplied manifest to a distinct filename,
change `metadata.name` and the `app` label selector, then run it by path. For
example, a Postgres fault should select `app: postgres`:

```bash
./run_resilience.py chaos/postgres-pod-failure.yaml
```

Create the example file before running that command. The project handout asks
for additional faults targeting Kafka, the marketplace worker, and Postgres.

For manual application and cleanup, use:

```bash
kubectl apply -f chaos/cassandra-pod-failure.yaml
kubectl get podchaos
kubectl delete -f chaos/cassandra-pod-failure.yaml
```

## Useful Inspection and Troubleshooting Commands

```bash
kind get nodes --name ece573-prj07
kubectl get -n chaos-mesh pods
kubectl get services,statefulsets,deployments,pods
kubectl get podchaos
kubectl logs deployment/marketplace-worker
kubectl logs deployment/outbox-worker
```
