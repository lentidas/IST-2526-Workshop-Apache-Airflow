# Airflow on Talos — Workshop Demo Notes

Running Apache Airflow (KubernetesExecutor) on a Talos Kubernetes cluster that runs as
Docker containers inside a colima VM on macOS.

Cluster facts:
- Talos v1.13.4, Kubernetes v1.36.1, arm64, containerd 2.2.4
- kube context: `admin@ist-airflow-demo`
- Nodes: 1 control-plane (`10.5.0.2`) + 3 workers (`10.5.0.3`–`10.5.0.5`)

> Convention: run every `kubectl` / `helm` command from this directory so the
> `-f` paths to the YAML files resolve.

---

## 1. Cluster creation (Talos on Docker, via colima)

On macOS there is no native Docker daemon, so Docker runs inside a **colima** VM and
Talos brings up each node as a Docker container in that engine. The one non-obvious
step is pointing the Talos CLI at colima's Docker socket via `DOCKER_HOST`.

```sh
# Make sure the colima VM (and its Docker engine) is running first.
colima start

# Point the Docker client / talosctl at colima's Docker socket.
export DOCKER_HOST=unix:///Users/goncaloheleno/.colima/arm64/docker.sock

# Create the cluster with the Docker provisioner: 1 control-plane + 3 workers.
# Talos/kube configs are written to dedicated files (kept out of the default paths).
talosctl cluster create docker \
  --workers 3 \
  --name ist-airflow-demo \
  --talosconfig-destination ~/.talos/ist-airflow-demo.yaml \
  --kubeconfig ~/.kube/talos-ist-airflow-demo.yaml
```

> Note: `talosctl cluster create docker` bootstraps the control plane and writes the
> kubeconfig automatically — no separate `talosctl bootstrap` / `talosctl kubeconfig`
> steps are needed with the Docker provisioner.

Point your tools at the generated configs (e.g. for this shell / session):

```sh
export TALOSCONFIG=~/.talos/ist-airflow-demo.yaml
export KUBECONFIG=~/.kube/talos-ist-airflow-demo.yaml
```

Sanity check the cluster:

```sh
kubectl config current-context          # -> admin@ist-airflow-demo
kubectl get nodes -o wide               # -> 1 control-plane + 3 workers
kubectl get storageclass                # -> none yet (we add these next)
```

### Tear down / recreate

To destroy the cluster and clean up its config files (useful when rebuilding from
scratch):

```sh
talosctl cluster destroy --name ist-airflow-demo
rm -rf ~/.talos/ist-airflow-demo.yaml
rm -rf ~/.talos/clusters/ist-airflow-demo
rm -f  ~/.kube/talos-ist-airflow-demo.yaml
```

---

## 2. Storage provisioners

The fresh cluster has **no StorageClass**. Airflow's PostgreSQL needs RWO storage, and
KubernetesExecutor task logs need an **RWX** shared volume, so we install two provisioners.

### 2a. local-path (default, ReadWriteOnce)

Rancher's local-path-provisioner, patched for Talos:
- export path moved under `/var` (Talos root filesystem is read-only),
- namespace labelled PSA `privileged` (the helper pod runs as root + hostPath),
- marked as the cluster default StorageClass.

Manifest: `storage/local-path-storage.yaml`

```sh
kubectl apply -f storage/local-path-storage.yaml
kubectl -n local-path-storage rollout status deploy/local-path-provisioner
kubectl get sc            # -> local-path (default)
```

### 2b. nfs (ReadWriteMany) via NFS-Ganesha

Userspace NFS server (Ganesha) so we don't depend on a kernel `nfsd` module (Talos
nodes don't expose it). It exposes an RWX StorageClass named `nfs`, backed by a
local-path PVC. Its namespace is PSA `privileged` (the server needs the
`DAC_READ_SEARCH` and `SYS_RESOURCE` capabilities).

Files: `storage/nfs-namespace.yaml`, `storage/nfs-values.yaml`

```sh
helm repo add nfs-ganesha-server-and-external-provisioner \
  https://kubernetes-sigs.github.io/nfs-ganesha-server-and-external-provisioner/
helm repo update nfs-ganesha-server-and-external-provisioner

kubectl apply -f storage/nfs-namespace.yaml

helm upgrade --install nfs \
  nfs-ganesha-server-and-external-provisioner/nfs-server-provisioner \
  --version 1.8.0 -n nfs-storage -f storage/nfs-values.yaml

kubectl -n nfs-storage rollout status statefulset/nfs-nfs-server-provisioner
kubectl get sc            # -> local-path (default), nfs
```

---

## 3. Install Airflow

Chart `apache-airflow/airflow` 1.22.0 (Airflow 3.2.2). Values: `airflow-values.yaml`.

Key choices in the values file:
- `executor: KubernetesExecutor` — each task runs in its own pod; no Celery/Redis.
- bundled PostgreSQL on `local-path`; Redis disabled.
- example DAGs enabled via the `AIRFLOW__CORE__LOAD_EXAMPLES=True` **env var**
  (setting it in `config.core` / airflow.cfg is ignored — the chart's env var wins).
- task logs persisted to the shared **`nfs`** RWX volume (`logs.persistence`).
- demo admin user `admin` / `admin`.

```sh
helm repo add apache-airflow https://airflow.apache.org
helm repo update apache-airflow

# IMPORTANT: install WITHOUT `--wait`.
# migrateDatabaseJob and createUserJob run as Helm post-install hooks. With `--wait`,
# Helm waits for the scheduler/api pods to be Ready before running those hooks, but
# the pods' `wait-for-airflow-migrations` init container can't pass until the
# migration runs -> deadlock. Without `--wait` the hooks run promptly. Watch progress
# in k9s / kubectl instead.
helm upgrade --install airflow apache-airflow/airflow \
  --version 1.22.0 \
  -n airflow --create-namespace \
  -f airflow-values.yaml

# Watch it come up.
kubectl -n airflow get pods -w
```

### Gotcha: enabling `logs.persistence` on an already-installed release

Switching the triggerer from its per-pod `volumeClaimTemplates` to the shared
`airflow-logs` PVC changes an **immutable StatefulSet** field, so `helm upgrade`
fails with *"updates to statefulset spec ... are forbidden"*. Fix: delete the
StatefulSet (keep the running pod) and re-run the upgrade.

```sh
kubectl -n airflow delete statefulset airflow-triggerer --cascade=orphan
helm upgrade airflow apache-airflow/airflow --version 1.22.0 -n airflow -f airflow-values.yaml
# The old logs-airflow-triggerer-0 100Gi PVC is now orphaned and safe to delete.
```

### Access the UI

```sh
kubectl -n airflow port-forward svc/airflow-api-server 8080:8080
# open http://localhost:8080   (login: admin / admin)
```

### Verify task logs work (KubernetesExecutor)

Without a shared RWX log volume the UI shows a `NameResolutionError` trying to fetch
logs from the (already-deleted) task pod on port 8793. With `logs.persistence` on `nfs`,
the api-server reads the log files directly off the shared volume and the error is gone.

---

## 4. Demo DAG — "Morning Executive Briefing"

A fan-out → join → load ETL that showcases the KubernetesExecutor: three parallel
extract tasks, an aggregation that waits for all three, then a report step.

```
fetch_sales ─┐
fetch_marketing ─┼─► aggregate_data ─► generate_and_send_report
fetch_support ─┘
```

Airflow guarantees the order: the three `fetch_*` tasks run in parallel; `aggregate_data`
only starts after all three succeed (DAG dependencies); then the report step runs.

### Simplifications vs. the presentation slide — **mention these when presenting**

1. **Data is generated on the fly** inside the `fetch_*` tasks — we do **not** read from S3.
2. **The DAG is delivered with `kubectl cp`** into the shared dags volume — **not**
   git-synced from a Git repository.
3. **The final step outputs / logs the report** — it does **not** send a real email.

### Implementation notes

- `aggregate_data` is implemented as a **`KubernetesPodOperator`** (runs in a separate
  container) — a nice teaching moment: "any container image can be an Airflow task."
- Prerequisites verified on this cluster:
  - the `airflow` namespace has no PSA enforce label, so KPO task containers don't need
    a strict securityContext;
  - the `airflow-worker` service account can create pods;
  - the scheduler image already ships `apache-airflow-providers-cncf-kubernetes` 10.17.1.
### Delivery — copy the DAG into the shared dags volume

Enable `dags.persistence` on the `nfs` RWX class (already set in
`airflow-values.yaml`) and re-run the Airflow upgrade:

```sh
helm upgrade airflow apache-airflow/airflow --version 1.22.0 -n airflow -f airflow-values.yaml
```

> **Critical gotcha — copy into the `dag-processor`, NOT the scheduler.**
> With `dags.persistence` the shared `airflow-dags` PVC is mounted on the
> **dag-processor** (and task pods), **not** the scheduler. In Airflow 3 the
> dag-processor is the component that scans the dags folder and serializes DAGs.
> If you `kubectl cp` into the scheduler, the dag-processor never sees the file,
> marks the DAG `is_stale=True`, and the scheduler silently skips it — runs sit
> in `queued` forever with no task pods. Always copy into the dag-processor.

```sh
# Copy the DAG into the dag-processor's shared dags volume.
DP=$(kubectl -n airflow get pod -l component=dag-processor -o name | head -1)
kubectl -n airflow cp dags/morning_executive_briefing.py \
  "${DP#pod/}":/opt/airflow/dags/morning_executive_briefing.py -c dag-processor

# The dags-folder bundle only refreshes ~every 300s ("Not time to refresh
# bundle" in the logs). Force an immediate re-parse by restarting the processor:
kubectl -n airflow rollout restart deploy/airflow-dag-processor
```

### Run it

```sh
# Trigger from the CLI (or just hit the play button in the UI).
SCHED=$(kubectl -n airflow get pod -l component=scheduler -o name | head -1)
kubectl -n airflow exec "${SCHED#pod/}" -c scheduler -- \
  airflow dags trigger morning_executive_briefing --run-id demo-$(date +%H%M%S)

# Watch the per-task states (all 5 should end `success`).
kubectl -n airflow exec "${SCHED#pod/}" -c scheduler -- \
  airflow tasks states-for-dag-run morning_executive_briefing <run-id>
```

> Verified end-to-end: the three `fetch_*` tasks run in parallel, `aggregate_data`
> runs in its own `python:3.12-slim` pod (KubernetesPodOperator) and pushes its
> result via XCom, and `generate_and_send_report` logs the final briefing.

### Gotcha: manual runs have `logical_date=None` (Airflow 3)

In Airflow 3 a **manually triggered** run has `logical_date=None`, so code like
`context["logical_date"].day` throws. The DAG uses a small `_ref_date(context)`
helper that falls back `logical_date` → `data_interval_start` →
`dag_run.run_after` / `queued_at` → `datetime.now(utc)`.

### Gotcha: api-server OOMKilled during heavy parsing / restarts

A burst of `rollout restart`s while the dag-processor parsed all ~94 example DAGs
pushed the **api-server** into an **OOMKill (exit 137)**; task pods then failed to
reach the execution API (`ConnectError [Errno 111]`). No resource limits are set
by default — it recovered on its own once things settled. If it recurs, give the
api-server explicit requests/limits or disable example DAGs.

> Reminder: the port-forward dies whenever the api-server pod restarts. Re-run
> `kubectl -n airflow port-forward svc/airflow-api-server 8080:8080` to get the UI back.

---

## 5. metrics-server (cluster resource usage)

Optional but handy for the demo: enables `kubectl top nodes/pods` and the live
CPU/memory columns in k9s. Values: `metrics-server-values.yaml`.

Talos kubelet serving certificates are self-signed, so metrics-server is started
with `--kubelet-insecure-tls` (acceptable for a demo, not production).

```sh
helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/
helm repo update metrics-server

helm upgrade --install metrics-server metrics-server/metrics-server \
  -n kube-system -f metrics-server-values.yaml

kubectl -n kube-system rollout status deploy/metrics-server
kubectl top nodes        # -> real CPU/memory once it has scraped (~30s)
```
