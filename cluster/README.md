# Cluster — one-shot bootstrap (Talos + add-ons)

Bring up the demo Kubernetes cluster **with storage and metrics already installed**,
straight from `talosctl cluster create`. Instead of applying storage manifests and
running `helm install` by hand afterwards, all add-ons are baked into a single Talos
machine-config patch ([patch-addons.yaml](patch-addons.yaml)) that the control-plane
node reconciles at bootstrap.

> Airflow itself is **not** part of this bootstrap — install it afterwards with its
> own Helm values (see `airflow-values.yaml` in the repo root).

---

## What the patch installs

The patch uses two Talos `cluster.*` mechanisms:

| Mechanism | What it loads | Why |
| --- | --- | --- |
| `cluster.extraManifests` | k3s **helm-controller** (`deploy-cluster-scoped.yaml`, v0.17.2) | Talos has no native Helm. The helm-controller adds the `helm.cattle.io/v1` **`HelmChart`** CRD + a controller that runs `helm install` jobs for us. |
| `cluster.inlineManifests` | **local-path-provisioner** (plain patched manifest) | Default **RWO** StorageClass. Path under `/var`, PSA-privileged namespace, marked default. |
| `cluster.inlineManifests` | **nfs-server-provisioner** (`HelmChart` CR) | **RWX** StorageClass `nfs` via userspace NFS-Ganesha. Namespace pre-created PSA-privileged. |
| `cluster.inlineManifests` | **metrics-server** (`HelmChart` CR) | `kubectl top` + live CPU/memory in k9s. Started with `--kubelet-insecure-tls` (Talos kubelet certs are self-signed). |

End state: StorageClasses `local-path` (default) + `nfs`, and a working metrics
pipeline — identical to the hand-rolled setup, but reproducible in one command.

---

## Prerequisites

On macOS, Docker runs inside a **colima** VM and Talos brings up each node as a Docker
container in that engine. Point `talosctl` at colima's Docker socket via `DOCKER_HOST`.

```sh
# Make sure the colima VM (and its Docker engine) is running first.
colima start

# Point the Docker client / talosctl at colima's Docker socket.
export DOCKER_HOST=unix:///Users/goncaloheleno/.colima/arm64/docker.sock
```

---

## Create the cluster

Run from the repo root so the `@cluster/patch-addons.yaml` path resolves.

```sh
talosctl cluster create docker \
  --workers 3 \
  --name ist-airflow-demo \
  --talosconfig-destination ~/.talos/ist-airflow-demo.yaml \
  --kubeconfig ~/.kube/talos-ist-airflow-demo.yaml \
  --config-patch-control-plane @cluster/patch-addons.yaml
```

Notes:
- `cluster create docker` bootstraps the control plane and writes the kubeconfig
  automatically — no separate `talosctl bootstrap` / `talosctl kubeconfig` steps.
- The `cluster.*` config only matters on the bootstrap (control-plane) node, hence
  `--config-patch-control-plane`.

Point your tools at the generated configs for this session:

```sh
export TALOSCONFIG=~/.talos/ist-airflow-demo.yaml
export KUBECONFIG=~/.kube/talos-ist-airflow-demo.yaml
```

---

## Verify the add-ons came up

The helm-controller fetches and installs the charts after the API server is ready, so
give it a minute or two on a cold start (it pulls images + charts from the internet).

```sh
kubectl get nodes -o wide                 # 1 control-plane + 3 workers, Ready

# Helm-controller turned each HelmChart CR into an install job:
kubectl get helmchart -A                   # nfs-server-provisioner, metrics-server
kubectl -n kube-system get jobs | grep helm-install

# Storage:
kubectl get storageclass                   # local-path (default), nfs
kubectl -n local-path-storage rollout status deploy/local-path-provisioner
kubectl -n nfs-storage rollout status statefulset/nfs-nfs-server-provisioner

# Metrics (works ~30s after metrics-server is Ready):
kubectl top nodes
```

### Troubleshooting

- **`HelmChart` stuck / no job yet:** the helm-controller CRD may not have been ready
  on the first manifest pass. Talos keeps reconciling, so it usually resolves on its
  own; otherwise check the controller: `kubectl -n kube-system logs deploy/helm-controller`.
- **`helm-install-*` job failing:** inspect it with
  `kubectl -n kube-system logs job/helm-install-nfs-server-provisioner`. Most failures
  are transient network/repo issues — delete the job and the controller recreates it.
- **`kubectl top` errors right after boot:** metrics-server needs one scrape interval;
  wait ~30s and retry.

---

## Tear down / recreate

```sh
talosctl cluster destroy --name ist-airflow-demo
rm -rf ~/.talos/ist-airflow-demo.yaml
rm -rf ~/.talos/clusters/ist-airflow-demo
rm -f  ~/.kube/talos-ist-airflow-demo.yaml
```

Recreate with the single `talosctl cluster create docker ... --config-patch-control-plane`
command above — storage and metrics come back automatically.

---

## Relationship to the manual files

This patch supersedes the manual add-on files used in the original walkthrough:

- `storage/local-path-storage.yaml` → embedded as an inline manifest.
- `storage/nfs-namespace.yaml` + `storage/nfs-values.yaml` → the `nfs-server-provisioner`
  `HelmChart` CR (`valuesContent`) + pre-created namespace.
- `metrics-server-values.yaml` → the `metrics-server` `HelmChart` CR (`valuesContent`).

Keep them as reference, or delete them once you've switched to the bootstrap patch.
