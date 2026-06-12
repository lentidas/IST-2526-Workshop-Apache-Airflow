# Talos K8s demo cluster in Docker

This directory contains a Talos machine-config patch that adds the basic add-ons needed for a working Talos Kubernetes cluster running in Docker.

We simply load this machine-config patch ([patch-addons.yaml](./patch-addons.yaml)) at bootstrap to bring up the demo Kubernetes cluster **with storage and metrics already installed**.

> [!NOTE]
> More information on the configurations of the installed add-ons can be found in the [patch file itself](./patch-addons.yaml).

## Pre-requisites

### macOS

For macOS users with Apple Silicon, typically they have Docker running inside a **colima** VM and Talos brings up each node as a Docker container in that engine.

If you have multiple colima profiles, make sure to specify the correct one in the commands below (replace `arm64` with your profile name if different).

```sh
# Make sure the colima VM (and its Docker engine) is running first.
colima start --profile arm64

# Point the Docker client / talosctl at colima's Docker socket.
export DOCKER_HOST=unix:///Users/<user>/.colima/arm64/docker.sock
```

## Create the cluster

> [!IMPORTANT]
> Run from the repo root so the `@cluster/patch-addons.yaml` path resolves.

```sh
# Create the cluster with the add-ons patch applied at bootstrap.
talosctl cluster create docker \
  --name ist-airflow-demo \
  --workers 3 \
  --cpus-controlplanes 2 --memory-controlplanes 2048 \
  --cpus-workers 2 --memory-workers 3072 \
  --talosconfig-destination ~/.talos/ist-airflow-demo.yaml \
  --config-patch-controlplanes @cluster/patch-addons.yaml

# Point your tools at the generated configuration files for this session:
export TALOSCONFIG=~/.talos/ist-airflow-demo.yaml
export KUBECONFIG=~/.kube/talos-ist-airflow-demo.yaml
```

> [!TIP]
> The command `cluster create docker` bootstraps the control plane and writes the kubeconfig automatically to `~/.kube/config`. Use `talosctl kubeconfig` if you want to write it to a different location or with a different name.

## Tear down / recreate

```sh
talosctl cluster destroy --name ist-airflow-demo
rm -rf ~/.talos/ist-airflow-demo.yaml
rm -rf ~/.talos/clusters/ist-airflow-demo
rm -f  ~/.kube/talos-ist-airflow-demo.yaml # If you created a custom kubeconfig with `talosctl kubeconfig`.
```

Recreate with the single `talosctl cluster create docker ... --config-patch-control-plane`
command above — storage and metrics come back automatically.
