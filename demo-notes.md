# Airflow on Talos — Workshop Demo Notes

For the demo, we are running Apache Airflow with the `KubernetesExecutor` (that is, each Airflow task runs in its own Kubernetes pod) on a Talos Kubernetes cluster that runs as Docker containers inside a colima VM on macOS.

Cluster facts:
- Talos v1.13.4, Kubernetes v1.36.1, arm64, containerd 2.2.4
- kube context: `admin@ist-airflow-demo`
- Nodes: 1 control-plane (`10.5.0.2`) + 3 workers (`10.5.0.3`–`10.5.0.5`)

## Install Airflow

- Chart `apache-airflow/airflow` 1.22.0 (Airflow 3.2.2)
- Values: [`airflow-values.yaml`](./airflow-values.yaml)

The key configuration choices in `airflow-values.yaml` are:

- `executor: KubernetesExecutor` — each task runs in its own pod, which we think is the better Kubernetes-native approach;
- Bundled PostgreSQL stores its database on a volume provisioned by `local-path`;
- Redis disabled;
- We enabled the embedded example DAGs via the `AIRFLOW__CORE__LOAD_EXAMPLES=True` **env var** because setting it in `config.core` / `airflow.cfg` is ignored.
- In order to have task logs persisted and shared between pods, we use the shared **`nfs`** RWX volume (`logs.persistence`).
- A demo admin user is created, with credentials `admin` / `admin`.

```sh
helm repo add apache-airflow https://airflow.apache.org
helm repo update apache-airflow
```

> [!IMPORTANT]
> Run the `helm install` command **without** the `--wait` command option.
> The initialization jobs `migrateDatabaseJob` and `createUserJob` run as Helm post-install hooks. With `--wait`, Helm waits for the scheduler/api pods to be Ready before running those hooks, but the pods' `wait-for-airflow-migrations` init container can't pass until the migration runs.
> That means we run into a deadlock, so it is preferable to watch progress in `k9s` / `kubectl` instead.

Install Airflow using our values (run from the repo root so the path to `airflow-values.yaml` resolves):

```sh
helm upgrade --install airflow apache-airflow/airflow \
  --version 1.22.0 \
  -n airflow --create-namespace \
  -f airflow-values.yaml
```

You should see something like this:

```sh
helm upgrade --install airflow apache-airflow/airflow \
  --version 1.22.0 \
  -n airflow --create-namespace \
  -f airflow-values.yaml
Release "airflow" does not exist. Installing it now.
I0612 11:39:50.137843   81756 warnings.go:107] "Warning: would violate PodSecurity \"restricted:latest\": runAsNonRoot != true (pod or container \"statsd\" must set securityContext.runAsNonRoot=true), seccompProfile (pod or container \"statsd\" must set securityContext.seccompProfile.type to \"RuntimeDefault\" or \"Localhost\")"
I0612 11:39:50.138258   81756 warnings.go:107] "Warning: would violate PodSecurity \"restricted:latest\": runAsNonRoot != true (pod or containers \"wait-for-airflow-migrations\", \"scheduler\", \"scheduler-log-groomer\" must set securityContext.runAsNonRoot=true), seccompProfile (pod or containers \"wait-for-airflow-migrations\", \"scheduler\", \"scheduler-log-groomer\" must set securityContext.seccompProfile.type to \"RuntimeDefault\" or \"Localhost\")"
I0612 11:39:50.138939   81756 warnings.go:107] "Warning: would violate PodSecurity \"restricted:latest\": runAsNonRoot != true (pod or containers \"wait-for-airflow-migrations\", \"api-server\" must set securityContext.runAsNonRoot=true), seccompProfile (pod or containers \"wait-for-airflow-migrations\", \"api-server\" must set securityContext.seccompProfile.type to \"RuntimeDefault\" or \"Localhost\")"
I0612 11:39:50.139459   81756 warnings.go:107] "Warning: would violate PodSecurity \"restricted:latest\": runAsNonRoot != true (pod or containers \"wait-for-airflow-migrations\", \"dag-processor\", \"dag-processor-log-groomer\" must set securityContext.runAsNonRoot=true), seccompProfile (pod or containers \"wait-for-airflow-migrations\", \"dag-processor\", \"dag-processor-log-groomer\" must set securityContext.seccompProfile.type to \"RuntimeDefault\" or \"Localhost\")"
I0612 11:39:50.154003   81756 warnings.go:107] "Warning: would violate PodSecurity \"restricted:latest\": runAsNonRoot != true (pod or containers \"wait-for-airflow-migrations\", \"triggerer\", \"triggerer-log-groomer\" must set securityContext.runAsNonRoot=true), seccompProfile (pod or containers \"wait-for-airflow-migrations\", \"triggerer\", \"triggerer-log-groomer\" must set securityContext.seccompProfile.type to \"RuntimeDefault\" or \"Localhost\")"
I0612 11:39:50.305625   81756 warnings.go:107] "Warning: would violate PodSecurity \"restricted:latest\": runAsNonRoot != true (pod or container \"run-airflow-migrations\" must set securityContext.runAsNonRoot=true), seccompProfile (pod or container \"run-airflow-migrations\" must set securityContext.seccompProfile.type to \"RuntimeDefault\" or \"Localhost\")"
I0612 11:40:39.112519   81756 warnings.go:107] "Warning: would violate PodSecurity \"restricted:latest\": runAsNonRoot != true (pod or container \"create-user\" must set securityContext.runAsNonRoot=true), seccompProfile (pod or container \"create-user\" must set securityContext.seccompProfile.type to \"RuntimeDefault\" or \"Localhost\")"
NAME: airflow
LAST DEPLOYED: Fri Jun 12 11:39:48 2026
NAMESPACE: airflow
STATUS: deployed
REVISION: 1
DESCRIPTION: Install complete
TEST SUITE: None
NOTES:
Thank you for installing Apache Airflow 3.2.2!

Your release is named airflow.
You can now access your dashboard(s) by executing the following command(s) and visiting the corresponding port at localhost in your browser:
Airflow API Server:     kubectl port-forward svc/airflow-api-server 8080:8080 --namespace airflow
Default user (Airflow UI) Login credentials:
    username: admin
    password: admin
Default Postgres connection credentials:
    username: postgres
    password: postgres
    port: 5432

You can get Fernet Key value by running the following:

    echo Fernet Key: $(kubectl get secret --namespace airflow airflow-fernet-key -o jsonpath="{.data.fernet-key}" | base64 --decode)

 DEPRECATION WARNING:
    Dags Git-Sync bevaiour with `dags.gitSync.recommendedProbeSetting` equal `false` is deprecated and will be removed in future.
    Please change your values as support for the old name will be dropped in a future release.

  DEPRECATION WARNING:
    The default for `enableServiceLinks` will become False in Chart 2.0.
    If you relied on these environment variables, explicitly set ``enableServiceLinks: true``, or migrate your code to use dns based service lookups.

#####################################################
#  WARNING: You should set a static API secret key  #
#####################################################

You are using a dynamically generated API secret key, which can lead to
unnecessary restarts of your Airflow components.

Information on how to set a static API secret key can be found here:
https://airflow.apache.org/docs/helm-chart/stable/production-guide.html#api-secret-key
```

### Access the UI

Port-forward the api-server to access the UI:

```sh
kubectl -n airflow port-forward svc/airflow-api-server 8080:8080
```

Afterwards, open http://localhost:8080   (login: admin / admin)

## Demo DAG — "Morning Executive Briefing"

As discussed in the presentation, we have created an example fan-out → join → load ETL (Extract, Transform, Load) workflow that showcases what Apache Airflow can do.

In this example, we have a DAG (Directed Acyclic Graph) with four tasks that:

1. Simulate fetching data from three different sources (sales, marketing, support);
2. Aggregate the data in a single step (e.g., combine, clean, and analyze the data);
3. Generate a report that is sent to executives.

The DAG structure looks like this:

```
fetch_sales     ─┐
fetch_marketing ─┼─► aggregate_data ─► generate_and_send_report
fetch_support   ─┘
```

Airflow guarantees the tasks are executed in order and the tasks that run in parallel do so concurrently.

> [!NOTE]
> For simplicity and demo purposes, this DAG is self-contained and does not interact with real external systems or APIs.
>
> This means that:
>
> 1. **Data is generated on the fly** inside the `fetch_*` tasks. Typically, in this step we would read from a data repository such as S3 buckets.
> 2. **The DAG is delivered with `kubectl cp`** into the shared DAGs volume. A better practice is to use a Git-synced repository, where Airflow automatically syncs the DAGs from.
> 3. **The final step outputs / logs the report**. In the real world, we would probably want to send the report via email or post it to a dashboard.

### How to copy the DAG into the shared dags volume

> [!WARNING]
> **Copy the `morning_executive_briefing.py` DAG into the `airflow-dag-processor` pod, not the `airflow-scheduler` pod.**
> 
> With `dags.persistence` the shared `airflow-dags` PVC is mounted on the `airflow-dag-processor` pod. In Airflow 3 the dag-processor is the component that scans the dags folder and serializes DAGs. 
> 
> If you `kubectl cp` into the scheduler, the dag-processor never sees the file, marks the DAG `is_stale=True`, and the scheduler silently skips it. It will rest in a queued state forever with no task pods.

```sh
# Copy the DAG into the dag-processor's shared dags volume.
DP=$(kubectl -n airflow get pod -l component=dag-processor -o name | head -1)
kubectl -n airflow cp dags/morning_executive_briefing.py \
  "${DP#pod/}":/opt/airflow/dags/morning_executive_briefing.py -c dag-processor

# The dags-folder bundle only refreshes ~every 300s. If we are pressed for time, we can force an immediate re-parse by restarting the processor.
kubectl -n airflow rollout restart deploy/airflow-dag-processor
```

### Run it

```sh
# Trigger from the CLI (or just hit the play button in the UI).
SCHED=$(kubectl -n airflow get pod -l component=scheduler -o name | head -1)
kubectl -n airflow exec "${SCHED#pod/}" -c scheduler -- \
  airflow dags trigger morning_executive_briefing --run-id demo-$(date +%Y%m%d-%H%M%S)

# Watch the per-task states (all 5 should end `success`).
kubectl -n airflow exec "${SCHED#pod/}" -c scheduler -- \
  airflow tasks states-for-dag-run morning_executive_briefing <run-id>
```

## Other demo DAGs

Apache Airflow comes with a set of example DAGs that are automatically installed when you set `AIRFLOW__CORE__LOAD_EXAMPLES=True` (which we do in our `airflow-values.yaml`).


