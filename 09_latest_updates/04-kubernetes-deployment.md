# 04 — Spark on Kubernetes

Spark on Kubernetes (K8s) has matured from "experimental" (Spark 2.3) to "production-ready" (Spark 3.x+). It's the canonical deployment mode for new self-managed Spark clusters in 2024+.

## Why K8s

The historical sequence:
- **Standalone** — Spark's own scheduler. Simple, limited multi-tenancy.
- **YARN** — Hadoop's scheduler. Battle-tested, requires HDFS-adjacent ops.
- **Mesos** — fading.
- **Kubernetes** — current default for new on-prem / cloud deployments.

K8s wins because:
- Most orgs already operate Kubernetes for everything else.
- Unified packaging (containers) for app + Spark.
- Better resource isolation.
- Autoscaling per-job (Spark Operator dynamically scales pods).

## Two deployment modes

### 1. spark-submit with master = k8s

Spark itself talks to the K8s API. You run:

```bash
./bin/spark-submit \
    --master k8s://https://api.k8s.example.com:443 \
    --deploy-mode cluster \
    --name my-job \
    --conf spark.kubernetes.namespace=spark-jobs \
    --conf spark.kubernetes.container.image=spark:3.5.0-python \
    --conf spark.executor.instances=10 \
    --conf spark.kubernetes.executor.deleteOnTermination=true \
    src/main.py
```

Spark creates a Driver pod, which creates Executor pods. When the job ends, pods are deleted.

### 2. Spark Operator (the production pattern)

A Kubernetes Operator (the Google open-source [Spark Operator](https://github.com/kubeflow/spark-operator)) lets you declare jobs as CRDs:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: silver-build
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: my-registry/spark:3.5-python
  mainApplicationFile: local:///opt/jobs/silver_build.py
  sparkVersion: "3.5.0"
  driver:
    cores: 2
    memory: 8g
    serviceAccount: spark-runner
  executor:
    cores: 4
    memory: 16g
    instances: 10
```

Then `kubectl apply -f silver-build.yaml`. The Operator handles driver pod lifecycle, log collection, status reporting, cleanup. This is the production pattern.

## Networking

Driver and executors need to talk to each other. Common setups:
- **Driver pod has a ClusterIP service**; executors connect to it.
- **Driver runs as a `Service`** so it has a stable address.
- **Executors find driver via the Service DNS name.**

Headless services work too. Read the docs for your operator's networking model.

## Storage

Spark on K8s needs storage for:
- **Job artifacts** (JARs, Python files) — usually pulled from S3/GCS or baked into the container.
- **Shuffle data** — usually node-local SSD via emptyDir or hostPath.
- **External shuffle service** — optional; not always available on K8s.

For the shuffle:
- **emptyDir on tmpfs / local SSD** for fast shuffle.
- **PersistentVolumeClaim** is possible but usually too slow.
- **Disaggregated shuffle services** (Apache Celeborn, etc.) — newer pattern for very large clusters.

## Autoscaling

Two layers:
- **Dynamic allocation**: Spark adds/removes executor pods based on load.
- **Cluster Autoscaler**: K8s adds/removes nodes based on pending pods.

Together: Spark says "I need 50 executors", scheduler can't place 30 of them, Cluster Autoscaler adds nodes, executors start. After job completes, executors terminate; nodes drain back.

Tune:
```bash
spark.dynamicAllocation.enabled = true
spark.dynamicAllocation.shuffleTracking.enabled = true     # for K8s — no external shuffle service
spark.dynamicAllocation.minExecutors = 2
spark.dynamicAllocation.maxExecutors = 100
spark.dynamicAllocation.executorIdleTimeout = 60s
```

`shuffleTracking` keeps an executor alive if it holds shuffle data needed by other tasks.

## Multi-tenancy

K8s isolation strategies:
- **One namespace per team / tenant.**
- **ResourceQuota and LimitRange** per namespace.
- **PriorityClass** for scheduling.
- **NetworkPolicy** for isolation.

For multi-team Spark, this is far cleaner than YARN queues.

## Observability

- **Spark UI**: usually port-forwarded or exposed via Ingress.
- **Spark History Server**: runs as a Deployment, reads event logs from S3.
- **Logs**: standard K8s log collection (Fluent Bit + ES / Loki / CloudWatch).
- **Metrics**: Spark JMX / Prometheus exporter; scraped by Prometheus.

## Specific to PySpark

Containers need:
- The Spark distribution.
- A compatible Python.
- All Python dependencies pre-installed OR `spark.archives` with a packed env.

Example Dockerfile:

```dockerfile
FROM spark:3.5.0-python3
USER root
RUN pip install --no-cache-dir delta-spark==3.2.0 pandas==2.0.0
COPY my_pipeline/ /opt/jobs/
USER spark
```

## When K8s is NOT the right answer

- **Managed offerings often beat self-managed.** EMR, Databricks, Glue, Dataproc all handle the K8s/YARN choice for you.
- **For interactive notebooks**, Databricks / Hex / Posit are usually better than self-managed K8s + notebook.
- **For one-off jobs**, you may not want the ongoing K8s investment.

K8s is best when:
- You already operate Kubernetes.
- You need fine-grained control or cost.
- You run many jobs across many tenants.

## References

- Spark on Kubernetes docs: https://spark.apache.org/docs/latest/running-on-kubernetes.html
- Spark Operator: https://github.com/kubeflow/spark-operator
- 📺 [Spark on Kubernetes — Databricks](https://www.youtube.com/results?search_query=spark+on+kubernetes+production)
- Apache Celeborn (disaggregated shuffle): https://celeborn.apache.org/
