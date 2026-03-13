# Wire Server Upgrade Report — 2026-03-10

## Environment

| Component       | Value                          |
|----------------|-------------------------------|
| Namespace       | `default`                      |
| Admin host      | `ctadmin@80.75.154.73` (port 2222) |
| New bundle      | `/home/ctadmin/wire-server-deploy-new` |
| Old bundle      | `/home/ctadmin/wire-server-deploy` |
| Kubernetes nodes | `172.25.203.102/103/104`      |
| External RabbitMQ | `172.25.203.111/112/113`    |

---

## Upgrade Process

The upgrade was performed using the `wire-upgrade` CLI tool.

**1. Sync chart images to cluster nodes:**
```bash
wire-upgrade sync-chart-images --namespace default
```
This pushed all container images from the new bundle to the containerd registry on each Kubernetes node (`172.25.203.102/103/104`) via SSH + `ctr images import`.

**2. Sync values from live cluster:**
```bash
wire-upgrade install-or-upgrade wire-server --sync-values --namespace default
```
Fetched live helm values from the cluster, merged with the new bundle templates, and wrote `values.yaml` / `secrets.yaml` to `values/wire-server/`.

**3. Install wire-server:**
```bash
wire-upgrade install-or-upgrade wire-server --namespace default
```
Ran `helm upgrade --install wire-server` using the merged values and secrets files from the new bundle.

---

## Issues Encountered and Resolutions

### 1. Elasticsearch Index Mapping Conflict

**Symptom:**
```
400 - {"error":{"type":"illegal_argument_exception",
  "reason":"Mapper for [email_unvalidated] conflicts with existing mapping"}}
```

**Root Cause:**
The existing Elasticsearch index had `email_unvalidated` mapped with a different `index` setting than what the new Wire version expects. Elasticsearch does not allow changing field mapping properties on an existing index.

**Resolution:**
Delete the conflicting index and recreate it via the `elasticsearch-index` subchart:

```bash
# Delete the conflicting index
d kubectl exec -n default deployment/brig -- \
  curl -X DELETE http://elasticsearch-external:9200/directory

# Run the elasticsearch-index chart standalone
d bash -c "KUBECONFIG=/wire-server-deploy/kubeconfig helm upgrade --install elasticsearch-index \
  charts/wire-server/charts/elasticsearch-index \
  --set elasticsearch.host=elasticsearch-external \
  --set cassandra.host=cassandra-external \
  -n default"

# Trigger full reindex from Cassandra
d kubectl exec -n default deployment/brig -- \
  curl -X POST http://localhost:8080/i/index/reindex
```

---

### 2. Cannon CrashLoopBackOff — RabbitMQ Authentication Failure

**Symptom:**
```
cannon: ConnectionClosedException Abnormal "Handshake failed.
  Please check the RabbitMQ logs for more information"
```

**How it worked before the upgrade:**

- **Cannon** did not use RabbitMQ at all in the old Wire version — no RabbitMQ config existed in cannon's ConfigMap
- **Other services** (brig, gundeck, galley, background-worker) connected to the **in-cluster** `rabbitmq` StatefulSet (`rabbitmq-0` pod at `10.233.107.144`) using `wire-server/verysecurepassword` credentials
- The external RabbitMQ cluster (`172.25.203.111-113`) existed but was not used by Wire services

**Root Cause (multi-layered):**

The new Wire version introduced RabbitMQ as a required dependency for `cannon` and switched all services to use the external RabbitMQ cluster. After the helm upgrade:

1. Cannon's ConfigMap was updated to connect to `rabbitmq-external` (external 3-node cluster at `172.25.203.111-113`)
2. Cannon's Kubernetes secret had `guest/guest` credentials
3. RabbitMQ's default configuration blocks the `guest` user from remote (non-localhost) AMQP connections (`loopback_users` restriction)
4. The `rabbitmq-external` Kubernetes service only exposed port `5672` (AMQP), not port `15672` (management API). Since the management plugin was also only enabled on `rabbitmq1`, requests from pods that DNS-resolved to `rabbitmq2` or `rabbitmq3` got "Connection refused" on port `15672`

**Key finding:**
The in-cluster `rabbitmq-0` pod and the external RabbitMQ cluster (`172.25.203.111-113`) are separate systems with different credentials. The `wire-server/verysecurepassword` credentials only apply to the in-cluster RabbitMQ and are not valid on the external cluster.

**Resolution:**

**Step 1:** Allow `guest` remote connections on all 3 external RabbitMQ nodes:

```bash
# Repeat on 172.25.203.111, .112, .113
ssh ctadmin@172.25.203.111
echo "loopback_users = none" | sudo tee -a /etc/rabbitmq/rabbitmq.conf
sudo systemctl restart rabbitmq-server
```

**Step 2:** Enable RabbitMQ management plugin on all 3 nodes (was only on rabbitmq1):

```bash
# On 172.25.203.112 and .113
sudo rabbitmq-plugins enable rabbitmq_management
sudo systemctl restart rabbitmq-server
```

**Note on cannon 500 errors during startup:**
After cannon restarts, Wire clients immediately reconnect and hit `/await` (WebSocket notification endpoint) before cannon's RabbitMQ connection is fully established. These requests return 500 transiently. Once all clients re-establish their WebSocket sessions the errors stop. The debug log message `ResponseRaw - cannot collect metrics or log info on errors` on the `/await` path is **normal** — it is the expected log entry for WebSocket connections in cannon, not an error indicator.

**Step 3:** Ensure cannon Kubernetes secret uses `guest/guest`:

```bash
d kubectl patch secret cannon -n default \
  --type='json' \
  -p="[{\"op\":\"replace\",\"path\":\"/data/rabbitmqUsername\",\"value\":\"$(echo -n 'guest' | base64)\"},
       {\"op\":\"replace\",\"path\":\"/data/rabbitmqPassword\",\"value\":\"$(echo -n 'guest' | base64)\"}]"

d kubectl delete pod -n default cannon-0 cannon-1 cannon-2
```

**Future Resolution — Proper Fix for Management API Port:**

The `rabbitmq-external` Kubernetes service and Endpoints object should be updated to also expose port `15672` so the management API is reachable via DNS rather than relying on direct IP access. This also makes it explicit which ports the external RabbitMQ exposes.

```bash
# Add port 15672 to the service
d kubectl patch service rabbitmq-external -n default \
  --type='json' \
  -p='[{"op":"add","path":"/spec/ports/-","value":{"name":"management","port":15672,"protocol":"TCP","targetPort":15672}}]'

# Add port 15672 to the endpoints
d kubectl patch endpoints rabbitmq-external -n default \
  --type='json' \
  -p='[{"op":"replace","path":"/subsets","value":[{"addresses":[{"ip":"172.25.203.111"},{"ip":"172.25.203.112"},{"ip":"172.25.203.113"}],"ports":[{"name":"http","port":5672,"protocol":"TCP"},{"name":"management","port":15672,"protocol":"TCP"}]}]}]'
```

Ideally this should be added to the `rabbitmq-external` Helm chart values so it persists across upgrades.

---

### 3. Gundeck and Cannon — Intermittent RabbitMQ Connection Drops

**Symptom:**
```
gundeck: ConnectionClosedException Abnormal "Network.Socket.sendBuf: resource vanished (Connection reset by peer)"
gundeck: Network.Socket.sendBuf: invalid argument (Bad file descriptor)
```

**Root Cause:**

Connections from Kubernetes pods to the external RabbitMQ cluster cross a network boundary. When connections are idle (no messages flowing), intermediate network state (NAT tables, Kubernetes network path) silently drops TCP connections after a timeout. RabbitMQ then sends a TCP RST when it next tries to write to the dead socket, which gundeck/cannon see as "Connection reset by peer". The subsequent reconnect attempt hits "Bad file descriptor" because the AMQP library tries to reuse the invalidated socket before fully re-establishing.

RabbitMQ cluster status confirmed no alarms, no partitions, and all 20 connections healthy — the drops were transient network-layer timeouts, not a RabbitMQ issue.

**Resolution:**

Add `heartbeatTimeout: 30` to the RabbitMQ config for both `cannon` and `gundeck` in `values.yaml`:

```yaml
cannon:
  config:
    rabbitmq:
      host: rabbitmq-external
      heartbeatTimeout: 30

gundeck:
  config:
    rabbitmq:
      host: rabbitmq-external
      heartbeatTimeout: 30
```

With heartbeats every 30 seconds, both sides exchange keepalive frames frequently enough to detect dead connections before the network drops them silently, and well within any NAT/firewall idle timeout.

---

### 4. Values and Secrets Files — RabbitMQ Credentials

**Issue:**
The `new-secrets.yaml` template had `guest/guest` for all RabbitMQ entries. This is correct for the external RabbitMQ cluster after applying `loopback_users = none`.

**Updated `secrets.yaml`** (located at `values/wire-server/secrets.yaml` on admin host):
- All `rabbitmq.username`: `guest`
- All `rabbitmq.password`: `guest`

---

## Loki / Grafana Setup (Pre-Upgrade)

### nginx CrashLoopBackOff — IPv6 Not Supported

**Symptom:**
```
socket() [::]:8080 failed (97: Address family not supported by protocol)
```

**Resolution:**
Edit the `loki-wrapper-gateway` ConfigMap to remove the IPv6 listen line:

```bash
d kubectl edit configmap loki-wrapper-gateway -n monitoring
# Remove: listen [::]:8080;
# Keep:   listen 8080;

d kubectl rollout restart deployment loki-wrapper-gateway -n monitoring
```

### Port Forwarding for Grafana → Loki

```bash
d kubectl port-forward --address 0.0.0.0 svc/loki-wrapper-gateway 3101:80 -n monitoring
```

Use `http://172.25.203.100:3101` as the Loki URL in Grafana.

---

## Rollback Procedure

If the wire-server upgrade needs to be rolled back:

```bash
# Check revision history
d helm history wire-server -n default

# Roll back to previous revision
d helm rollback wire-server -n default

# Verify
d kubectl get pods -n default
```

> **Note:** Helm rollback does not revert database migrations (Cassandra/PostgreSQL). Check Wire release notes for breaking schema changes before rolling back.

---

## Known Limitations / Future Improvements

### Values Sync Strategy

The current `--sync-values` mechanism works as follows:
1. Load `prod-values.example.yaml` / `prod-secrets.example.yaml` as the base template
2. Fetch live helm values from the cluster
3. Deep merge: template is base, live cluster values override matching keys

**Problem:** Only keys that exist in the template are carried over from the cluster. New fields introduced in the new Wire version keep their template defaults without being explicitly reviewed. Fields that exist in the live cluster but not in the template are silently dropped.

**Ideal approach:**
1. Fetch live helm values from the cluster first (source of truth)
2. Use live values as the base (`values.yaml` / `secrets.yaml`)
3. Deep merge new template fields on top — only adding keys that do not yet exist in the live values

This ensures:
- All existing live configuration is preserved as-is
- New fields from the new Wire version are added with their template defaults
- Nothing from the live cluster is silently dropped

This change should be implemented in `wire_upgrade/values_sync.py` by inverting the merge order:
```python
# Current (wrong direction):
result = deep_merge(template, cluster_values)

# Improved (cluster is base, template fills new keys only):
result = deep_merge(cluster_values, template)
```

Where `deep_merge(base, override)` only adds keys from `override` that are missing in `base`, without overwriting existing values.

---

## Upgrade Result

| Item | Value |
|------|-------|
| Previous version | `wire-server-5.23.0` (revision 30) |
| New version | `wire-server-5.27.0` (revision 31) |
| Deployed | Tue Mar 10 08:32:57 2026 |
| Helm status | `deployed` |

## Post-Upgrade Checklist

- [x] Elasticsearch index recreated and reindexed (`brig-index-migrate-data` Completed)
- [x] All cannon pods running and connected to RabbitMQ (3/3 Running)
- [x] All services (brig, galley, gundeck, cannon, background-worker) `Ready`
- [x] All migration jobs completed (cassandra, galley, gundeck, spar, elasticsearch)
- [x] RabbitMQ management API accessible on all 3 nodes (`172.25.203.111-113:15672`)
- [x] `loopback_users = none` applied to all 3 RabbitMQ nodes
- [x] `secrets.yaml` updated and copied to admin host
- [x] Loki/Grafana connectivity verified
