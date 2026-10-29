# QuorumDB on GKE: design notes

How QuorumDB runs on Google Kubernetes Engine across three zones, why each piece is there, and what it costs. The Raft implementation itself is unchanged; see the README for how it works.

## 1. Infrastructure (Terraform)

### What was built

All GCP resources are in [`infra/terraform/`](infra/terraform/), one file per concern:

| File | Creates |
|---|---|
| `providers.tf` | Google provider, state in a GCS bucket (bucket name passed at `terraform init`) |
| `variables.tf` | `project_id`, `region` (default `us-west1`), `machine_type` (default `e2-standard-2`) |
| `apis.tf` | Enables the Compute, Kubernetes Engine and Artifact Registry APIs |
| `network.tf` | A custom VPC and one regional subnet, with secondary ranges for pod and Service IPs |
| `artifact_registry.tf` | A Docker repository in the same region |
| `cluster.tf` | A regional GKE cluster in the region's first three zones, with Dataplane V2 |
| `node_pools.tf` | `db`: one `e2-standard-2` per zone (3 nodes). `tools`: one node for benchmark and chaos clients |
| `service_account.tf` | A node service account with GKE's minimum node role and read access to the image repository |

`scripts/push-image.sh` builds the image for `linux/amd64` and pushes it, tagged with the git commit.

### Why

- **Three zones, one node each.** A zone is a separate failure domain: its own power, cooling and network. Raft needs a majority (2 of 3), so with one node per zone, losing any single zone leaves a working cluster. Two nodes in one zone would mean that zone's failure takes out the majority.
- **Regional cluster.** The location is the region, so GKE runs the Kubernetes control plane in every zone. If the leader's zone fails, the API server is still up to reschedule pods and serve `kubectl`.
- **Dataplane V2.** GKE's eBPF dataplane (built on Cilium) enforces Kubernetes NetworkPolicy without installing Calico. The chaos test uses NetworkPolicy to cut links between zones.
- **VPC-native networking.** Pods get IPs from a secondary range of the subnet, so traffic between pods in different zones is routed directly by the VPC with no overlay. Dataplane V2 requires this.
- **A separate tools node.** `redis-benchmark` with 50 connections uses a lot of CPU. On a 2-vCPU database node, it would compete with QuorumDB and the results would measure that competition.
- **Remote state.** Terraform state in GCS survives a lost laptop and is locked while an apply runs.
- **Image built on the laptop.** `docker buildx` cross-compiles Go for amd64, which is fast and needs no Cloud Build setup.

### Tradeoffs

- **Regional cluster fee.** GKE charges a management fee per cluster, and its free tier covers zonal and Autopilot clusters but not regional ones. A zonal cluster with nodes in three zones would be free, but its control plane would live in one zone.
- **Nodes have public IPs.** Private nodes are more secure but need Cloud NAT to reach the internet, which is another billed resource. The workloads here only talk to each other and to Google APIs, so public nodes keep the setup small.
- **e2-standard-2 is small.** 2 vCPUs (shared hyperthreads) and 8 GB. It is cheap, and with Raft every write waits on two disk flushes, so CPU is not the first bottleneck. The numbers would be higher on bigger machines.
- **pd-balanced disks.** These are network-attached disks, so each fsync is a network round trip. Local SSDs would be faster but lose data when a VM is stopped, which defeats the point of a write-ahead log.
- **`remove_default_node_pool`.** Provider 8 deprecates it, because GKE 1.36.4+ can create a cluster with no default pool. The deprecated way works on every GKE version the release channel might pick.

### Interview questions

**Why three zones and not two?**
Raft needs a majority. With two zones, one zone has at least two of the three nodes, and losing that zone loses the majority. With three zones and one node each, any one zone can fail.

**What happens to writes if a whole zone goes down?**
If it was a follower's zone, nothing visible: the leader still has a majority (itself plus the other follower). If it was the leader's zone, the two remaining nodes stop hearing heartbeats, elect a new leader after a randomized 300–600 ms timeout, and continue. Writes that were in flight on the old leader get an "outcome unknown" error, never a false OK.

**Why a dedicated node service account?**
The default Compute Engine account often has the Editor role on the whole project, so any pod that reached the metadata server could act with it. The dedicated account can only write logs and metrics and pull from one repository.
