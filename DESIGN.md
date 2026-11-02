# Design notes

How QuorumDB runs on Google Cloud across three data centers, why each part is there, what it costs, and the problems I ran into. The part that keeps the three nodes in agreement (Raft) did not change for any of this.

A few words used below:

- **Node:** one running copy of the database. There are always three.
- **Zone:** one of Google's data centers within a region, with its own power, cooling and network. `us-west1` has zones `us-west1-a`, `-b` and `-c`.
- **Kubernetes:** a system that runs programs on a group of computers and restarts them when they stop. Google's version is GKE.
- **Pod:** one program running under Kubernetes. Each node of the database is a pod.
- **Terraform:** a tool that creates cloud resources from text files, so the whole setup can be built and deleted with one command.

## The cloud setup

**What I built.** The Terraform files in [`infra/terraform`](infra/terraform) describe everything:

- a private network for the cluster,
- an image registry, which stores the packaged database program,
- a Kubernetes cluster whose own control system runs in all three zones,
- three small computers for the database, one in each zone (`e2-standard-2`: 2 CPUs and 8 GB of memory),
- one more computer of the same size for the test programs,
- an identity for those computers that can only write logs and download the database program.

`make up` builds all of it and `make down` deletes it. [`scripts/push-image.sh`](scripts/push-image.sh) packages the program for Google's Intel-based computers on my Apple laptop and uploads it.

**Why.**

- **Three zones, one node each.** The database keeps working as long as two of the three nodes can talk to each other. With one node per zone, losing any single data center still leaves two. With only two zones, one of them would hold two nodes, and losing it would stop everything.
- **A regional cluster.** Kubernetes' own control system also runs in all three zones, so if a zone goes down, Kubernetes is still there to restart things.
- **Dataplane V2.** This is Google's networking layer for the cluster. It enforces network rules, which the chaos test uses to cut links between zones.
- **A separate computer for tests.** The speed test uses a lot of CPU. On a 2-CPU database computer it would slow the database down, and the results would measure that.
- **Terraform's records in a storage bucket.** Terraform keeps a record of what it built. Keeping it in the cloud means it isn't lost with my laptop, and two people can't change things at the same time.

**Tradeoffs.**

- **Cost.** Google charges a fee for a cluster whose control system runs in three zones; its free tier only covers a control system in one zone. The free option would be cheaper but less resilient.
- **Public addresses.** The computers have public IP addresses. Private ones are safer but need a NAT gateway to reach the internet, which is another paid service.
- **Small computers.** They are cheap, and the numbers would be higher on bigger ones.
- **Network disks.** Every save to disk travels over Google's network. Disks attached directly to the computer are faster but lose their data when the computer stops, which defeats the point of saving to disk.
- **One deprecated setting.** Terraform warns that `remove_default_node_pool` is deprecated. The newer way only works on the newest Kubernetes versions, so I kept the old way, which works on all of them.

**Interview questions.**

*Why three zones and not two?* The database needs a majority, two of three nodes. With two zones, one zone holds two nodes, and losing it loses the majority. With three, any one zone can fail.

*What happens if the leader's whole zone goes down?* The other two nodes stop hearing from the leader, hold a vote within about half a second, and carry on. Writes that were in progress get an "outcome unknown" error, never a false "OK".

*Why give the computers their own identity?* By default, Google Cloud computers often get an identity that can change almost anything in the project. This one can only write logs and download the database program, so a compromised computer can do much less.

## Running it on Kubernetes

**What I built.** The files in [`deploy/k8s/base`](deploy/k8s/base):

- **A StatefulSet** runs the three nodes. Each one gets a fixed name (`quorumdb-0`, `quorumdb-1`, `quorumdb-2`) and its own 10 GB disk, which follows it when it restarts.
- **A headless Service** gives each node a fixed network name, such as `quorumdb-0.quorumdb-peers`. The nodes use these names to find each other, even after a node restarts with a new IP address.
- **A client Service** is the one address that applications connect to.
- **A PodDisruptionBudget** tells Kubernetes that during maintenance it may only take down one node at a time.
- **Health checks** (Kubernetes calls them probes) on `/healthz` and `/readyz`, which I added to the database.

Settings like the node's name and the list of other nodes come from environment variables, so the same program works on my laptop and in Kubernetes.

**Why.**

- **A StatefulSet, not a Deployment.** A node must always come back with the same name and the same disk, because its saved data belongs to that name. A StatefulSet guarantees that pairing. A Deployment gives pods random names.
- **Network names exist before a node is "ready".** A node only counts as ready once it knows who the leader is, and to find out it must reach the other nodes by name. Without this setting, a new cluster could never start.
- **All three start at once.** By default Kubernetes waits for the first pod to be ready before starting the second, which would never happen here.
- **One node per zone.** A placement rule (topologySpreadConstraints) tells Kubernetes to put exactly one node in each zone, and to wait rather than put two in one zone.
- **Only one program per disk.** The disks are marked ReadWriteOncePod, so two copies of a node can never write to the same data at once, even during a messy restart.
- **Health checks with meaning.** "Ready" means the node knows the current leader, so applications are only sent to nodes that can answer. "Alive" only means the program responds, and Kubernetes waits 30 seconds before restarting it, so a slow node isn't restarted for nothing.
- **No CPU limit.** Kubernetes enforces a CPU limit by pausing the program, which could delay the leader's heartbeats and cause needless elections. Each node is still guaranteed 1 CPU.
- **tini.** The database runs under a tiny helper program called tini, as a regular user rather than as an administrator. Without tini, the chaos test couldn't freeze it (see the chaos test section).

**Tradeoffs.**

- **A node is tied to its zone.** A disk can only attach to a computer in its own zone. If a whole zone goes down, that node waits for the zone to come back instead of moving. The other two keep the database running.
- **Readiness depends on the others.** If two nodes are down, the third becomes "not ready" too. That is correct, because one node alone can't save anything, but it means "not ready" doesn't always mean "this node is broken".
- **The number of nodes is fixed.** Asking Kubernetes for five replicas would not add them to the group, because the list of nodes is set at startup.
- **Inside the cluster only.** There is no password or encryption yet, so the database is not exposed to the internet.

**Interview questions.**

*Why a StatefulSet?* Each node's data belongs to that node's identity. A StatefulSet keeps the name, the network name and the disk together, and never runs two copies of the same node at once.

*A computer dies. What happens?* The other two nodes keep the database running, and elect a new leader if needed. Kubernetes restarts the lost node on a computer in the same zone, where its disk is. The node reads its saved data and catches up on what it missed.

*Why is "ready" defined as "knows the leader"?* A node without a leader can't save anything or give a guaranteed up-to-date answer, so sending users there would only give them errors.

## Cost and cleanup

While it's running, Google charges for:

- the cluster management fee,
- four `e2-standard-2` computers and their 30 GB system disks,
- three 10 GB database disks,
- a public IP address per computer,
- traffic between zones (every save is sent to two other zones),
- storage for the program images, and logs.

Check current prices with Google's [pricing calculator](https://cloud.google.com/products/calculator), and run `make down` as soon as you're done.

`make down` deletes the database first and waits until its three disks are gone, and only then deletes everything Terraform made. The order matters: Kubernetes created those disks, not Terraform, so deleting the cluster first would leave them behind and still billed. Afterwards, these should all print nothing:

```bash
gcloud container clusters list
gcloud compute disks list --filter="name~^pvc-"
gcloud compute instances list
```

The one thing left is the small bucket with Terraform's records. Delete it with `gcloud storage rm -r gs://YOUR_PROJECT-quorumdb-tfstate`.

## The speed test

**What I built.** [`cmd/benchmark`](cmd/benchmark/main.go) finds the leader and a follower, then runs the standard Redis speed test tool, `redis-benchmark`. It sends 200,000 saves and 200,000 reads from 50 connections to the leader one request at a time, then 16 at a time, and then to a follower, three times each. Every run is saved as JSON. On my laptop it runs next to the nodes (`make bench`). On Google Cloud it runs as a one-off Kubernetes job on the test computer (`make gke-bench`). [`cmd/results`](cmd/results/main.go) turns the JSON into the README tables.

**Why.**

- **Run it inside the cluster.** From my laptop, every request would travel through Kubernetes' remote access tunnel, and the test would measure the tunnel.
- **The same tool in both places.** Same code, same settings, same `redis-benchmark` version, so the only differences are the computers and the network.
- **Mixed order.** Each round runs every setting once before the next round starts. If the computers slow down over time, every setting is affected equally.
- **Record where things ran.** Which node is the leader changes over time, so the results say which zone the leader, the follower and the test computer were in.
- **Results come out in the job's log.** The job prints the JSON at the end of its output, and a script cuts it out. No extra storage is needed.

**Tradeoffs.**

- **One test computer.** When sending 16 requests at a time, the 2-CPU test computer may be the limit, not the database.
- **The tool waits for each answer.** When the database slows down, the test sends less, which makes slow moments look better than they would under a fixed load.
- **Three runs.** Enough to see how much results vary, not enough for precise statistics.
- **Shared computers.** Google's `e2` computers share hardware with other customers, so results can vary between days.

**Interview questions.**

*What limits how many saves per second it can do?* Each save waits for two disk writes (the leader's and one follower's) and a round trip to another zone. Saves that arrive together share one round trip and one disk write, which is why sending 16 at a time is so much faster.

*Why are requests through a follower slower?* The follower passes every request to the leader and waits for the answer, which adds an extra network hop, usually to another zone.

*Why not just report the best run?* The best run hides how much results vary. The middle of three, with the lowest and highest, shows it honestly, and the raw numbers are in the repository.

## The chaos test

**What I built.** The chaos test ([`cmd/chaos`](cmd/chaos)) can break the three nodes in two ways, with the same checks:

| Problem | On my laptop | On Google Cloud |
|---|---|---|
| Crash | Kill the program instantly and start it again | Kill the program instantly inside its pod, then delete the pod so Kubernetes starts a new one |
| Freeze | Pause the program and resume it | The same, sent into the pod with `kubectl exec` |
| Cut the network | A small relay between every pair of nodes stops passing traffic | A network rule (NetworkPolicy) blocks traffic into a node from the cut nodes |
| Cut off the leader's zone | Cut every link between the leader and the others | Cut every link between the leader's zone and the other zones |

After each run it checks that every save that got "OK" is still there, that every answer was correct (with the porcupine checker), and that all three nodes ended with the same data. It also measures how long a new leader took to appear. On Google Cloud it runs as a Kubernetes job on the test computer, with permission to restart, freeze and cut off the database pods, and only those.

**Why.**

- **A crash must really be a crash.** When Kubernetes deletes a pod, it first asks the program to stop, and the database saves everything and exits cleanly, which tests nothing. So the test kills it instantly first. Then it deletes the pod, because if the same program crashes again and again, Kubernetes starts waiting longer and longer, up to 5 minutes, before restarting it.
- **tini makes freezing possible.** Linux ignores freeze and kill signals sent to the first program in a container from inside the container. With tini as the first program, the database is a normal second program that can be frozen.
- **One network rule per node.** Kubernetes network rules can only allow traffic, and if two rules apply to one node, anything either allows gets through. Two separate "block" rules would cancel each other out. So the test keeps a list of every cut link and writes exactly one rule per node.
- **One-way cuts work.** Each node opens its own connections to send its messages. Blocking connections into B from A stops A's messages to B, while B's messages to A still get through on B's own connections.
- **Test clients can still reach a cut-off node.** Only traffic between nodes is cut. That's the dangerous case: an old leader that still gets requests must not answer them wrongly.
- **Each run has its own keys.** All runs on Google Cloud share one cluster, so every run uses new key names and deletes them at the end.
- **Counting lost saves directly.** The porcupine checker says yes or no for a whole run. Writing some keys exactly once and reading every one back gives a plain number, "0 lost out of N", that anyone can check.
- **Timing the new leader also checks the test.** If a network rule didn't really cut the leader off, no new leader would be picked, and the test would report it.

**Tradeoffs.**

- **Problems take longer to cause on Google Cloud.** Each command goes through Kubernetes and takes a fraction of a second, against microseconds on my laptop, so leader times there include that delay.
- **A crashed node is down longer.** Kubernetes has to create a new pod and reattach its disk, which takes seconds.
- **Traffic is only dropped.** Network rules silently drop traffic. They can't reject it immediately the way the laptop's relay can.
- **Strong test permissions.** The test can freeze and delete database pods. It is limited to one namespace, but it shouldn't exist in a real production cluster.

**Problems I ran into.**

- **The network cuts didn't cut.** On a test cluster on my laptop (kind), the network rules blocked new connections but not ones that were already open. The nodes keep their connections open, so the leader never noticed it had been cut off. The leader timing showed it: no new leader within 10 seconds. That's why the first step on Google Cloud is a short run to confirm the cuts work there.
- **New leaders appeared impossibly fast.** At first, crash timings were shorter than the minimum election time. The clock started after the test had also asked Kubernetes to delete the pod, which takes time. Now it starts just before the crash.
- **Leaders that could never be elected.** Some timings came from a second problem caused while another was still active. If the other two nodes were already cut off from each other, no leader could ever be picked, so those timings meant nothing. Now only problems with nothing else broken are timed.
- **The laptop got slower, not the code.** My laptop results dropped after these changes. Running the old version gave the same numbers: macOS Low Power Mode was on.

**Interview questions.**

*Why not just delete a pod to simulate a crash?* Deleting a pod is polite: Kubernetes asks the program to stop and gives it time. The database then saves everything cleanly, so a half-written save never happens. A real crash gives no warning, so the test kills the program instantly first.

*How do you know a network cut actually worked?* When the leader is cut off, the other two must stop hearing it and pick a new leader within a second or so. The test times that every time. A cut that did nothing shows up as "no new leader within 10 seconds", which is how the laptop test cluster's problem showed up.

*The checker already looks at every answer. Why count lost saves separately?* Because "every save that got OK is still there" is what people really care about, and checking it key by key gives a number anyone can verify. The broken versions show the two agree: when the database says OK too early, the checker fails every run and the count finds lost saves.

## Running it on Google Cloud for the first time

1. **Log in:** `gcloud auth login`, `gcloud auth application-default login`, `gcloud config set project YOUR_PROJECT`, and `gcloud components install gke-gcloud-auth-plugin`.
2. **Make the bucket for Terraform's records (once):** `make gke-state-bucket`.
3. **Build and deploy:** `make up`. Terraform shows its plan and asks before creating anything; from here on, it costs money. `make gke-status` should show one node per zone, one leader and two followers.
4. **Check that the network cuts work:** `make gke-chaos RUNS=5`. In the summary, the `leader failover, leader-zone` and `leader failover, isolate` lines should show measured times, and few or no cases of `no new leader within 10s`. If most of them show no new leader, stop: the network results wouldn't mean anything.
5. **The full chaos test:** `make gke-chaos`, 100 runs, saved to `results/gke/chaos.json`. If a run fails, the job prints a command to copy its details, and keeps them for an hour.
6. **The speed test:** `make gke-bench`, saved to `results/gke/benchmark.json`. It first switches the database back to its normal settings.
7. **Update the tables:** `make results`, then commit the JSON files with the README.
8. **Delete everything:** `make down`, then run the three checks in [Cost and cleanup](#cost-and-cleanup).

Worth taking screenshots of: `make gke-status` showing the three zones, `kubectl get pods -n quorumdb -o wide` showing each node on a different computer, the chaos job's output, and the finished results table.
