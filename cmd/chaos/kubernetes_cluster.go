package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/simar-s2/quorumdb/internal/kube"
	"github.com/simar-s2/quorumdb/internal/netproxy"
	"github.com/simar-s2/quorumdb/internal/results"
)

// kubernetesCluster is the QuorumDB StatefulSet in a Kubernetes cluster, driven with kubectl from a
// pod inside that cluster. Unlike the local cluster it is not created for each run: every run uses
// the same three pods, with its own keys.
type kubernetesCluster struct {
	namespace string
	pods      [nodeCount]string // quorumdb-0, quorumdb-1, quorumdb-2
	addresses [nodeCount]string // quorumdb-0.quorumdb-peers:6379, ...
	zones     [nodeCount]string // a pod never changes zone: its disk is zonal

	// blocked[to][from] is true while traffic from node "from" to node "to" is cut.
	blocked [nodeCount][nodeCount]bool
}

// chaosPolicyLabel marks the NetworkPolicies this test creates, so healing can delete them all.
const chaosPolicyLabel = "app.kubernetes.io/managed-by=quorumdb-chaos"

// connectKubernetesCluster heals anything a previous run left behind and waits until all three pods
// are ready, so every run starts from a healthy cluster.
func connectKubernetesCluster(namespace string) (*kubernetesCluster, error) {
	c := &kubernetesCluster{namespace: namespace}
	for i := 0; i < nodeCount; i++ {
		c.pods[i] = fmt.Sprintf("quorumdb-%d", i)
		c.addresses[i] = fmt.Sprintf("quorumdb-%d.quorumdb-peers:6379", i)
	}
	if err := c.healAndWait(); err != nil {
		return nil, err
	}
	for i, pod := range c.pods {
		vm, err := kube.NodeOfPod(namespace, pod)
		if err != nil {
			return nil, err
		}
		if c.zones[i], _, err = kube.ZoneAndMachineType(vm); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *kubernetesCluster) nodeName(i int) string     { return c.pods[i] }
func (c *kubernetesCluster) redisAddress(i int) string { return c.addresses[i] }
func (c *kubernetesCluster) zone(i int) string         { return c.zones[i] }

// exec runs a command in a QuorumDB container, like `kubectl exec quorumdb-1 -- pidof quorumdb`.
func (c *kubernetesCluster) exec(pod string, command ...string) (string, error) {
	arguments := append([]string{"-n", c.namespace, "exec", pod, "-c", "quorumdb", "--"}, command...)
	return kube.Kubectl(arguments...)
}

// kill crashes the node with SIGKILL, then deletes its pod so the StatefulSet controller creates a
// new one that reattaches the same disk.
//
// Deleting the pod alone would not be a crash: Kubernetes sends SIGTERM first and QuorumDB shuts down
// cleanly, flushing its log. Killing the process alone is not enough either: the kubelet restarts the
// container in place, but after a few crashes it waits longer and longer, up to 5 minutes
// (CrashLoopBackOff). A new pod starts with no such history.
func (c *kubernetesCluster) kill(i int) (time.Time, bool) {
	pod := c.pods[i]
	// The kill itself cannot report success (the container, and the kill command in it, die with the
	// server), so first check that the server is running.
	if _, err := c.exec(pod, "pidof", "quorumdb"); err != nil {
		return time.Time{}, false
	}
	killedAt := time.Now()
	c.exec(pod, "sh", "-c", "kill -KILL $(pidof quorumdb)")
	// Delete in the background so the nemesis can start timing the failover right away.
	go kube.Kubectl("-n", c.namespace, "delete", "pod", pod, "--wait=false")
	return killedAt, true
}

// restart does nothing: the StatefulSet controller recreates a killed pod by itself.
func (c *kubernetesCluster) restart(i int) {}

// setFrozen sends SIGSTOP or SIGCONT to the server process through kubectl exec. A frozen leader
// still believes it leads, which tests that it cannot serve stale reads or lose writes. This is why
// the image runs QuorumDB under tini: the kernel ignores SIGSTOP sent to a container's PID 1 from
// inside the container, but quorumdb is tini's child, not PID 1.
func (c *kubernetesCluster) setFrozen(i int, frozen bool) bool {
	signal := "CONT"
	if frozen {
		signal = "STOP"
	}
	// If the server is not running, pidof prints nothing and kill fails, so this reports false.
	_, err := c.exec(c.pods[i], "sh", "-c", "kill -"+signal+" $(pidof quorumdb)")
	return err == nil
}

// linkModes: a NetworkPolicy can only drop traffic. There is no equivalent of the local proxy's reset.
func (c *kubernetesCluster) linkModes() []netproxy.Mode { return []netproxy.Mode{netproxy.Drop} }

// setLinks cuts or restores links with NetworkPolicy, which GKE Dataplane V2 enforces.
//
// NetworkPolicy can only allow traffic, and when several policies select one pod, anything one of them
// allows is allowed. So separate "block quorumdb-0" and "block quorumdb-2" policies on the same pod
// would cancel each other out. Instead this remembers every cut link and writes exactly one policy per
// receiving pod, listing which peers may still reach its Raft port. A pod with no cut links has none.
//
// Blocking only what a pod receives is enough, even for one-way cuts: each node opens its own TCP
// connections for the RPCs it sends. Blocking connections from A into B stops A's RPCs to B, while
// B's RPCs to A, on connections B opened, still work.
func (c *kubernetesCluster) setLinks(links []link, mode netproxy.Mode) {
	receivers := map[int]bool{}
	for _, l := range links {
		c.blocked[l.to][l.from] = mode != netproxy.Pass
		receivers[l.to] = true
	}
	var policies, policiesToDelete []string
	for to := range receivers {
		var blockedSenders []string
		for from := 0; from < nodeCount; from++ {
			if c.blocked[to][from] {
				blockedSenders = append(blockedSenders, c.pods[from])
			}
		}
		if len(blockedSenders) == 0 {
			policiesToDelete = append(policiesToDelete, policyName(c.pods[to]))
		} else {
			policies = append(policies, receiverPolicy(c.namespace, c.pods[to], blockedSenders))
		}
	}
	if len(policies) > 0 {
		if _, err := kube.KubectlWithInput(strings.Join(policies, "---\n"), "apply", "-f", "-"); err != nil {
			fmt.Fprintf(os.Stderr, "chaos: %v\n", err)
		}
	}
	if len(policiesToDelete) > 0 {
		arguments := append([]string{"-n", c.namespace, "delete", "networkpolicy", "--ignore-not-found"}, policiesToDelete...)
		if _, err := kube.Kubectl(arguments...); err != nil {
			fmt.Fprintf(os.Stderr, "chaos: %v\n", err)
		}
	}
}

func policyName(receiver string) string { return "chaos-cut-into-" + receiver }

// receiverPolicy is the NetworkPolicy for one pod whose incoming Raft traffic from some peers is cut.
// The Redis and health ports stay open to everyone, so the test clients can still reach a cut-off
// node (as in the local test, where only links between nodes are cut) and the kubelet can still
// probe it. The Raft port is open only to pods not in blockedSenders. NotIn also matches pods that
// have no pod-name label at all, such as the test client.
func receiverPolicy(namespace, receiver string, blockedSenders []string) string {
	return fmt.Sprintf(`apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: %s
  namespace: %s
  labels:
    app.kubernetes.io/managed-by: quorumdb-chaos
spec:
  podSelector:
    matchLabels:
      statefulset.kubernetes.io/pod-name: %s
  policyTypes: [Ingress]
  ingress:
  - ports:
    - port: 6379
    - port: 8080
  - from:
    - podSelector:
        matchExpressions:
        - key: statefulset.kubernetes.io/pod-name
          operator: NotIn
          values: [%s]
    ports:
    - port: 7000
`, policyName(receiver), namespace, receiver, strings.Join(blockedSenders, ", "))
}

// healAll removes every cut, resumes frozen nodes and waits for every pod to be back and ready.
func (c *kubernetesCluster) healAll() {
	if err := c.healAndWait(); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: %v\n", err)
	}
}

func (c *kubernetesCluster) healAndWait() error {
	if _, err := kube.Kubectl("-n", c.namespace, "delete", "networkpolicy", "-l", chaosPolicyLabel, "--ignore-not-found"); err != nil {
		return err
	}
	c.blocked = [nodeCount][nodeCount]bool{}
	// A frozen pod never becomes ready, so resume every node first. SIGCONT to a running process does nothing.
	for i := 0; i < nodeCount; i++ {
		c.setFrozen(i, false)
	}
	return c.waitAllReady(5 * time.Minute)
}

// waitAllReady waits until all three pods exist, are not being deleted, and pass their readiness probe.
func (c *kubernetesCluster) waitAllReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		// One line per pod: "<name> <Ready condition> <deletion time, if being deleted>".
		output, err := kube.Kubectl("-n", c.namespace, "get", "pods", "-l", "app=quorumdb", "-o",
			`jsonpath={range .items[*]}{.metadata.name} {.status.conditions[?(@.type=="Ready")].status} {.metadata.deletionTimestamp}{"\n"}{end}`)
		ready := 0
		if err == nil {
			for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
				if fields := strings.Fields(line); len(fields) == 2 && fields[1] == "True" {
					ready++
				}
			}
		}
		if ready == nodeCount {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("only %d of %d QuorumDB pods ready after %s: %v %s", ready, nodeCount, timeout, err, output)
		}
		time.Sleep(time.Second)
	}
}

// stop leaves the cluster running and healthy for the next run.
func (c *kubernetesCluster) stop() { c.healAll() }

// saveLogs copies each pod's log since the run started into dir, for a failed run. A pod that was
// killed and recreated only has the new pod's log; the old one is in the cluster's logging system.
func (c *kubernetesCluster) saveLogs(dir string, since time.Time) {
	for _, pod := range c.pods {
		output, err := kube.Kubectl("-n", c.namespace, "logs", pod, "-c", "quorumdb", "--since-time="+since.UTC().Format(time.RFC3339))
		if err != nil {
			output += "\n" + err.Error()
		}
		os.WriteFile(filepath.Join(dir, pod+".log"), []byte(output), 0o644)
	}
}

// checkKubernetesSettings makes sure the StatefulSet runs with the settings this test reports, the
// same ones the local test uses: a snapshot every snapshotAt entries and a 2 s command timeout.
func checkKubernetesSettings(namespace string, snapshotAt int) error {
	environmentValue := func(name, fallback string) (string, error) {
		output, err := kube.Kubectl("-n", namespace, "get", "statefulset", "quorumdb", "-o",
			fmt.Sprintf(`jsonpath={.spec.template.spec.containers[0].env[?(@.name=="%s")].value}`, name))
		if value := strings.TrimSpace(output); value != "" || err != nil {
			return value, err
		}
		return fallback, nil
	}
	threshold, err := environmentValue("QUORUMDB_SNAPSHOT_THRESHOLD", "10000")
	if err != nil {
		return err
	}
	commandTimeout, err := environmentValue("QUORUMDB_COMMAND_TIMEOUT", "5s")
	if err != nil {
		return err
	}
	if threshold != fmt.Sprint(snapshotAt) || commandTimeout != "2s" {
		return fmt.Errorf("the StatefulSet has snapshot threshold %s and command timeout %s, want %d and 2s; "+
			"deploy deploy/k8s/chaos-settings first (scripts/gke-deploy.sh chaos-settings)", threshold, commandTimeout, snapshotAt)
	}
	return nil
}

// kubernetesPlacement records the zone and machine type of each QuorumDB pod and of this client.
func kubernetesPlacement(namespace string) ([]results.Node, results.Client) {
	var nodes []results.Node
	for i := 0; i < nodeCount; i++ {
		node := results.Node{ID: fmt.Sprintf("quorumdb-%d", i), Address: fmt.Sprintf("quorumdb-%d.quorumdb-peers:6379", i)}
		if vm, err := kube.NodeOfPod(namespace, node.ID); err == nil {
			node.Zone, node.MachineType, _ = kube.ZoneAndMachineType(vm)
		}
		nodes = append(nodes, node)
	}
	var client results.Client
	if vm := os.Getenv("NODE_NAME"); vm != "" {
		client.Zone, client.MachineType, _ = kube.ZoneAndMachineType(vm)
	}
	return nodes, client
}
