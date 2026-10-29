// Package kube runs kubectl for the benchmark and chaos tools when they run inside the cluster.
// Inside a pod, kubectl finds the API server and the pod's service account token by itself.
package kube

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// commandTimeout bounds every kubectl call, so a slow API server cannot hang a run.
const commandTimeout = 30 * time.Second

// Kubectl runs kubectl with the given arguments and returns its standard output.
func Kubectl(arguments ...string) (string, error) {
	return KubectlWithInput("", arguments...)
}

// KubectlWithInput runs kubectl with input on standard input, as `kubectl apply -f -` needs.
func KubectlWithInput(input string, arguments ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, "kubectl", arguments...)
	command.Stdin = strings.NewReader(input)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return stdout.String(), fmt.Errorf("kubectl %s: %v: %s", strings.Join(arguments, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// NodeOfPod returns the name of the node (VM) a pod is running on.
func NodeOfPod(namespace, pod string) (string, error) {
	output, err := Kubectl("-n", namespace, "get", "pod", pod, "-o", "jsonpath={.spec.nodeName}")
	return strings.TrimSpace(output), err
}

// ZoneAndMachineType reads the zone and machine type labels that GKE puts on every node.
func ZoneAndMachineType(node string) (zone, machineType string, err error) {
	output, err := Kubectl("get", "node", node, "-o",
		`jsonpath={.metadata.labels.topology\.kubernetes\.io/zone}|{.metadata.labels.node\.kubernetes\.io/instance-type}`)
	if err != nil {
		return "", "", err
	}
	zone, machineType, _ = strings.Cut(strings.TrimSpace(output), "|")
	return zone, machineType, nil
}
