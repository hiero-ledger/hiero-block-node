# Chaos templates

This directory holds the [Chaos Mesh](https://chaos-mesh.org/) `NetworkChaos`
manifest templates used by `solo-e2e-test`'s latency and bandwidth chaos
events. This README explains how the pieces fit together, the two failure
modes that consumed most of the tuning effort, and what to change when
writing or adjusting a chaos test.

- Chaos Mesh docs: https://chaos-mesh.org/docs/simulate-network-chaos-on-kubernetes/
- Chaos Mesh NetworkChaos API reference: https://chaos-mesh.org/docs/next/simulate-network-chaos-on-kubernetes/
- `solo-chaos` (sibling tooling this integration is modeled on/aims to converge with): `hashgraph/solo-chaos`

## How a chaos event runs

1. A test YAML under `../../tests/*.yaml` declares an `events` list. An event
   with `type: inject-latency` or `type: inject-bandwidth` carries an `args`
   block describing `source`, `target`, and the fault parameters.
2. `solo-test-runner.sh`'s `execute_inject_latency` / `execute_inject_bandwidth`
   resolve `source`/`target` into Kubernetes label selectors (or, for
   bandwidth's `target.kind: service`, a Service ClusterIP — see below),
   validate at least one pod matches via `chaos-dryrun.sh`, then render the
   matching template here with `envsubst` and `kubectl apply` it.
3. `wait_for_networkchaos_injected` blocks until Chaos Mesh reports the
   resource's `AllInjected` condition, failing fast with `kubectl describe`
   output if injection doesn't succeed within 30s — this catches control-plane
   failures (webhook rejection, CRD not ready) early. **It does not confirm
   the fault is actually affecting real traffic** — see "Verifying a fault is
   real" below.
4. A matching `clear-latency` / `clear-bandwidth` event later deletes the
   `NetworkChaos` resource by the same deterministic name
   (`chaos_resource_name`, derived from the test name + event `name` arg).

Both templates share the same pod-label convention:

| Side | Label selector | Source |
|------|-----------------|--------|
| Consensus node (CN) | `solo.hedera.com/type=network-node` | emitted by Solo CLI |
| Block node (BN) | `block-node.hiero.com/type=block-node` | emitted by the BN Helm chart |

`chaos_label_selector` in `solo-test-runner.sh` maps the test YAML's
`kind: network-node` / `kind: block-node` to these. An optional `name:` field
adds an `app.kubernetes.io/instance=<name>` filter to target one specific
pod (e.g. `block-node-1`) instead of all pods of that kind.

## Latency injection (`network-latency.yaml.tmpl`)

Uses Chaos Mesh's `netem` action (delay/jitter/correlation, optionally
packet loss). `direction: to|from|both` and `bidirectional: true|false` in
the event's `args` control which packets are affected, matching Chaos Mesh's
own semantics directly — see the Chaos Mesh docs linked above for the exact
meaning of each `direction` value. This action supports both pod-selector
and `direction`-based targeting without the gotchas below, because `netem`
faults don't need packet-header address matching the way `bandwidth` does.

## Bandwidth injection (`network-bandwidth.yaml.tmpl`)

Uses Chaos Mesh's `bandwidth` action (`tc tbf` — token bucket filter) to cap
throughput. This action has three non-obvious behaviors that cost significant
debugging time (PR #3387) and are easy to get wrong when writing a new test:

### 1. Rate units: `kbit` vs `kbps`

Chaos Mesh's `rate` field uses `kbit`/`mbit`/`gbit`/... for **kilobits/sec**
and `kbps`/`mbps`/`gbps`/... for **kilobytes/sec** (8x `kbit`) — the reverse
of common networking shorthand, where "kbps" is usually read as kilobits/sec.
A test written with `rate: "100kbps"` intending "100 kilobits/sec" is
silently applied as 800 kilobits/sec — 8x looser than intended, with no
error. **Always use the `kbit` family when expressing a rate in bits/sec.**

### 2. Bandwidth shaping is always egress-only, on the `selector` pod

The `tc tbf` qdisc this action installs is always attached to the
**`selector`** pod's own network interface, shaping **outbound** traffic.
`direction: to` vs `direction: from` only changes which IP field (destination
vs source) the packet filter matches — it does not create an ingress qdisc.
Setting `selector` to the node you want to *throttle the inbound rate of*,
with `direction: from`, does not work: it just shapes that pod's own egress,
filtered for a source address (the sender) that can never appear on packets
*it* sends. To cap how fast a target receives data, the qdisc must live on
the **sender's** egress with `direction: to` and the target as the packet
filter.

### 3. Targeting a Service vs a pod IP

Chaos Mesh's target-matching ipset is built from resolved **pod IPs** and is
evaluated inside the **sender's own network namespace** — before kube-proxy's
DNAT (ClusterIP → pod IP) happens on the host side for Service-routed
traffic. If the sender reaches the target via a Kubernetes Service
(`<name>.<namespace>.svc.cluster.local` → ClusterIP), the packet is *still
addressed to the ClusterIP* at the point the qdisc filter checks it, so a
pod-selector `target:` block never matches — the throttled `tc` band shows
`0 bytes` moved for the entire test, at any rate, and Chaos Mesh's own
`AllInjected=True` status gives no indication anything is wrong.

The fix used here: `target: { kind: service, name: <service-name> }` in the
event's `args`. `execute_inject_bandwidth` resolves that Service's ClusterIP
at runtime (`kubectl get svc <name> -o jsonpath='{.spec.clusterIP}'`) and
renders it as Chaos Mesh's [`externalTargets`](https://chaos-mesh.org/docs/simulate-network-chaos-on-kubernetes/)
field instead of a pod selector — `externalTargets` accepts arbitrary
IPs/domains and is checked against the packet's real destination address,
which *is* the ClusterIP at that point, so it matches correctly. This only
works with `direction: to`, which is also what point 2 above requires anyway.

**Rule of thumb:** if the sender addresses the target by a Service DNS name
anywhere in its config, use `target: { kind: service, name: ... }`. If it
addresses the target by pod IP directly (rare in this harness), a normal
pod-selector `target: { kind: ..., name: ... }` is fine.

## Verifying a fault is real

`AllInjected=True` only means Chaos Mesh believes the rule was applied — it
says nothing about whether real traffic is being shaped. If a bandwidth test
shows no measurable effect and you suspect the fault isn't actually landing,
check the `tc` counters directly. App container images in this harness don't
ship `tc`/`ip`/`ipset`, so use an ephemeral debug container that shares the
pod's network namespace instead of a plain `kubectl exec`:

```bash
# tc qdisc counters (the throttled tbf band's "Sent N bytes" should grow)
kubectl debug <pod> -n solo-network --image=nicolaka/netshoot \
  --target=<container> -i --quiet -- tc -s qdisc show

# ipset contents (needs NET_ADMIN — the default debug profile doesn't have it)
kubectl debug <pod> -n solo-network --image=nicolaka/netshoot \
  --target=<container> --profile=netadmin -i --quiet -- ipset list
```

Container names in this harness: `root-container` for CN pods,
`block-node-server` for BN pods. Note `-i` (`--stdin`) is required for
`kubectl debug` to wait for the container and attach — without it, the
command patches the ephemeral container into the pod spec and returns
immediately without ever running/attaching, which looks like "no output" but
isn't an error. If you script this (e.g. call it from inside
`solo-test-runner.sh`'s event loop), redirect the command's own stdin from
`/dev/null` — the event loop reads events from a piped `while read`, and an
unredirected `kubectl debug -i` will consume lines meant for that loop.

## Adding or modifying a bandwidth-lag test

The `bandwidth-lag*.yaml` tests under `../../tests/` follow one pattern:
throttle CN→BN1 and BN-peers→BN1 ingest for a window, snapshot block heights
at peak chaos, clear the throttle, and assert on the resulting divergence and
recovery. To change severity, edit the `rate` in both `inject-bandwidth`
events (keep `limit`/`buffer` proportionate — see each file's own comments)
and retune `min_spread`/`tolerance_blocks` against what a CI run actually
observes; these values are empirical, not derived from a formula.

**Known issue affecting recovery assertions:** at 100kbit/s for 150s, the
consensus-node↔block-node connection can enter a permanent reconnect
deadlock that doesn't resolve even after the throttle clears — see
`agent/proposals/solo-chaos-spike/findings/005-cn-bn-reconnect-deadlock-after-bandwidth-starvation.md`
(local, not committed) for the full root-cause chain. Tune severity with that
in mind: a cap tight enough to deadlock the connection will fail
`blocks-converged`/`blocks-increasing`/`block-rate-floor` indefinitely, not
just slowly.
