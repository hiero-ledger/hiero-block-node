# Block Node Hardware Specifications

This document defines the minimum hardware requirements and storage/network benchmark
targets for running a Block Node (BN) in a production mainnet environment.

## Table of Contents

1. [Minimum Server Specifications](#minimum-tier-1-server-specifications)
2. [Storage Benchmark Targets](#storage-benchmark-targets)
3. [Network Requirements](#network-requirements)
4. [Network Throughput and Storage Growth Estimates](#network-throughput-and-storage-growth-estimates)
5. [Additional Considerations](#additional-considerations)

---

## Minimum Tier 1 Server Specifications

Two deployment profiles are supported based on how block history is stored at the Tier 1 level:

> Note: Tier 2 operators have the flexibility of choice. Here we recommend they adopt the Remote
> Full History specs and customize their HDD storage size based on their business needs and
> deployed plugins.

### 1. Local Full History (LFH)

All block history is stored locally on the server.

| Component | Minimum Specification                                     |
|-----------|-----------------------------------------------------------|
| CPU       | 24 cores / 48 threads, 2024 or newer (PCIe 4+), ≥ 2.0 GHz |
| RAM       | 256 GB                                                    |
| Fast NVMe Disk | 8 TB NVMe SSD                                             |
| Bulk Storage Disk | 100 TB                                                    |
| Network   | 2 × 10 Gbps NICs                                          |
| OS        | Linux (Ubuntu 22.04 LTS or Debian 11 LTS recommended)     |

### 2. Remote Full History (RFH)

Block history is stored remotely (e.g. cloud object store).

| Component | Minimum Specification |
|-----------|----------------------|
| CPU       | 24 cores / 48 threads, 2024 or newer (PCIe 4+), ≥ 2.0 GHz |
| RAM       | 256 GB |
| Fast NVMe Disk | 8 TB NVMe SSD |
| Network   | 2 × 10 Gbps NICs |
| OS        | Linux (Ubuntu 22.04 LTS or Debian 11 LTS recommended) |

**Recommendations:**

- NICs: 20 Gbps or higher are recommended for better throughput and future-proofing,
  even though 10 Gbps is the stated minimum.
- Bulk storage: 500 TB is recommended for LFH to accommodate long-term block history
  and state growth. 300 Tb would be the next preferred and 100 TB the minimum for Tier 1.
- Servers may be sourced from bare metal providers or cloud providers offering dedicated
  instances. LFH configurations require significant storage capacity and are typically
  sourced from bare metal providers.
- OS disk requirements are minimal; the OS disk sees little activity after start-up and
  does not require explicit sizing beyond standard OS installation needs.

---

## Storage Benchmark Targets

The Block Node is I/O-intensive. The following benchmarks define the sustained throughput,
IOPS, and latency targets that storage must meet to avoid becoming a bottleneck.

### Disk Performance Targets

| Disk Type  | Sustained Write | Sustained Read | Required Write IOPS | Required Read IOPS | P99 Write Latency | P99 Read Latency |
|------------|-----------------|----------------|---------------------|--------------------|-------------------|------------------|
| Fast NVMe  | 4.8 Gbps        | 19.8 Gbps      | 250k                | 800k               | < 300 µs          | < 200 µs         |
| Bulk Disk  | 2 Gbps          | 10 Gbps        | 150k                | 500k               | —                 | —                |

**Notes:**

- IOPS profile numbers are averages; peak IOPS will be defined by the speed of cache,
  not the speed of the disk itself.
- The Fast NVMe disk serves recent/live block storage; the Bulk Disk serves the
  historic block archive in LFH configurations.
- P99 latency targets apply only to the Fast NVMe tier. Bulk Disk latency is
  workload-dependent and not explicitly bounded.

---

## Network Requirements

| Requirement | Target |
|-------------|--------|
| Minimum NIC throughput | 10 Gbps (20 Gbps recommended) |
| CN ↔ BN latency | < 100 µs (TOR switch crossing only) |
| Colocation | CNs and BNs should be in the same data center, ideally the same rack |

**Notes:**

- Consensus Nodes (CNs) and Block Nodes (BNs) must be colocated in the same data
  center. Same-rack placement is strongly preferred to keep round-trip latency within
  the < 100 µs budget, which assumes traffic crosses only a Top-of-Rack (TOR) switch.
- Higher inter-node latency risks stream backpressure and increased buffering requirements.

---

## Network Throughput and Storage Growth Estimates

This section provides capacity planning estimates for operators sizing storage and network links.
All figures are derived from a linear block-size model fitted to 13 real mainnet blocks from an
~11K TPS mixed-workload test (R² = 0.9996). The model constants are:

```
Block_gz(T)   =   88,963 + 372.8 × T   bytes   (on-disk, gzip-compressed)
Block_ungz(T) = -245,737 + 910.5 × T   bytes   (wire, uncompressed)

T = transactions per block = TPS × block_interval
```

**Assumptions used in the tables below:**
- Block interval: 1 second (1 block/sec - double that of the network in early 2026 but good for scaling)
- Compression ratio: 2.39× (gzip, from v3 mixed-workload model)
- Worst-case egress subscribers: 33 (13 Block Nodes backfilling + 10 Mirror Nodes + 10 DApps)
- Worst-case ingress: 8× parallel catch-up streams from Consensus Nodes

### Block Size by TPS

| TPS    | Tx/block | On-disk / block (gz) | Wire size / block (ungz) |
|-------:|---------:|---------------------:|-------------------------:|
|  2,000 |    2,000 |               0.83 MB |                  1.58 MB |
| 10,000 |   10,000 |               3.64 MB |                  8.69 MB |
| 20,000 |   20,000 |               7.20 MB |                 17.13 MB |

### Daily and Monthly On-Disk Storage (local block files, gz)

> These figures cover raw block storage only. Allow additional headroom for OS, JVM,
> indexes, and recent (uncompressed) working files (~1.5–2× the gz figures).

| TPS    | Per day (gz) | Per month (gz) |
|-------:|-------------:|---------------:|
|  2,000 |      69 GB   |       2.1 TB   |
| 10,000 |     314 GB   |       9.4 TB   |
| 20,000 |     622 GB   |      18.7 TB   |

**Planning target (20% headroom over model):**

| TPS    | Per day (planned) | Per month (planned) |
|-------:|------------------:|--------------------:|
|  2,000 |         83 GB     |         2.5 TB      |
| 10,000 |        377 GB     |        11.3 TB      |
| 20,000 |        747 GB     |        22.4 TB      |

### Ingress Bandwidth (Consensus Node → Block Node)

Steady-state ingress carries one uncompressed block stream per second.
Worst-case reflects 8 Consensus Nodes simultaneously streaming to a single BN.

| TPS    | Steady-state ingress | Worst-case ingress (8× catch-up) |
|-------:|---------------------:|---------------------------------:|
|  2,000 |            ~60 Mbps  |                      ~480 Mbps   |
| 20,000 |           ~600 Mbps  |                    ~4,800 Mbps   |

> At 20K TPS worst-case, ingress alone approaches the 10 Gbps NIC minimum.
> A 20 Gbps NIC (or bonded pair) is **strongly recommended** for any deployment
> expected to operate at ≥ 10K TPS or to backfill aggressively.

### Egress Bandwidth (Block Node → Subscribers)

Each downstream subscriber (Mirror Node, Block Node, DApp) receives its own uncompressed stream.

| TPS    | Per subscriber / day | Per subscriber / month | 33 subscribers / day | 33 subscribers / month |
|-------:|---------------------:|-----------------------:|---------------------:|-----------------------:|
|  2,000 |             6.5 TB   |             196 TB     |            215 TB    |              6.5 PB    |
| 20,000 |            65 TB     |             1.9 PB     |           2,145 TB   |             64.4 PB    |

**Worst-case bandwidth peaks:**

| TPS    | Worst-case egress (33 subscribers) |
|-------:|------------------------------------:|
|  2,000 |                        ~2,000 Mbps  |
| 20,000 |                       ~20,000 Mbps  |

> At 20K TPS with 33 subscribers (1/3 of max BNs (13) backfilling, 10 MNs and 10 DApps subscribed),
> egress at ~20 Gbps saturates the 10 Gbps minimum NIC.
> Block Nodes serving many live subscribers at high TPS **require** at least a 20 Gbps NIC
> and may need 40 Gbps or bonded links for headroom.

### Sizing Summary

| Scenario | On-disk (1 year, gz, no headroom) | Peak ingress | Peak egress (33 sub) | NIC minimum |
|----------|----------------------------------:|-------------:|---------------------:|------------:|
| 2K TPS   |                            25 TB  |    480 Mbps  |            ~2 Gbps   |   10 Gbps   |
| 20K TPS  |                           227 TB  |  4,800 Mbps  |           ~20 Gbps   |   20+ Gbps  |

> The 100 TB bulk disk minimum (LFH) covers approximately 4 years at 2K TPS or ~5 months at 20K TPS
> (gz only). The recommended 500 TB covers ~4 years at 10K TPS.

---

## Additional Considerations

- **Clock speed**: Base CPU clock speed must be ≥ 2.0 GHz. Higher clock speeds reduce
  per-block processing latency, which is important for keeping up with mainnet block
  production rates.
- **PCIe generation**: PCIe 4.0 or higher is required to sustain the NVMe throughput
  targets above. PCIe 3.0 configurations will likely be bandwidth-limited.
- **CPU vintage**: A 2024 or newer CPU is recommended to benefit from improved
  single-thread IPC and PCIe 4+ native support.
- **OS disk**: Minimal sizing is sufficient; the OS disk sees negligible I/O after
  initial start-up.
