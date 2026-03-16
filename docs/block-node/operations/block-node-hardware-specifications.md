# Block Node Hardware Specifications

This document defines the minimum hardware requirements and storage/network benchmark
targets for running a Block Node (BN) in a production mainnet environment.

## Table of Contents

1. [Minimum Server Specifications](#minimum-tier-1-server-specifications)
2. [Storage Benchmark Targets](#storage-benchmark-targets)
3. [Network Requirements](#network-requirements)
4. [Additional Considerations](#additional-considerations)

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
