# Block Node Hardware Specifications

This document defines the minimum hardware requirements and storage/network
benchmark targets for running a Block Node (BN) in a production mainnet
environment.

## Table of Contents

1. [Minimum Server Specifications](#minimum-tier-1-server-specifications)
2. [Storage Benchmark Targets](#storage-benchmark-targets)
3. [Network Requirements](#network-requirements)
4. [Network Throughput and Storage Growth Estimates](#network-throughput-and-storage-growth-estimates)
5. [Additional Considerations](#additional-considerations)

---

## Minimum Tier 1 Server Specifications

Two deployment profiles are supported based on how block history is stored at
the Tier 1 level:

> Note: Tier 2 operators have the flexibility of choice. Here we recommend they
> adopt the Local Full History specs and customize their HDD storage size based
> on their business needs, depreciation cycles, and deployed plugins.

### 1. Local Full History (LFH)

All block history is stored locally on the server. The NVMe holds recent/live
blocks and live state; the bulk disk holds the long-term compressed block
archive.

|     Component     |                   Minimum Specification                   |
|-------------------|-----------------------------------------------------------|
| CPU               | 24 cores / 48 threads, 2024 or newer (PCIe 4+), ≥ 2.0 GHz |
| RAM               | 256 GB                                                    |
| Fast NVMe Disk    | 8 TB NVMe SSD (recent blocks + live state)                |
| Bulk Storage Disk | 100 TB HDD or equivalent (compressed block archive)       |
| Network           | 2 × 10 Gbps NICs                                          |
| OS                | Linux (Ubuntu 24.04 LTS or Debian 13.4 LTS recommended)   |

### 2. Remote Full History (RFH)

Block history is stored remotely (e.g. cloud object store). The NVMe holds
recent/live blocks and live state only; historical data is offloaded to object
storage.

|   Component    |                   Minimum Specification                   |
|----------------|-----------------------------------------------------------|
| CPU            | 24 cores / 48 threads, 2024 or newer (PCIe 4+), ≥ 2.0 GHz |
| RAM            | 256 GB                                                    |
| Fast NVMe Disk | 8 TB NVMe SSD (recent blocks + live state)                |
| Network        | 2 × 10 Gbps NICs                                          |
| OS             | Linux (Ubuntu 24.04 LTS or Debian 13.4 LTS recommended)   |

#### Recommendations

* NICs: 25 Gbps or higher are recommended for better throughput and
  future-proofing, although 10 Gbps is the stated minimum.
* Bulk storage: 500 TB is recommended for LFH to accommodate long-term block
  history and state growth. A lower 300 TB is considered adequate, potentially
  with a shorter upgrade timeline, and 100 TB is the minimum requirement for
  Tier 1.
* Servers may be sourced from bare metal providers or cloud providers offering
  dedicated instances. LFH configurations require significant storage capacity
  and are typically sourced from bare metal providers or purchased outright for
  self hosting or colocation.
* OS disk requirements are minimal; the OS disk sees little activity after
  start-up and does not require explicit sizing beyond standard OS installation
  needs and at least 10 GB for OCI image storage.

---

## Storage Benchmark Targets

The Block Node is I/O-intensive. The following benchmarks define the
**aggregate** sustained throughput, IOPS, and latency targets that storage must
meet to avoid becoming a bottleneck. Values represent aggregate disk performance
across all drives in the configuration, not per-drive requirements.

### Disk Performance Targets

| Disk Type | Sustained Write | Sustained Read | Required Write IOPS | Required Read IOPS | Random Read AIO IOPS | P99 Write Latency | P99 Read Latency |
|-----------|-----------------|----------------|---------------------|--------------------|----------------------|-------------------|------------------|
| Fast NVMe | 4 GBps          | 6 GBps         | 350k                | 900k               | 1M                   | < 300 µs          | < 200 µs         |
| Bulk Disk | 300 MBps        | 1 GBps         | 150k                | 500k               | -                    | —                 | —                |

#### Notes

* IOPS profile numbers are averages; peak IOPS will be defined by the speed of
  cache, not the speed of the disk itself.
* The Fast NVMe disk serves recent/live block storage and live state management;
  the Bulk Disk serves the historic block archive in LFH configurations.
* P99 latency targets apply only to the Fast NVMe tier. Bulk Disk latency is
  workload-dependent and not explicitly bounded.

---

## Network Requirements

|       Requirement        |            Target             |
|--------------------------|-------------------------------|
| Minimum NIC throughput   | 10 Gbps (25 Gbps recommended) |
| CN ↔ BN latency          | < 10ms total P95              |
| CN ↔ BN ↔ Client latency | < 25ms total P95              |

#### Notes

* Consensus Nodes (CNs) and Block Nodes (BNs) must have strong and stable
  network connections without excessive latency.
* Excessive (over 30ms) inter-node latency risks stream backpressure and
  increased buffering requirements.

---

## Network Throughput and Storage Growth Estimates

This section provides capacity planning estimates for operators sizing storage
and network links. All figures are derived from a linear block-size model fitted
to 13 real mainnet blocks from an ~11K TPS mixed-workload test (R² = 0.9996).
The model constants are:

```
Block_zstd(T) =   88,963 + 372.8 × T   bytes   (on-disk, zstd-compressed)
Block_raw(T)  =  245,737 + 910.5 × T   bytes   (wire, uncompressed)

T = transactions per block = TPS × block_interval
```

#### Assumptions used in the tables below

* Block interval: 1 second (1 block/sec — conservative; mainnet in early 2026
  runs at 0.5 blocks/sec)
* Compression ratio: 2.39× (zstd, from v3 mixed-workload model)
* Worst-case egress subscribers: 33 (13 Block Nodes backfilling +
  10 Mirror Nodes + 10 DApps)
* Worst-case ingress: 4 parallel catch-up streams from Consensus Nodes
  (flow-control limited)

### Block Size by TPS

Derived directly from the model constants above.

|    TPS | Tx/block | On-disk / block (zstd) | Wire size / block (raw) |
|-------:|---------:|-----------------------:|------------------------:|
|  2,000 |    2,000 |                0.83 MB |                 1.58 MB |
| 10,000 |   10,000 |                3.82 MB |                 8.86 MB |
| 20,000 |   20,000 |                7.54 MB |                17.96 MB |

### Daily and Monthly On-Disk Storage (local block files, zstd)

> These figures cover raw block storage only. Allow additional headroom for Live
> State, indexes, overhead, and recent working files.

|    TPS | Per day (zstd) | Per month (zstd) |
|-------:|---------------:|-----------------:|
|  2,000 |          72 GB |           2.2 TB |
| 10,000 |         330 GB |           9.9 TB |
| 20,000 |         652 GB |          19.6 TB |

#### Planning target (20% headroom over model)

|    TPS | Per day (planned) | Per month (planned) |
|-------:|------------------:|--------------------:|
|  2,000 |             86 GB |              2.6 TB |
| 10,000 |            396 GB |             11.9 TB |
| 20,000 |            782 GB |             23.5 TB |

### Ingress Bandwidth (Consensus Node → Block Node)

Steady-state ingress carries one uncompressed block stream.
Worst-case reflects 4 Consensus Nodes simultaneously streaming to a single BN
(flow-control limited).

|    TPS | Steady-state ingress | Worst-case ingress (4× catch-up) |
|-------:|---------------------:|---------------------------------:|
|  2,000 |            ~1.8 MB/s |                          ~8 MB/s |
| 20,000 |           ~17.5 MB/s |                         ~70 MB/s |

> NIC sizing is driven by egress (see below), not ingress.
>
> Ingress values are all based assuming uncompressed data, with HTTP automatic
> compression a reasonable compression value of 3x smaller may be used.

### Egress Bandwidth (Block Node → Subscribers)

Each downstream subscriber (Mirror Node, Block Node, DApp) receives its own
uncompressed stream. All figures use the raw (uncompressed) wire size.

|    TPS | Per subscriber / day | Per subscriber / month | 33 subscribers / day | 33 subscribers / month |
|-------:|---------------------:|-----------------------:|---------------------:|-----------------------:|
|  2,000 |               156 GB |                 4.6 TB |               5.1 TB |                 154 TB |
| 20,000 |               1.5 TB |                  45 TB |                50 TB |                 1.5 PB |

**Worst-case peak bandwidth (burst — 4 in-flight blocks per subscriber):**

|    TPS | Steady-state egress (33 sub) | Burst egress (33 sub, 4× in-flight) |
|-------:|-----------------------------:|------------------------------------:|
|  2,000 |                     ~60 MB/s |                            ~67 MB/s |
| 20,000 |                    ~580 MB/s |                           ~648 MB/s |

> At 20K TPS with 33 active subscribers, burst egress approaches ~6 Gbps
> Block Nodes serving many live subscribers at high TPS **require** at least a
> 10 Gbps NIC and may need 25 Gbps or multiple bonded 10 Gbps links for headroom.
>
> Egress values are all based assuming uncompressed data; with HTTP automatic
> compression a reasonable compression value of 3x smaller may be used.

### Sizing Summary

| Scenario | On-disk (1 year, zstd, no headroom) | Peak ingress | Burst egress (33 sub) | NIC minimum |
|----------|------------------------------------:|-------------:|----------------------:|------------:|
| 2K TPS   |                             26.3 TB |      80 Mbps |             ~600 Mbps |     10 Gbps |
| 20K TPS  |                              237 TB |     700 Mbps |               ~6 Gbps |    10+ Gbps |

> The 100 TB bulk disk minimum (LFH) covers approximately 4 years at 2K TPS or
> approximately 5 months at 20K TPS.
> The recommended 500 TB covers approximately 4 years at 10K TPS.

---

## Additional Considerations

* **Clock speed**: Base CPU clock speed must be ≥ 2.0 GHz. Higher clock speeds
  reduce per-block processing latency, which is important for keeping up with
  mainnet block production rates.
* **PCIe generation**: PCIe 4.0 or higher is required to sustain the combined
  NVMe and network maximum throughput targets above. PCIe 3.0 configurations may
  be bandwidth-limited.
* **CPU vintage**: A 2024 or newer CPU is recommended to benefit from improved
  single-thread IPC and PCIe 4+ support.
* **OS disk**: Minimal sizing is sufficient; the OS disk sees negligible I/O
  after initial start-up. Allow at least 10 GB for OCI image storage.
