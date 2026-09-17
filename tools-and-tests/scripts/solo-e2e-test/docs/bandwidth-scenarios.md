# Network Bandwidth Scenarios

Long-form companion to the README's Network Chaos section. Use this when you want to know *why* each bandwidth-lag tier exists, *how to map your own environment's numbers onto it*, and *what to track across runs to catch a real regression* rather than noise.

## What these tests actually measure

Each tier caps CN→BN1's (and BN-peers→BN1's) egress bandwidth at a fixed rate for several minutes, then clears it. The question every tier answers is the same: **given a link that provides less bandwidth than BN needs to keep up with real block production, does BN (a) survive, (b) fall measurably behind, and (c) fully catch back up once the constraint lifts?** The tiers differ only in *how far* below BN's real need the capped link sits.

## The formula — map your own numbers onto this, or these onto yours

Everything here reduces to one ratio:

```
sustained-need floor (Mbit/s) = avg_bytes_per_block × 8 ÷ 1,000,000 × blocks_per_second
margin_ratio = your_link_bandwidth ÷ floor
```

`avg_bytes_per_block` and `blocks_per_second` must be **your own real, measured numbers** — from Mirror Node, from BN's own `files_recent_total_bytes_stored`/`files_recent_blocks_written` metrics, or from your own network's block explorer. Don't reuse this harness's own bytes/block figures directly unless your content profile (transaction-type mix, message sizes) genuinely matches this harness's — content mix drives block size far more than raw TPS does (confirmed directly: a 10k-TPS pure-CryptoTransfer profile produced *smaller* blocks here than a 6k-TPS profile with a CryptoTransfer/HCS mix).

Once you have your `margin_ratio`, this table tells you what to expect, based on what's actually been confirmed on this harness:

| `margin_ratio` | Confirmed tier | What to expect |
|---|---|---|
| ≥ ~1.8x | `bandwidth-lag` (base) | Real, measurable lag under sustained pressure, but BN reliably catches back up once the link improves. This is "under-provisioned but tolerable." |
| ~1.0-1.1x | `bandwidth-lag-stress` | More pronounced, more variable lag; still fully recovers, but with less headroom — a link running close to this ratio has little slack for further degradation. |
| ~0.5-0.6x | `bandwidth-lag-severe` | Structurally insufficient — BN falls behind faster than it can plausibly catch up while the constraint holds. Recovery is still checked and expected once the constraint *lifts*, but don't expect BN to keep pace while it's active. |
| below ~0.15x (approximate, extrapolated — see caveat) | *not tested this session* | Earlier investigation (finding 005) found a **permanent CN↔BN reconnect deadlock** — a qualitatively different, non-recovering failure — at rates this far below the real floor, using an older/smaller content-size baseline. Treat this as a rough danger-zone marker, not a confirmed boundary against today's numbers. |
| ≥ ~2.5-3x (untested) | — | Not yet characterized as its own tier; base tier (1.8x) is the highest margin confirmed so far. |

**Two directions to use this:**
- **"Will BN hold up on my link?"** — compute your own floor and margin ratio, find the closest row, and read off the expected behavior class. If your ratio sits below ~0.6x, expect BN to need active backfill/intervention rather than passive keep-up; if it sits at 1.8x or above, expect BN to absorb it on its own.
- **"Are these test numbers still valid?"** — if your production content profile or block rate has shifted, recompute the floor and check whether the *currently configured* tier rates still land at the ratios above. If they've drifted (e.g., blocks got bigger for unrelated reasons — a new field, a compression change, more transaction types), the tiers need re-deriving, the same way this session had to when a first-pass content assumption turned out to be off by ~48x.

## Confirmed current tiers

| Test file | Content profile | Rate | `margin_ratio` |
|---|---|---|---|
| `tests/bandwidth-lag.yaml` | NLG `CryptoTransferLoadTest` only, ~868 KB/block | 6 mbit/s | ~1.8x |
| `tests/bandwidth-lag-stress.yaml` | same | 3500 kbit/s | ~1.08x |
| `tests/bandwidth-lag-severe.yaml` | same | 1750 kbit/s | ~0.54x |
| `tests/bandwidth-lag-monitor-only.yaml` / `-late-snapshot.yaml` | Mirror Node Monitor, configurable TPS/mix (CryptoTransfer/ConsensusSubmitMessage), ~2.55 MB/block at a 5k/5k split | 18 mbit/s | ~1.8x |
| `tests/bandwidth-lag-monitor-only-late-snapshot-stress.yaml` | same Monitor content profile | 10500 kbit/s | ~1.08x |
| `tests/bandwidth-lag-monitor-only-late-snapshot-severe.yaml` | same Monitor content profile | 5250 kbit/s | ~0.54x |

> **Floor basis for the Monitor tier family**: all three Monitor rates share the same floor (~9.71 Mbit/s), derived from four real samples (866,698 / 2,574,119 / 2,782,229 / 3,991,184 bytes/block, mean ~2.55MB). A larger, more recent sample set (7 real runs, mean ~3.31MB/block) suggests the true floor may now be closer to ~12.4 Mbit/s — meaning base's actual margin may be closer to ~1.45x than the nominal ~1.8x. Not yet re-derived against the fuller dataset; flagged here rather than silently changed, since base's current rate is already confirmed via real passing CI runs and changing it would need its own re-confirmation pass.

## Running a bandwidth test

```bash
CHAOS_ENABLED=true TOPOLOGY=paired-3 task test:run TEST_FILE=tests/bandwidth-lag.yaml
```

In CI:

```bash
gh workflow run solo-e2e-test.yml --ref <branch> -f topology=paired-3 -f test-definition=bandwidth-lag
```

For the Monitor-only variants, also set traffic composition and rate via the `mirror-node-pinger-tps` (ConsensusSubmitMessage) and `mirror-node-xfer-tps` (CryptoTransfer) inputs — `0` disables either scenario independently, so you can run pure-CryptoTransfer, pure-ConsensusSubmitMessage, or any mix:

```bash
gh workflow run solo-e2e-test.yml --ref <branch> -f topology=paired-3 \
  -f test-definition=bandwidth-lag-monitor-only-late-snapshot \
  -f mirror-node-pinger-tps=5000 -f mirror-node-xfer-tps=5000
```

> **One at a time.** Never trigger a second `solo-e2e-test.yml` run while one is in flight — see the README's own concurrency note; the risk is the same as for latency tests.

## Reading the assertions

- **`all-healthy` / `all-have-blocks`** — BN survives the constraint without crashing or losing data. Process-level resilience, independent of whether it's keeping pace.
- **`blocks-diverged`** — samples block heights at 5 points spread across the chaos window and checks the **peak** spread against `min_spread`. This is deliberately not a single fixed-point sample: real spread at a fixed delay varied 1-6 blocks across otherwise-identical runs on this harness, so a single sample is not a reliable pass/fail signal on its own. The raw spread number (in blocks) is **not portable** to another environment — it depends on this environment's own block-cutting cadence and content size. What *is* portable is the margin ratio above.
- **`blocks-converged` / `blocks-increasing` / `block-rate-floor`** — confirms recovery once the constraint lifts: does BN catch up on its own, with no intervention? This is the single most production-relevant signal in the suite.
- **`avg-block-size`** — diagnostic only, sampled from the post-recovery window. Tells you what content profile the tier's rate was calibrated against — check this before assuming a tier's rate still applies to a changed content profile.

## Using this for milestone-over-milestone regression tracking

Pass/fail alone under-reports what changed. At each milestone, record and diff:

1. **The peak spread across the 5 snapshots**, for each tier. A real regression looks like the peak spread growing meaningfully at the *same* configured rate (same margin ratio) — e.g., base tier's peak was 7 out of a 1-7 historical range; if a new BN version consistently peaks higher or fails to recover at the same 1.8x margin that used to pass cleanly, that's a genuine capacity regression, not noise.
2. **Recovery shape**, not just pass/fail: how many `blocks-increasing` attempts it took to see growth, and the measured `block-rate-floor` rate. A version that technically passes but takes visibly longer to resume, or recovers at a visibly lower rate, is degrading even while "passing."
3. **`avg-block-size` itself.** If it drifts up or down at the same content configuration, the *floor* has moved — meaning the tier rates below it may need re-deriving before comparing pass/fail against a prior milestone at all. Comparing a tier's pass/fail across milestones without checking this first risks mistaking "content got bigger" for "BN got worse."
4. **Whether a tier stops recovering at all** (fails `blocks-converged`/`blocks-increasing` outright, not just a slower `block-rate-floor`) — this is the sharpest possible regression signal and should be treated as a blocker regardless of the other metrics.

## Caveats

- These tests are calibrated to **this harness's own achievable content density**, which is smaller than a real production network's at comparable TPS (confirmed: this harness's ~2.55MB/block at 10k combined Monitor TPS vs. a real reference network's ~69MB uncompressed block at the same TPS). A passing run here is evidence the *lag/recovery mechanism* works at this scale, not proof BN handles true production-scale content — recompute the floor with your own real numbers before extrapolating.
- Sustained high combined TPS (~10k) via Monitor has been observed, in a minority of runs, to coincide with an extended (20+ minute) network-wide stall: **all** BNs, including the two unthrottled peers, freeze at an identical block height with zero throughput. Root cause traced to CN's own log: the network's one-time genesis WRAPS proof construction (normally well under a minute) took 15-17 minutes under this load in every run checked — real and reproducible — but this alone was directly disproven as the stall's cause (one clean run showed the same slow construction with zero disruption to block production). The stall itself looks intermittent/probabilistic rather than a guaranteed consequence of high TPS, likely tied to CI-runner resource contention. If a bandwidth test at very high Monitor TPS shows a total stall (all BNs frozen, including unthrottled peers), check CN's own logs for this pattern before attributing it to the bandwidth cap itself — it is a separate, CN-side finding worth flagging to whoever owns consensus-node performance work.
