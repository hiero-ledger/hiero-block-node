# Release Process

This document is the authoritative guide for release managers. It describes what each
`release-*.yaml` workflow does, when to run it, and the end-to-end steps for cutting an RC,
promoting it to GA, or issuing a patch.

---

## Workflow overview

### `release-automation.yaml`

**Trigger:** `workflow_dispatch` only — must be run manually.

**What it does:**

| Step | Effect |
|------|--------|
| Compute version | Derives the next semver from `version.txt` on the dispatching branch plus the `release_type` input (`rc`, `GA`, `custom`). |
| Create / switch release branch | Creates `release/X.Y` from `main` if it doesn't exist, or checks it out. |
| Bump version | Runs `./gradlew versionAsSpecified` to write the new version everywhere. |
| Close milestone | On GA only: closes the GitHub milestone matching the release version. |
| Commit + tag | GPG-signs the bump commit and pushes the `vX.Y.Z` tag. The tag push triggers `release-push-image.yaml` in parallel. |
| Build protobuf artifact | Runs `:protobuf-sources:generateBlockNodeProtoArtifact` on the already-checked-out repo. |
| Build block-stream-tools artifact | Runs `:tools:shadowJar` and renames the fat-JAR to `block-stream-tools-X.Y.Z.jar`. |
| Upload artifacts | Both artifacts are uploaded as workflow artifacts within this run so the `create_release` job can retrieve them without a separate job waiting on a race. |
| Generate release notes | Calls the reusable `release-notes-generator.yaml` (see below). |
| Create draft release | Creates (or updates) a **draft** GitHub release with the generated notes and both artifacts attached. The release stays draft until the release manager manually publishes it. |
| Bump main snapshot (first cut only) | When `release/X.Y` is created for the first time, opens a PR on `main` to advance `version.txt` to `X.(Y+1).0-SNAPSHOT`. |

**Result:** A GPG-signed tag, a draft GitHub release with release notes + artifacts, and (on first
cut) an open PR bumping `main`.

**When to use:**
- Cutting the first RC for a new minor (`rc` from `main` or `release/X.Y`).
- Cutting subsequent RCs (`rc` from `release/X.Y`).
- Promoting to GA (`GA` from `release/X.Y`).
- Issuing a patch or hotfix (`custom` from `release/X.Y`, supplying the exact version string).

---

### `release-notes-generator.yaml`

**Trigger:** Called automatically by `release-automation.yaml` (`workflow_call`), or manually
via `workflow_dispatch`.

**What it does:**

Checks out the release branch, resolves the latest tag, installs git-cliff, generates the
changelog, and uploads it as a workflow artifact named `release-notes-<tag>`.

| Release type | Range |
|---|---|
| RC (`is_prerelease: true`) | Incremental — commits since the previous tag on the branch (`--latest`). |
| GA (`is_prerelease: false`) | Full cycle — all commits from the previous stable GA tag to this one. |

The output contains a placeholder header prompting the release manager to add a 2–4 sentence
narrative summary before publishing.

**Result:** Workflow artifact `release-notes-<tag>` containing `release_notes.md`.

**When to use manually:** To regenerate notes after editing `cliff.toml`, or to preview notes
before triggering the full release automation.

---

### `release-push-image.yaml`

**Trigger:** Automatically on `v*` tag push and `main` branch push; also `workflow_dispatch`.

**What it does:**

| Job | Output |
|-----|--------|
| `publish-app` | Builds the block-node server and solo-dev Docker images; pushes to GHCR (`ghcr.io/hiero-ledger/hiero-block-node:<version>` and `-solo-dev:<version>`). |
| `publish-jars` | Publishes all project JARs to Maven Central (release) or Maven Central Snapshots (SNAPSHOT). Requires GPG signing. |
| `publish-simulator` | Builds and pushes the simulator Docker image to GHCR. |
| `helm-chart-release-app` | Packages and pushes the `block-node-server` Helm chart to the OCI registry (`ghcr.io/hiero-ledger/hiero-block-node`). |
| `helm-chart-release-simulator` | Packages and pushes the `blockstream-simulator` Helm chart. |

**Mutable `main` tag:** When triggered by a push to `main`, the images are tagged with the
`main` branch name (not a version), providing a rolling latest-snapshot image for integration
environments.

**Result:** Docker images, JARs, and Helm charts published. **This workflow does NOT create or
update the GitHub release** — that is `release-automation.yaml`'s job. The workflows run
concurrently after the tag push and are independent.

**When to use manually:** To re-publish images or charts after a failed run, or to publish a
specific version that was tagged outside the normal automation flow.

---

## End-to-end release steps

### Before you start (all release types)

1. Verify every PR and issue in the milestone is closed or moved to the next milestone.
2. Confirm there are no outstanding cherry-picks needed on `release/X.Y`.
3. Ensure the milestone name matches the semver being released (e.g., `0.39.0` for the GA).

---

### Cutting a release candidate

1. Navigate to **Actions → Release Automation** → **Run workflow**.
2. Select the branch:
   - First RC for a new minor: select `main`.
   - Subsequent RCs: select `release/X.Y`.
3. Set `release_type` to `rc`. Leave `custom_version` empty.
4. Click **Run workflow** and wait for the `release` job to complete (~10–15 min).
5. The workflow creates the tag, the draft release, and both artifacts (protobuf + block-stream-tools).
   `release-push-image.yaml` fires simultaneously and publishes Docker images and JARs.
6. If this was the **first RC**, review and merge the auto-opened PR on `main` that bumps
   `version.txt` to `X.(Y+1).0-SNAPSHOT`.
7. Navigate to the draft release on GitHub. Add a 2–4 sentence narrative above the changelog
   (replace the placeholder line).
8. Share the draft with the team for integration and performance testing.
9. **Do not publish yet** — keep the release as draft until GA.

---

### Promoting to GA

1. Confirm all integration and performance tests pass on the latest RC image.
2. Cherry-pick any remaining fixes from `main` to `release/X.Y` if needed.
3. Navigate to **Actions → Release Automation** → **Run workflow**.
4. Select branch `release/X.Y`.
5. Set `release_type` to `GA`. Leave `custom_version` empty.
6. Click **Run workflow** and wait for the `release` job (~10–15 min).
   The workflow:
   - Strips the `-rcN` suffix from the version (e.g., `0.39.0-rc3` → `0.39.0`).
   - Closes the `0.39.0` milestone.
   - Creates the GA tag and a new draft release with full-cycle release notes.
   - Attaches protobuf and block-stream-tools artifacts.
7. `release-push-image.yaml` fires simultaneously and publishes the GA Docker images to GHCR
   and JARs to Maven Central.
8. Navigate to the draft release. Review:
   - The full-cycle changelog (covers all commits since the previous GA).
   - Attached artifacts: protobuf `.tgz`, `block-stream-tools` fat-JAR, Helm charts (added by
     `release-push-image.yaml`).
9. Add or refine the narrative summary at the top of the release notes.
10. Click **Publish release** to make the release public and mark it as the latest release.

---

### Issuing a patch / hotfix

1. Cherry-pick the fix commit(s) from `main` to `release/X.Y`.
2. Navigate to **Actions → Release Automation** → **Run workflow**.
3. Select branch `release/X.Y`.
4. Set `release_type` to `custom` and enter the exact version string (e.g., `0.39.1`).
5. Follow steps 6–10 from the GA flow above (no milestone closure on a patch unless you have one).

---

## Artifact reference

| Artifact | Built by | Attached to release by |
|----------|----------|------------------------|
| `block-node-protobuf-X.Y.Z.tgz` | `release-automation.yaml` (`release` job) | `release-automation.yaml` (`create_release` job) |
| `block-stream-tools-X.Y.Z.jar` | `release-automation.yaml` (`release` job) | `release-automation.yaml` (`create_release` job) |
| Docker images (server, solo-dev, simulator) | `release-push-image.yaml` | GHCR (not attached to the GitHub release) |
| `block-node-server` Helm chart | `release-push-image.yaml` | OCI registry on GHCR (not attached to the GitHub release) |
| JARs on Maven Central | `release-push-image.yaml` | Maven Central / Snapshots |

---

## Troubleshooting

**Draft release has no artifacts attached.**
The `create_release` job runs with `continue-on-error: true` so a failed artifact download
does not block the release creation. Check the `release` job logs to confirm both artifact
upload steps succeeded, then re-run `create_release` individually.

**`release-push-image.yaml` failed but the tag is already pushed.**
Re-run the failed jobs directly from the Actions UI. The Docker and JAR publish jobs are
idempotent; re-running them is safe.

**Release notes are missing or incorrect.**
Trigger `release-notes-generator.yaml` manually with the correct `release_branch` and
`is_prerelease` inputs. Download the artifact and paste the content into the draft release body
on GitHub.

**Milestone close failed on GA run.**
Close the milestone manually from the GitHub Issues → Milestones page. The rest of the release
is unaffected.
