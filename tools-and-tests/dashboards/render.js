#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
//
// Renders the canonical Grafana dashboards in ./src into the in-repo deployment targets
// (local docker-compose + the Helm chart) so both stay in exact sync from a single source.
//
//   node render.js          write the rendered dashboards into every target
//   node render.js --check   verify the targets match ./src (used by CI); non-zero on drift
//
// Edit dashboards ONLY under ./src. The rendered copies in the target dirs are generated.
// Per-target extras that are NOT shared (docker: block-node-server-logs, cAdvisor; chart:
// node-exporter-full, kubernetes-views-pods) are hand-maintained and intentionally not listed.

const fs = require("fs");
const path = require("path");

const dashboardsDir = __dirname;
const repoRoot = path.resolve(dashboardsDir, "../..");
const srcDir = path.join(dashboardsDir, "src");

// The dashboards shared by every in-repo Grafana target (v2 schema, single universal file each).
const SHARED = [
    "block-node-server.json",
    "performance-metrics.json",
    "Hiero-Block-Node-Plugins/app-core.json",
    "Hiero-Block-Node-Plugins/backfill.json",
    "Hiero-Block-Node-Plugins/block-access-service.json",
    "Hiero-Block-Node-Plugins/block-provider-files-historic.json",
    "Hiero-Block-Node-Plugins/block-provider-files-recent.json",
    "Hiero-Block-Node-Plugins/cloud-storage-archive.json",
    "Hiero-Block-Node-Plugins/health.json",
    "Hiero-Block-Node-Plugins/messaging-facility.json",
    "Hiero-Block-Node-Plugins/roster-bootstrap-rsa.json",
    "Hiero-Block-Node-Plugins/roster-bootstrap-tss.json",
    "Hiero-Block-Node-Plugins/server-status.json",
    "Hiero-Block-Node-Plugins/stream-publisher.json",
    "Hiero-Block-Node-Plugins/stream-subscriber.json",
    "Hiero-Block-Node-Plugins/verification.json",
];

const TARGETS = [
    path.join(repoRoot, "block-node/app/docker/metrics/dashboards"),
    path.join(repoRoot, "charts/block-node-server/dashboards"),
];

// Fleet (multi-instance) dashboards: env-profiles.json names, per variant (cloud / latitude /
// local), which source dashboard and schema to render. Each variant is rendered into one output.
// Skipped gracefully until env-profiles.json exists.
const fleetDir = path.join(dashboardsDir, "fleet");
const fleetProfilesPath = path.join(fleetDir, "env-profiles.json");
const fleetDistDir = path.join(fleetDir, "dist");

function destinationsFor(rel) {
    return TARGETS.map((target) => path.join(target, rel));
}

// Guard against SHARED drifting from the actual src/ contents (e.g. a dashboard added to src/
// but not registered here would otherwise be silently un-rendered).
function verifySharedList() {
    const found = [];
    const walk = (dir, prefix) => {
        for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
            const rel = prefix ? `${prefix}/${entry.name}` : entry.name;
            if (entry.isDirectory()) {
                walk(path.join(dir, entry.name), rel);
            } else if (entry.name.endsWith(".json")) {
                found.push(rel);
            }
        }
    };
    walk(srcDir, "");
    const listed = new Set(SHARED);
    const present = new Set(found);
    const missing = SHARED.filter((rel) => !present.has(rel));
    const unlisted = found.filter((rel) => !listed.has(rel));
    if (missing.length > 0 || unlisted.length > 0) {
        if (missing.length > 0) {
            console.error("Listed in render.js SHARED but missing from src/:");
            missing.forEach((rel) => console.error("  " + rel));
        }
        if (unlisted.length > 0) {
            console.error("Present in src/ but not listed in render.js SHARED (add them):");
            unlisted.forEach((rel) => console.error("  " + rel));
        }
        process.exit(1);
    }
}

function renderPlain() {
    for (const rel of SHARED) {
        const content = fs.readFileSync(path.join(srcDir, rel));
        for (const dest of destinationsFor(rel)) {
            fs.mkdirSync(path.dirname(dest), { recursive: true });
            fs.writeFileSync(dest, content);
        }
    }
    console.log(`Rendered ${SHARED.length} single-instance dashboards into ${TARGETS.length} targets.`);
}

function checkPlain(stale) {
    for (const rel of SHARED) {
        const content = fs.readFileSync(path.join(srcDir, rel));
        for (const dest of destinationsFor(rel)) {
            if (!fs.existsSync(dest) || !fs.readFileSync(dest).equals(content)) {
                stale.push(path.relative(repoRoot, dest));
            }
        }
    }
}

// Replace `token` inside every string value of a JSON-like structure (so serialization escapes
// any quotes the replacement introduces — substituting on serialized text would corrupt the JSON).
function substituteToken(value, token, replacement) {
    if (typeof value === "string") {
        return value.split(token).join(replacement);
    }
    if (Array.isArray(value)) {
        return value.map((item) => substituteToken(item, token, replacement));
    }
    if (value !== null && typeof value === "object") {
        const result = {};
        for (const [key, entry] of Object.entries(value)) {
            result[key] = substituteToken(entry, token, replacement);
        }
        return result;
    }
    return value;
}

// Build one variant of a fleet dashboard: keep the source's DS_PROMETHEUS datasource variable
// first, append the variant's fleet variables, then substitute the __FLEET__ selector token.
// Classic sources hold variables in templating.list; Grafana v2 sources hold them in variables[].
function buildFleetVariant(source, profile) {
    const dashboard = JSON.parse(JSON.stringify(source));
    if (profile.schema === "v2") {
        const datasourceVariable = dashboard.variables[0];
        dashboard.variables = [datasourceVariable, ...profile.variables];
    } else {
        const datasourceVariable = dashboard.templating.list[0];
        dashboard.templating.list = [datasourceVariable, ...profile.variables];
    }
    const substituted = substituteToken(dashboard, "__FLEET__", profile.fleetSelector);
    return JSON.stringify(substituted, null, 2) + "\n";
}

// The deploy target expects full-history-metrics.json regardless of whether the variant's source
// is the classic or the .v2 schema file, so strip the .v2 marker from the output filename.
function fleetOutputName(sourceFile) {
    return sourceFile.replace(/\.v2\.json$/, ".json");
}

function fleetOutputs() {
    if (!fs.existsSync(fleetProfilesPath)) {
        return [];
    }
    const profiles = JSON.parse(fs.readFileSync(fleetProfilesPath, "utf8"));
    const outputs = [];
    for (const [variant, profile] of Object.entries(profiles.variants)) {
        const source = JSON.parse(fs.readFileSync(path.join(fleetDir, profile.source), "utf8"));
        outputs.push({
            path: path.join(fleetDistDir, variant, fleetOutputName(profile.source)),
            content: buildFleetVariant(source, profile),
        });
    }
    return outputs;
}

// Every *.json currently under fleet/dist/ (the fully-generated output tree).
function fleetDistFiles() {
    if (!fs.existsSync(fleetDistDir)) {
        return [];
    }
    const found = [];
    const walk = (dir) => {
        for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
            const full = path.join(dir, entry.name);
            if (entry.isDirectory()) {
                walk(full);
            } else if (entry.name.endsWith(".json")) {
                found.push(full);
            }
        }
    };
    walk(fleetDistDir);
    return found;
}

function renderFleet() {
    const outputs = fleetOutputs();
    if (outputs.length === 0) {
        console.log("Fleet: no env-profiles under tools-and-tests/dashboards/fleet/ yet — skipping.");
        return;
    }
    for (const output of outputs) {
        fs.mkdirSync(path.dirname(output.path), { recursive: true });
        fs.writeFileSync(output.path, output.content);
    }
    // dist/ is fully generated: drop any leftover *.json a renamed source or removed variant left.
    const expected = new Set(outputs.map((output) => output.path));
    for (const file of fleetDistFiles()) {
        if (!expected.has(file)) {
            fs.rmSync(file);
        }
    }
    console.log(`Rendered fleet dashboards into ${outputs.length} variant outputs.`);
}

function checkFleet(stale, orphans) {
    const expected = new Set();
    for (const output of fleetOutputs()) {
        expected.add(output.path);
        if (!fs.existsSync(output.path) || fs.readFileSync(output.path, "utf8") !== output.content) {
            stale.push(path.relative(repoRoot, output.path));
        }
    }
    // fleet/dist/ is fully generated, so any *.json there that render.js would not produce is a
    // leftover from a renamed source or a removed variant — flag it (unlike the target dirs, which
    // intentionally hold hand-maintained extras).
    for (const file of fleetDistFiles()) {
        if (!expected.has(file)) {
            orphans.push(path.relative(repoRoot, file));
        }
    }
}

function check() {
    const stale = [];
    const orphans = [];
    checkPlain(stale);
    checkFleet(stale, orphans);
    if (stale.length > 0 || orphans.length > 0) {
        if (stale.length > 0) {
            console.error("Rendered dashboards are out of sync with their sources under tools-and-tests/dashboards/:");
            for (const file of stale) {
                console.error("  " + file);
            }
        }
        if (orphans.length > 0) {
            console.error("Leftover generated files under fleet/dist/ that no source produces (delete them):");
            for (const file of orphans) {
                console.error("  " + file);
            }
        }
        console.error("\nEdit only the sources (src/ and fleet/), then run:");
        console.error("  node tools-and-tests/dashboards/render.js");
        process.exit(1);
    }
    console.log("All rendered dashboards are in sync with their sources.");
}

const mode = process.argv[2];
if (mode === "--check") {
    verifySharedList();
    check();
} else if (mode === undefined) {
    verifySharedList();
    renderPlain();
    renderFleet();
} else {
    console.error(`Unknown argument: ${mode}\nUsage: node render.js [--check]`);
    process.exit(2);
}
