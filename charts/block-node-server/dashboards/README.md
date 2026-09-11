# Dashboards (partly generated)

`block-node-server.json`, `performance-metrics.json`, and everything under
`Hiero-Block-Node-Plugins/` are **generated** — do not edit them here. Edit the single source under
`tools-and-tests/dashboards/src/` and run `node tools-and-tests/dashboards/render.js`. CI fails if
these copies drift from the source.

`node-exporter-full.json` and `kubernetes-views-pods.json` are generic third-party, Kubernetes-only
dashboards and are maintained directly in this directory.
