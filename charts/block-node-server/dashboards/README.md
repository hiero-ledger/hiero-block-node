# Grafana dashboards

This folder holds the only copy of the Block Node Grafana dashboards. Edit them here.

|                          Path                           |                                   Contents                                   |
|---------------------------------------------------------|------------------------------------------------------------------------------|
| `block-node/`                                           | Block Node dashboards, shared by the Helm chart and the docker-compose stack |
| `block-node/Hiero-Block-Node-Plugins/`                  | One dashboard per plugin or facility                                         |
| `kubernetes-views-pods.json`, `node-exporter-full.json` | Chart-only infrastructure dashboards                                         |

## Where they are used

- **Helm chart**: `templates/grafana-dashboard-configmap.yaml` publishes every `*.json` under this folder as its own
  ConfigMap with the `grafana_dashboard: "1"` label, so the kube-prometheus-stack Grafana sidecar loads it. A new
  dashboard file needs no template change.
- **docker-compose**: the `copyDockerFolder` Gradle task copies `block-node/` into
  `block-node/app/build/docker/metrics/dashboards/block-node/`, which Grafana provisions next to the docker-only
  dashboards (`cAdvisor.json`, `block-node-server-logs.json`).

## Writing a dashboard

The `block-node/` dashboards use the classic Grafana dashboard model (`panels`, `templating`) and work unchanged on
docker, Kubernetes, and Grafana Cloud:

- the model is classic, not the v2 schema (`elements`, `layout`): a Grafana with the v2 dashboard features off (the
  docker and chart defaults) provisions a v2 file without error but renders it empty;
- the datasource is the `DS_PROMETHEUS` variable;
- `environment` and `instance` are include-all variables with `allValue: ".*"`, and every query filters with
  `{environment=~"$environment", instance=~"$instance"}`. `.*` also matches series that do not have the label, so
  the default "All" shows everything where those labels are not set (docker, a single Kubernetes install);
- each dashboard has a fixed top-level `uid`, so links and bookmarks survive a fresh Grafana;
- table `organize` transformations index and rename by the original field names (`Value #A`, `instance`).

To change a dashboard, edit it in Grafana, export it as classic JSON (**Export > Export as JSON**, classic), keep the
`uid`, and replace the file here. Open it in the docker-compose Grafana to check that every panel renders.
