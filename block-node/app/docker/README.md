This folder contains file that are used to start the block node server as docker container.
All docker workflows are defined with a corresponding gradle task. No script or file in this folder should be called 'by hand'.

Gradle tasks are of group `docker`
- updateDockerEnv
- createDockerImage
- startDockerContainer
- stopDockerContainer

The Block Node Grafana dashboards are not in this folder: `copyDockerFolder` copies them from
`charts/block-node-server/dashboards/block-node/` into the staged `metrics/dashboards/block-node/`. Running
`docker compose` directly from this folder starts Grafana without them.

`createDockerImage` builds four images from the `Dockerfile` stages:
- `block-node-server` (`base-image`): core only; plugins are mounted or downloaded into `plugins/`.
- `block-node-server-solo-dev` (`solo-dev`): every `blockNodePlugins` jar in `plugins/`, plus dev tools.
- `block-node-server-full` (`full`): every plugin in `plugin-catalog/<plugin>/`, selected at start by
the comma-separated `BN_PLUGINS` env var (default `all`).
- `block-node-server-plugins` (`plugins`): only `/plugin-catalog/<plugin>/`, no block node software.
`/plugin-catalog/.manifest` lists the `version=` and `plugins=`; the labels `org.opencontainers.image.version`
and `org.hiero.block-node.plugins` carry the same values.

```dockerfile
FROM ghcr.io/hiero-ledger/hiero-block-node:<version>
ENV VERSION=<version> BN_PLUGINS=facility-app-state,facility-messaging,health,server-status
COPY --from=ghcr.io/hiero-ledger/hiero-block-node-plugins:<version> --chown=2000:2000 \
    /plugin-catalog /opt/hiero/block-node/app-<version>/plugin-catalog
```
