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
