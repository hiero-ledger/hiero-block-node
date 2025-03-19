This folder contains file that are used to start the block node server as docker container.
All docker workflows are defined with a corresponding gradle task. No script or file in this folder should be called 'by hand'.

Gradle tasks are of group `docker`
- updateDockerEnv
- createDockerImage
- startDockerContainer
- stopDockerContainer
