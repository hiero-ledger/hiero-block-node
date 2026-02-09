# Use a base image with java on it already
FROM eclipse-temurin:25.0.2_10-jre-ubi10-minimal

ARG UID=2000
ARG GID=2000
ARG BN_WORKDIR="/opt/hiero/block-node"

RUN groupadd --gid ${GID} hedera && \
    useradd --no-user-group --create-home --uid ${UID} --gid ${GID} --shell /bin/bash hedera
# Configure SUDO support
RUN echo >> /etc/sudoers && \
    echo "%hedera ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Define version
ARG VERSION

# Set the working directory inside the container
WORKDIR ${BN_WORKDIR}

# Copy Distribution TAR file
# Ignore Codacy, the `distributions` context is set on command line and
# _must be_ so set in order to work with the wonky CI setup.
COPY --from=distributions app-${VERSION}.tar .

# Extract the TAR file
RUN tar -xvf app-${VERSION}.tar

# Extract the TAR file
RUN rm -f app-${VERSION}.tar

# Extract the TAR file
RUN mv app-${VERSION} app

# Copy the logging properties file
COPY logging.properties ${BN_WORKDIR}/logs/config/logging.properties

WORKDIR /

# Ensure proper file permissions
RUN chown -R $UID:$GID ${BN_WORKDIR}

# Expose the port that the application will run on
EXPOSE 40840/tcp

USER hedera
WORKDIR ${BN_WORKDIR}

# HEALTHCHECK for liveness and readiness
HEALTHCHECK --interval=30s --timeout=10s --start-period=3s --retries=3 \
  CMD curl -f http://localhost:40840/healthz/livez || exit 1 && \
      curl -f http://localhost:40840/healthz/readyz || exit 1

CMD ["java", "--module-path", "/opt/hiero/block-node/app/lib", "--module", "org.hiero.block.node.app/org.hiero.block.node.app.BlockNodeApp"]
