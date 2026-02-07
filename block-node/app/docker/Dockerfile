# Use a base image with java on it already
FROM eclipse-temurin:25-jre-ubi10-minimal AS builder

RUN ls /opt/java/openjdk

# Add the app into the builder image
ARG BN_WORKDIR="/opt/hiero/block-node"
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

# Create a log directory
RUN mkdir -p ${BN_WORKDIR}/logs/config

# Copy the logging properties file
COPY logging.properties ${BN_WORKDIR}/logs/config/logging.properties

# Stage 2: Final stage (using the ubi-micro image)
FROM redhat/ubi10-micro:10.1

## Copy the Java runtime from the builder stage into the micro image
COPY --from=builder /opt/hiero/block-node /opt/hiero/block-node

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/opt/java/openjdk

ENV PATH=${JAVA_HOME}/bin:${PATH}

## Copy the Java runtime from the builder stage into the micro image
COPY --from=builder ${JAVA_HOME} ${JAVA_HOME}

# Expose the port that the application will run on
EXPOSE 40840/tcp

# HEALTHCHECK for liveness and readiness
HEALTHCHECK --interval=30s --timeout=10s --start-period=3s --retries=3 \
  CMD curl -f http://localhost:40840/healthz/livez || exit 1 && \
      curl -f http://localhost:40840/healthz/readyz || exit 1

CMD ["java", "--module-path", "/opt/hiero/block-node/app/lib", "--module", "org.hiero.block.node.app/org.hiero.block.node.app.BlockNodeApp"]
