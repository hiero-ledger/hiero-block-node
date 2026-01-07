########################################################################################################################
#
# Define Global Build Arguments
#
########################################################################################################################
ARG UBUNTU_TAG="mantic-20240216"
ARG SOURCE_DATE_EPOCH="0"

########################################################################################################################
#
# Setup Builder Image
#
########################################################################################################################
FROM ubuntu:${UBUNTU_TAG} AS ubuntu-origin
# Define Build Arguments
ARG SOURCE_DATE_EPOCH
ARG UID=2000
ARG GID=2000
ARG BN_WORKDIR="/opt/hiero/block-node"

# Define Standard Environment Variables
ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Copy the repro sources list shell script file, don't use a bind mount because that
# is sensitive to local (real) user umask and file permissions.
COPY --chmod=755 repro-sources-list.sh /usr/local/bin/repro-sources-list.sh
# The script will be removed after it is run.
# Install basic OS utilities for building
RUN repro-sources-list.sh && \
    rm -rf /usr/local/bin/repro-sources-list.sh && \
    apt-get update && \
    apt-get install --yes --no-install-recommends tar gzip curl ca-certificates sudo && \
    apt-get autoclean --yes && \
    apt-get clean all --yes && \
    rm -rf /var/log/ && \
    rm -rf /var/cache/

##########################
####    Java Setup    ####
##########################

# Find versions here: https://github.com/adoptium/temurin25-binaries/releases
# ESUMs found in the .sha256.txt file for each version
RUN set -eux; \
        ARCH="$(dpkg --print-architecture)"; \
        case "${ARCH}" in \
           aarch64|arm64) \
            ESUM='a9d73e711d967dc44896d4f430f73a68fd33590dabc29a7f2fb9f593425b854c'; \
            BINARY_URL='https://github.com/adoptium/temurin25-binaries/releases/download/jdk-25.0.2%2B10/OpenJDK25U-jdk_aarch64_linux_hotspot_25.0.2_10.tar.gz' \
            ;; \
          amd64|i386:x86-64) \
            ESUM='987387933b64b9833846dee373b640440d3e1fd48a04804ec01a6dbf718e8ab8'; \
            BINARY_URL='https://github.com/adoptium/temurin25-binaries/releases/download/jdk-25.0.2%2B10/OpenJDK25U-jdk_x64_linux_hotspot_25.0.2_10.tar.gz' \
            ;; \
           *) \
            echo "Unsupported arch: ${ARCH}"; \
            exit 1; \
            ;; \
        esac; \
    curl -LfsSo /tmp/openjdk.tar.gz ${BINARY_URL}; \
    echo "${ESUM} */tmp/openjdk.tar.gz" | sha256sum -c -; \
    mkdir -p /usr/local/java; \
    tar --extract \
        --file /tmp/openjdk.tar.gz \
        --directory "/usr/local/java" \
        --strip-components 1 \
        --no-same-owner \
        ; \
    rm -f /tmp/openjdk.tar.gz /usr/local/java/lib/src.zip;


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

# Create a log directory
RUN mkdir -p ${BN_WORKDIR}/logs/config

# Copy the logging properties file
COPY logging.properties ${BN_WORKDIR}/logs/config/logging.properties

WORKDIR /

# Ensure proper file permissions
RUN chown -R $UID:$GID ${BN_WORKDIR}

########################################
####    Deterministic Build Hack    ####
########################################

# === Workarounds below will not be needed when https://github.com/moby/buildkit/pull/4057 is merged ===
# NOTE: PR #4057 has been merged but will not be available until the v0.13.x series of releases.
# Limit the timestamp upper bound to SOURCE_DATE_EPOCH.
# Workaround for https://github.com/moby/buildkit/issues/3180
RUN find $( ls / | grep -E -v "^(dev|mnt|proc|sys)$" ) \
  -newermt "@${SOURCE_DATE_EPOCH}" -writable -xdev \
  | xargs touch --date="@${SOURCE_DATE_EPOCH}" --no-dereference

##########################

FROM scratch AS java-builder
COPY --from=ubuntu-origin / /

# Necessary to prevent circular dependency in java-builder error
FROM java-builder AS production-image


########################################################################################################################
#
# Build Final Image
#
########################################################################################################################

# Define Build Arguments
ARG SOURCE_DATE_EPOCH

# Define Standard Environment Variables
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

ENV JAVA_VERSION="jdk-25.0.2+10"
ENV JAVA_HOME=/usr/local/java
ENV PATH=${JAVA_HOME}/bin:${PATH}
ENV BN_WORKDIR=/opt/hiero/block-node

# Install Java
COPY --from=java-builder ${JAVA_HOME}/ ${JAVA_HOME}/
# COPY bash script to container
COPY download-and-load-plugins.sh ${BN_WORKDIR}/download-and-load-plugins.sh
RUN chmod +x ${BN_WORKDIR}/download-and-load-plugins.sh

# Expose the port that the application will run on
EXPOSE 40840/tcp

USER hedera
WORKDIR ${BN_WORKDIR}

# HEALTHCHECK for liveness and readiness
HEALTHCHECK --interval=30s --timeout=10s --start-period=3s --retries=3 \
  CMD curl -f http://localhost:40840/healthz/livez || exit 1 && \
      curl -f http://localhost:40840/healthz/readyz || exit 1

# RUN the bin script for starting the server
ENTRYPOINT ["/bin/bash", "-c", "${BN_WORKDIR}/download-and-load-plugins.sh || sleep 30d"]
