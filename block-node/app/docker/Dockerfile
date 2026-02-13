########################################################################################################################
#
# Define Global Build Arguments
#
########################################################################################################################
ARG SOURCE_DATE_EPOCH="0"

########################################################################################################################
#
# Setup Builder Image
#
########################################################################################################################
FROM eclipse-temurin:25.0.2_10-jre-ubi10-minimal AS base-image
# Define Build Arguments
ARG SOURCE_DATE_EPOCH
ARG UID=2000
ARG GID=2000
ARG BN_WORKDIR="/opt/hiero/block-node"

# Define Standard Environment Variables
ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

RUN groupadd --gid ${GID} hedera && \
    useradd --no-user-group --create-home --uid ${UID} --gid ${GID} --shell /bin/bash hedera
# Configure SUDO support
RUN echo >> /etc/sudoers && \
    echo "%hedera ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Define version
ARG VERSION

# Set the working directory inside the container
WORKDIR ${BN_WORKDIR}

# Copy pre-extracted distribution directory (extracted in Gradle because
# UBI-minimal's tar fails under QEMU emulation on linux/arm64).
# The `distributions` build context is set on the command line (see docker-build.sh).
COPY --from=distributions app-${VERSION}/ app-${VERSION}/

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
ENV BN_WORKDIR=/opt/hiero/block-node

# Expose the port that the application will run on
EXPOSE 40840/tcp

USER hedera
WORKDIR ${BN_WORKDIR}

# HEALTHCHECK for liveness and readiness
HEALTHCHECK --interval=30s --timeout=10s --start-period=3s --retries=3 \
  CMD curl -f http://localhost:40840/healthz/livez || exit 1 && \
      curl -f http://localhost:40840/healthz/readyz || exit 1

# RUN the bin script for starting the server
ENTRYPOINT ["/bin/bash", "-c", "${BN_WORKDIR}/app-${VERSION}/bin/app || sleep 30d"]
