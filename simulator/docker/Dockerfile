FROM eclipse-temurin:21

# Create a non-root user and group
ARG UNAME=hedera
ARG UID=2000
ARG GID=2000
RUN groupadd -g $GID -o $UNAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $UNAME

WORKDIR /app

# Copy the distribution and resources
COPY simulator-*.tar ./simulator.tar

RUN mkdir -p /app/logs/config
COPY logging.properties /app/logs/config/logging.properties

# Create resources directory and copy block data
RUN mkdir -p src/main/resources
COPY block-0.0.3.tar.gz src/main/resources/

# Extract the distribution and block data
RUN tar -xf simulator.tar && \
    rm simulator.tar && \
    cd src/main/resources && \
    tar -xzf block-0.0.3.tar.gz && \
    rm block-0.0.3.tar.gz && \
    cd /app && \
    chown -R $UID:$GID /app

# Switch to non-root user
USER $UNAME

# Run the simulator using the extracted directory name
RUN SIMULATOR_DIR=$(ls -d simulator-*/) && \
    echo "#!/bin/bash\n/app/${SIMULATOR_DIR}bin/simulator" > /app/start.sh && \
    chmod +x /app/start.sh

ENTRYPOINT ["/app/start.sh"]
