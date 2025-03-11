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
COPY logging.properties .

# Extract the distribution and block data
RUN tar -xf simulator.tar && \
    rm simulator.tar && \
    cd /app && \
    chown -R $UNAME:$UNAME /app

# Switch to non-root user
USER $UNAME

# Run the simulator using the extracted directory name
RUN SIMULATOR_DIR=$(ls -d simulator-*/) && \
    echo "#!/bin/bash\n/app/${SIMULATOR_DIR}bin/simulator" > /app/start.sh && \
    chmod +x /app/start.sh

ENTRYPOINT ["/app/start.sh"]
