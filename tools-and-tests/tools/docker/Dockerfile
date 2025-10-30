# Use Eclipse Temurin with Java 25 as the base image
FROM eclipse-temurin:25

# Define version
ARG VERSION
ENV VERSION=${VERSION}

# Set the working directory inside the container
WORKDIR /app

# Copy Distribution TAR file
COPY --from=distributions tools-${VERSION}.tar .

# Extract the TAR file
RUN tar -xvf tools-${VERSION}.tar

# Make a stable symlink so we don’t need ${VERSION} at runtime
RUN ln -s /app/tools-${VERSION} /app/tools

# Expose the port that the application will run on
EXPOSE 8090

# Exec form: argv[0] is the binary; CMD will be appended
ENTRYPOINT ["/app/tools/bin/tools"]
# Default args; compose can override if needed
CMD ["networkCapacity","-m","server","-c","/app/conf/serverDefaultConfig.json","-p","8090"]
