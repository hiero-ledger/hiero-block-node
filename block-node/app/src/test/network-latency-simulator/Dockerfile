# Use an official Ubuntu base image
FROM ubuntu:latest

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV GRPC_SERVER="host.docker.internal:8080"
ENV GRPC_METHOD="BlockStreamGrpc/StreamSource"
ENV PATH_TO_PROTO="/usr/local/protos/blockstream.proto"
ENV PROTO_IMPORT_PATH="/usr/local/protos"
ENV INITIAL_LATENCY=500
ENV JITTER=500
ENV BANDWIDTH=64
ENV INCREASE_TIME=10
ENV MAX_LATENCY=12000

# Install required packages
RUN apt-get update && \
    apt-get install -y iproute2 iputils-ping curl net-tools iperf3 iptables kmod && \
    curl -L https://github.com/fullstorydev/grpcurl/releases/download/v1.8.7/grpcurl_1.8.7_linux_x86_64.tar.gz -o grpcurl.tar.gz && \
    tar -xvf grpcurl.tar.gz && mv grpcurl /usr/local/bin/grpcurl && rm grpcurl.tar.gz

# Copy scripts and protos folder into the container
COPY configure_latency.sh start.sh test-context/consumer.sh /usr/local/bin/
COPY test-context/protos /usr/local/protos

# Default command to run when starting the container
CMD ["bash", "-c", "start.sh"]
