/** Runtime module of the server. */
module com.hedera.block.server {
    requires io.helidon.webserver;
    requires io.helidon.webserver.http2;
    requires io.helidon.webserver.grpc;
    requires com.google.protobuf;

    requires static com.github.spotbugs.annotations;
}
