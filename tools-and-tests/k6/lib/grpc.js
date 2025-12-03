import {Stream} from "k6/net/grpc";

class ServerStatusRequest {
    constructor(client) {
        this.client = client;
        this.protoDef = 'org.hiero.block.api.BlockNodeService/serverStatus'
    }

    invoke(params = {}) {
        const req = {};
        return this.client.invoke(this.protoDef, req, params);
    }
}

class GetBlockRequest {
    constructor(client) {
        this.client = client;
        this.protoDef = 'org.hiero.block.api.BlockAccessService/getBlock'
    }

    invoke(blockNumber, params = {}) {
        const req = {block_number: blockNumber};
        return this.client.invoke(this.protoDef, req, params)
    }
}

class SubscribeBlockStreamRequest {
    constructor(client) {
        this.client = client;
        this.protoDef = 'org.hiero.block.api.BlockStreamSubscribeService/subscribeBlockStream'
    }

    invoke(params = {}) {
        return new Stream(this.client, this.protoDef , params);
    }
}

export { ServerStatusRequest, GetBlockRequest, SubscribeBlockStreamRequest };

