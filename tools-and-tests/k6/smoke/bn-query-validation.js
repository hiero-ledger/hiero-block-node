import { Client, StatusOK } from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import {GetBlockRequest, ServerStatusRequest} from "../lib/grpc.js";

// setup options
export const options = {
    thresholds: { // todo make these good defaults, we need these to display tags in the result, but also to ping us if they go over
        'grpc_req_duration{name:block_node_server_status}': ['p(95)<300'],
        'grpc_req_duration{name:get_block}': ['p(95)<300'],
    },
};

// load test configuration data
const data = new SharedArray('BN Test Configs', function () {
    return JSON.parse(open('./../data.json')).configs;
})[0];

// initialize gRPC client
const client = new Client();
client.load([data.protobufPath],
    'block-node/api/node_service.proto',
    'block-node/api/block_access_service.proto',
    'block-node/api/block_stream_subscribe_service.proto');

// run test
export default async () => {
    client.connect(data.blockNodeUrl, {
        plaintext: true
    });
    const serverStatusParams = {
        tags: {name: 'block_node_server_status'}
    };
    const response = new ServerStatusRequest(client).invoke(serverStatusParams);
    check(response, {
        'status is OK': (r) => r && r.status === StatusOK,
    });
    const firstAvailableBlock = response.message.firstAvailableBlock;
    const lastAvailableBlock = response.message.lastAvailableBlock;
    console.log(`First Available Block: ${firstAvailableBlock}, Latest Block: ${lastAvailableBlock}`);
    if (firstAvailableBlock === '18446744073709551615') {
        console.log(`No blocks to fetch, exiting test.`);
    } else {
        for (let i = parseInt(firstAvailableBlock); i <= parseInt(lastAvailableBlock); i++) {
            const getBlockRequestParams = {
                tags: {name: 'get_block'},
            }
            const blockResponse = new GetBlockRequest(client).invoke(i, getBlockRequestParams);
            try {
                check(blockResponse, {
                    'block fetch status is OK': (r) => r && r.status === StatusOK,
                    'fetched block number is correct': (r) => r && parseInt(r.message.block.items[0].blockHeader.number) === i,
                });
            } catch (e) {
                console.log(`error fetching block: ${i}\nresponse: ${JSON.stringify(blockResponse)}`)
            }
            // Sleep just for a tiny bit, it seems networking with k6 is not always fast enough and we do see
            // some errors when fetching blocks, sometimes no data is received at all.
            await new Promise(r => setTimeout(r, 50));
        }
        sleep(1)
    }
    client.close();
    sleep(1);
};
