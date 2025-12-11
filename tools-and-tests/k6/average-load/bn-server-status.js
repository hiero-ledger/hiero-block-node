import { Client, StatusOK } from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import {ServerStatusRequest} from "../lib/grpc.js";

// Configure k6 VUs scheduling and iterations & thresholds
export const options = {
    stages: [
        { duration: '2m', target: 40 }, // traffic ramp-up from 1 to 40 users (all CNs and a shadow MN) over 5 minutes.
        { duration: '3m', target: 100 }, // stay at 100 users for 3 minutes
        { duration: '1m', target: 0 }, // ramp-down to 0 users
    ],
    thresholds: { // todo make these good defaults, we need these to display tags in the result, but also to ping us if they go over
        'grpc_req_duration{name:block_node_server_status}': ['p(95)<300'],
    },
};

// load test configuration data
const data = new SharedArray('BN Test Configs', function () {
    return JSON.parse(open('./../data.json')).configs;
})[0];

// initialize gRPC client
const client = new Client();
client.load([data.protobufPath], 'block-node/api/node_service.proto');

// run test
export default () => {
    client.connect(data.blockNodeUrl, {
        plaintext: true
    });
    const params = {
        tags: {name: 'block_node_server_status'}
    };
    const response = new ServerStatusRequest(client).invoke(params);
    check(response, {
        'status is OK': (r) => r && r.status === StatusOK,
    });
    client.close();
    sleep(1);
};
