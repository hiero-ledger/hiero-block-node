import {Client, StatusDeadlineExceeded, StatusOK} from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import {ServerStatusRequest} from "../lib/grpc.js";
import { vu } from 'k6/execution';


// Configure k6 VUs scheduling and iterations & thresholds
export const options = {
    scenarios: {
        default: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '1m', target: 5 }, // traffic ramp-up from 0 to 5 users (all CNs and a shadow MN) over a minute.
                { duration: '1m', target: 10 }, // stay at 10 users for a minute.
                { duration: '1m', target: 0 }, // ramp-down to 0 users for a minute.
            ],
        },
    },
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
    if (vu.iterationInScenario === 0) {
        // connect only once per vu's runs, we should
        client.connect(data.blockNodeUrl, {
            plaintext: true
        });
    }
    const params = {
        tags: {name: 'block_node_server_status'}
    };
    let response = new ServerStatusRequest(client).invoke(params);
    check(response, {
        'status is OK': (r) => r && r.status === StatusOK,
    });
    sleep(1);
};
