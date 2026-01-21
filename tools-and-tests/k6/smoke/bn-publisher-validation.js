import { Client } from 'k6/net/grpc';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import { PublishBlockStreamRequest } from "../lib/grpc.js";
import { Trend } from "k6/metrics";
import { b64encode } from "k6/encoding";

// setup options
export const options = {
    scenarios: {
        default: {
            executor: 'shared-iterations',
            vus: 1,
            iterations: 1,
            maxDuration: '5s',
            gracefulStop: `5s`
        },
    },
    thresholds: { // todo make these good defaults, we need these to display tags in the result, but also to ping us if they go over
        'grpc_req_duration{name:block_node_server_status}': ['p(95)<300'],
        'grpc_req_duration{name:publish_block_stream}': ['p(95)<3000'],
        'checks': [{ threshold: 'rate == 1.0', abortOnFail: true }], // checks defined in this test must fail the test
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
    'block-node/api/block_stream_publish_service.proto');

// this trend measures the reception of a full block along with its endOfBlock message
const trend = new Trend('full_block_publish_duration', true);

// run test
export default async () => {
    client.connect(data.blockNodeUrl, {
        plaintext: true
    });
    // stream block from subscribe API
    const subscribeParams = {
        tags: {name: 'publish_block_stream'}
    }
    const timingMap = new Map();
    const stream = new PublishBlockStreamRequest(client).invoke(subscribeParams);
    let streamEndedSuccessfully = false;
    let successStatusReceived = false;
    // count number of end of blocks received
    const streamEnded = new Promise((resolve, reject) => {
        stream.on('data', (publishStreamResponse) => {
            console.log(publishStreamResponse);
            // if (subscribeStreamResponse.acknowledgement) {
            //     const receivedAt = instance.currentTestRunDuration;
            //     const header = subscribeStreamResponse.blockItems.blockItems[0].blockHeader;
            //     if (header) {
            //         // we want to put in the map only if a header is present because a block
            //         // could be sent in multiple chunks, we only care about the block received in full, starting from
            //         // the first chunk (must include header) to the endOfBlock message
            //         const bn = header.number;
            //         timingMap.set(parseInt(bn), receivedAt);
            //     }
            // } else if (subscribeStreamResponse.endOfBlock) {
            //     const receivedAt = instance.currentTestRunDuration;
            //     const bn = subscribeStreamResponse.endOfBlock.blockNumber;
            //     const startedAt = timingMap.get(parseInt(bn));
            //     trend.add(receivedAt - startedAt); // This should be accurate for more that millis (micros, nanos)
            //     timingMap.delete(parseInt(bn));
            // } else if (subscribeStreamResponse.status) {
            //     // success status is something expected, this is not something this
            //     // test should measure, as we want averages for individual blocks streamed
            //     if (subscribeStreamResponse.status !== `SUCCESS`) {
            //         console.log(`Received Non-Success Status: ${subscribeStreamResponse.status}`);
            //     } else {
            //         successStatusReceived = true;
            //     }
            // }
            // else {
            //     console.log(`Unknown Stream Response: , ${JSON.stringify(subscribeStreamResponse)}`);
            // }
        });
        stream.on('error', (err) => {
            console.log('Stream Error: ' + JSON.stringify(err));
            reject(err);
        });
        stream.on('end', () => {
            console.log('Stream Ended');
            streamEndedSuccessfully = true;
            resolve();
        });
    });
    const block = blocksToStream()[0];
const request = {
      block_items: {
        block_items: [
            {
                block_header: {
                    hapi_proto_version: {minor: 69},
                    software_version: {minor: 69},
                    block_timestamp: {seconds: 1734953412}
                },
            },
            {
                event_header: {
                    event_core: {creator_node_id: 1}
                }
            },
            {
                signed_transaction: "",
            },
            {
                transaction_result: {
                    status: "SUCCESS",
                    consensus_timestamp: {seconds: 1734953412},
                    transfer_list: {
                        accountAmounts: [
                            {
                                accountID: {accountNum: 50},
                                amount: -150
                            },
                            {
                                accountID: {accountNum: 60},
                                amount: 150
                            }
                        ]
                    },
                    token_transfer_lists: [
                        {
                            token: {tokenNum: 50},
                            transfers: [
                                {
                                    accountID: {accountNum: 60},
                                    amount: -150
                                },
                                {
                                    accountID: {accountNum: 40},
                                    amount: 150
                                }
                            ]
                        }
                    ]
                },
            },
            {
                block_footer: {
                    previous_block_root_hash: b64encode("\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000"),
                    root_hash_of_all_block_hashes_tree: b64encode("\\273\\355B\\245\\363\\373\\216@\\346Z\\257wY\\376T\\264\\002\\037p\\360\\233\\273\\200\\326\\370\\313\\006\\256\\244 [7\\211B\\3714\\301\\345\\003\\316\\t\\035a\\026o\\256\\373u"),
                    start_of_block_state_root_hash: b64encode("\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000")
                },
            },
            {
                block_proof: {
                    signed_block_proof: {
                        block_signature: b64encode("\\355\\b\\225W~\\304\\302Z\\304f\\334t>UA|\\3754=:\\235\\212\\374+\\3359\\016\\227\\0170\\271\\213>M\\317\\270\\017\\212\\237<q\\305\\341\\314Y\\306`\\323")
                    }
                }
            }
        ]
      }
    };
    console.log(`sending block: ${JSON.stringify(request)}`);
    stream.write(request);
    console.log(`sending endOfBlock for block 1`);
    const endOfBlockRequest = {
        end_of_block: {
            block_number: 0
        }
    }
    stream.write(endOfBlockRequest);
    // stream.end();
    try {
        // Wait for the stream to finish
        await streamEnded;
    } catch (err) {
        console.error(`Promise rejected with error: ${JSON.stringify(err)}`);
    } finally {
        // This k6 'check' will show up in your final summary table
        check(streamEndedSuccessfully, {
            'gRPC stream finished successfully': (s) => s === true,
        });
        check(successStatusReceived, {
            'gRPC stream received SUCCESS status': (s) => s === true,
        });
        client.close();
    }
};

function blocksToStream() {
    return [];
}

/**
 * Converts a string with octal escapes (e.g. \273\355) into a Uint8Array.
 * Handles both escaped octals and literal characters.
 */
function octalStringToUint8Array(input) {
    const bytes = [];
    // Match \XXX (1-3 digits) or any single character
    const regex = /\\([0-7]{1,3})|./g;
    let match;

    while ((match = regex.exec(input)) !== null) {
        if (match[1]) {
            // It's an escaped octal
            bytes.push(parseInt(match[1], 8));
        } else {
            // It's a literal character
            bytes.push(match[0].charCodeAt(0));
        }
    }
    return new Uint8Array(bytes);
}