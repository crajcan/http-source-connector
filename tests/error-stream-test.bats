#!/usr/bin/env bats

load './bats-helpers/bats-assert/load'
load './bats-helpers/bats-support/load'

setup() {
    cargo build -p mock-http-server
    ./target/debug/mock-http-server & disown
    MOCK_PID=$!

    CONFIG_FILE=$(mktemp)
    # use the fallible stream endpoint, which will send an error every 3rd request
    cp ./tests/error-stream-test-config.yaml $CONFIG_FILE
    UUID=$(uuidgen | awk '{print tolower($0)}')
    TOPIC=${UUID}-topic
    fluvio topic create $TOPIC

    sed -i.BAK "s/TOPIC/${TOPIC}/g" $CONFIG_FILE
    cat $CONFIG_FILE

    cargo build -p http-source
    ./target/debug/http-source --config $CONFIG_FILE & disown
    CONNECTOR_PID=$!
}

teardown() {
    echo "topic"
    echo $TOPIC 
    fluvio topic delete $TOPIC
    kill $MOCK_PID
    kill $CONNECTOR_PID
}

@test "http-connector-broken-stream-test" {
    echo "Starting consumer on topic $TOPIC"
    sleep 3

    # send get requests to mock server
    curl -s http://localhost:8080/get
    sleep 1
    curl -s http://localhost:8080/get
    sleep 1
    curl -s http://localhost:8080/get
    sleep 1

    run fluvio consume --start 0 --end 0 -d $TOPIC 
    assert_output --partial $'event:get request(s)\ndata:{ \"gets\": 1, \"posts\": 0 }'

    run fluvio consume --start 1 --end 1 -d $TOPIC 
    assert_output --partial $'event:get request(s)\ndata:{ \"gets\": 2, \"posts\": 0 }'

    sleep 2

    curl -s http://localhost:8080/get

    # Since the stream will break every 3rd request, this will fail unless the connector reconnects
    run fluvio consume --start 2 --end 2 -d $TOPIC 
    assert_output --partial $'event:get request(s)\ndata:{ \"gets\": 4, \"posts\": 0 }'
}