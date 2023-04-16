package io.github.protocol.kafka.client.examples;

import io.github.protocol.kafka.client.KafkaClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.UUID;

@Slf4j
public class KafkaOffsetForLeaderEpochExample {
    public static void main(String[] args) throws Exception {
        KafkaClient kafkaClient = new KafkaClient();
        kafkaClient.start();
        String clientId = UUID.randomUUID().toString();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, clientId, 0);
        Struct struct = new Struct(OffsetsForLeaderEpochRequest.schemaVersions()[0]);
        OffsetsForLeaderEpochRequest req = new OffsetsForLeaderEpochRequest(struct, (short) 0);
        OffsetsForLeaderEpochResponse response = kafkaClient.offsetForLeaderEpoch(requestHeader, req);
        log.info("offsetsForLeaderEpochResponse: {}", response);
        kafkaClient.stop();
    }
}
