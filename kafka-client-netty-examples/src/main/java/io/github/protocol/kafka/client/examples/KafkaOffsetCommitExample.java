package io.github.protocol.kafka.client.examples;

import io.github.protocol.kafka.client.KafkaClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.UUID;

@Slf4j
public class KafkaOffsetCommitExample {
    public static void main(String[] args) throws Exception {
        KafkaClient kafkaClient = new KafkaClient();
        kafkaClient.start();
        String clientId = UUID.randomUUID().toString();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, clientId, 0);
        OffsetCommitRequestData data = new OffsetCommitRequestData();
        OffsetCommitRequest req = new OffsetCommitRequest(data, (short) 0);
        OffsetCommitResponseData response = kafkaClient.offsetCommit(requestHeader, req);
        log.info("offsetCommitResponseData: {}", response);
        kafkaClient.stop();
    }
}
