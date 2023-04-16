package io.github.protocol.kafka.client.examples;

import io.github.protocol.kafka.client.KafkaClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SyncGroupRequest;

import java.util.UUID;

@Slf4j
public class KafkaSyncGroupExample {
    public static void main(String[] args) throws Exception {
        KafkaClient kafkaClient = new KafkaClient();
        kafkaClient.start();
        String clientId = UUID.randomUUID().toString();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, clientId, 0);
        SyncGroupRequestData data = new SyncGroupRequestData();
        SyncGroupRequest req = new SyncGroupRequest(data, (short) 0);
        SyncGroupResponseData response = kafkaClient.syncGroup(requestHeader, req);
        log.info("syncGroupResponseData: {}", response);
        kafkaClient.stop();
    }
}
