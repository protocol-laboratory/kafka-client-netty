package io.github.protocol.kafka.client.examples;

import io.github.protocol.kafka.client.KafkaClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.UUID;

@Slf4j
public class KafkaJoinGroupExample {
    public static void main(String[] args) throws Exception {
        KafkaClient kafkaClient = new KafkaClient();
        kafkaClient.start();
        String clientId = UUID.randomUUID().toString();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, clientId, 0);
        JoinGroupRequestData data = new JoinGroupRequestData();
        JoinGroupRequest req = new JoinGroupRequest(data, (short) 0);
        JoinGroupResponseData response = kafkaClient.joinGroup(requestHeader, req);
        log.info("joinGroupResponseData: {}", response);
        kafkaClient.stop();
    }
}
