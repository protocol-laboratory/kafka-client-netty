package io.github.protocol.kafka.client.examples;

import io.github.protocol.kafka.client.KafkaClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.UUID;

@Slf4j
public class KafkaFindCoordinatorExample {
    public static void main(String[] args) throws Exception {
        KafkaClient kafkaClient = new KafkaClient();
        kafkaClient.start();
        String clientId = UUID.randomUUID().toString();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, clientId, 0);
        Struct struct = new Struct(FindCoordinatorRequestData.SCHEMA_0);
        FindCoordinatorRequest req = new FindCoordinatorRequest(struct, (short) 0);
        FindCoordinatorResponseData response = kafkaClient.findCoordinator(requestHeader, req);
        log.info("findCoordinatorResponseData: {}", response);
        kafkaClient.stop();
    }
}
