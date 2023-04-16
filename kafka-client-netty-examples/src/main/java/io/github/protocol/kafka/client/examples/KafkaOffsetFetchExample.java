package io.github.protocol.kafka.client.examples;

import io.github.protocol.kafka.client.KafkaClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.UUID;

@Slf4j
public class KafkaOffsetFetchExample {
    public static void main(String[] args) throws Exception {
        KafkaClient kafkaClient = new KafkaClient();
        kafkaClient.start();
        String clientId = UUID.randomUUID().toString();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, clientId, 0);
        Struct struct = new Struct(OffsetFetchRequestData.SCHEMA_0);
        OffsetFetchRequest req = new OffsetFetchRequest(struct, (short) 0);
        OffsetFetchResponseData response = kafkaClient.offsetFetch(requestHeader, req);
        log.info("offsetFetchResponseData: {}", response);
        kafkaClient.stop();
    }
}
