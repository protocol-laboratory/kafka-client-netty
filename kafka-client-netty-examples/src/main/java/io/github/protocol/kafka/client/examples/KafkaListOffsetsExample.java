package io.github.protocol.kafka.client.examples;

import io.github.protocol.kafka.client.KafkaClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.Map;
import java.util.UUID;

@Slf4j
public class KafkaListOffsetsExample {
    public static void main(String[] args) throws Exception {
        KafkaClient kafkaClient = new KafkaClient();
        kafkaClient.start();
        String clientId = UUID.randomUUID().toString();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, clientId, 0);
        Struct struct = new Struct(ListOffsetRequest.schemaVersions()[0]);
        ListOffsetRequest req = new ListOffsetRequest(struct, (short) 0);
        Map<TopicPartition, ListOffsetResponse.PartitionData> response = kafkaClient.listOffsets(requestHeader, req);
        log.info("offsets: {}", response);
        kafkaClient.stop();
    }
}
