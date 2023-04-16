package io.github.protocol.kafka.client.examples;

import io.github.protocol.kafka.client.KafkaClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.Map;
import java.util.UUID;

@Slf4j
public class KafkaFetchExample {
    public static void main(String[] args) throws Exception {
        KafkaClient kafkaClient = new KafkaClient();
        kafkaClient.start();
        String clientId = UUID.randomUUID().toString();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, clientId, 0);
        Struct struct = new Struct(FetchRequest.schemaVersions()[0]);
        FetchRequest req = new FetchRequest(struct, (short) 0);
        Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> response =
                kafkaClient.fetch(requestHeader, req);
        log.info("fetchResponse: {}", response);
        kafkaClient.stop();
    }
}
