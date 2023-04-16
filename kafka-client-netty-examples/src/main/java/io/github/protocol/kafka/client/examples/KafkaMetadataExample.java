package io.github.protocol.kafka.client.examples;

import io.github.protocol.kafka.client.KafkaClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Slf4j
public class KafkaMetadataExample {
    public static void main(String[] args) throws Exception {
        KafkaClient kafkaClient = new KafkaClient();
        kafkaClient.start();
        String clientId = UUID.randomUUID().toString();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, clientId, 0);
        MetadataRequestData data = new MetadataRequestData();
        data.setAllowAutoTopicCreation(true);
        List<MetadataRequestData.MetadataRequestTopic> topicList = new ArrayList<>();
        Struct struct = new Struct(MetadataRequestData.MetadataRequestTopic.SCHEMA_0);
        topicList.add(new MetadataRequestData.MetadataRequestTopic(struct, (short) 0));
        data.setTopics(topicList);
        MetadataRequest req = new MetadataRequest(data, (short) 0);
        Collection<MetadataResponse.TopicMetadata> response = kafkaClient.metadata(requestHeader, req);
        log.info("metadata: {}", response);
        kafkaClient.stop();
    }
}
