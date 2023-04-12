package io.github.protocol.kafka.client.examples;

import io.github.protocol.kafka.client.KafkaClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.UUID;

@Slf4j
public class KafkaApiVersionsExample {
    public static void main(String[] args) throws Exception {
        KafkaClient kafkaClient = new KafkaClient();
        kafkaClient.start();
        String clientId = UUID.randomUUID().toString();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, clientId, 0);
        ApiVersionsRequestData requestData = new ApiVersionsRequestData();
        requestData.setClientSoftwareName("kafka-client-netty");
        requestData.setClientSoftwareVersion("0.0.1");
        ApiVersionsRequest req = new ApiVersionsRequest(requestData, (short) 0);
        ApiVersionsResponseData apiVersions = kafkaClient.apiVersions(requestHeader, req);
        log.info("apiVersions: {}", apiVersions);
        kafkaClient.stop();
    }
}
