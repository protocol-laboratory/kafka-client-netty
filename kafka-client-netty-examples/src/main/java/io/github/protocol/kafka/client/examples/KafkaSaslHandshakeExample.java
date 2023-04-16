package io.github.protocol.kafka.client.examples;

import io.github.protocol.kafka.client.KafkaClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;

import java.util.UUID;

@Slf4j
public class KafkaSaslHandshakeExample {
    public static void main(String[] args) throws Exception {
        KafkaClient kafkaClient = new KafkaClient();
        kafkaClient.start();
        String clientId = UUID.randomUUID().toString();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, clientId, 0);
        SaslHandshakeRequestData data = new SaslHandshakeRequestData();
        SaslHandshakeRequest req = new SaslHandshakeRequest(data);
        SaslHandshakeResponse response = kafkaClient.saslHandshake(requestHeader, req);
        log.info("saslHandshakeResponse: {}", response);
        kafkaClient.stop();
    }
}
