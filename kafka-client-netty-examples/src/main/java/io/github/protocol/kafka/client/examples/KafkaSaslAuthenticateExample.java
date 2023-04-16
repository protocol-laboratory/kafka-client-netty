package io.github.protocol.kafka.client.examples;

import io.github.protocol.kafka.client.KafkaClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;

import java.util.UUID;

@Slf4j
public class KafkaSaslAuthenticateExample {
    public static void main(String[] args) throws Exception {
        KafkaClient kafkaClient = new KafkaClient();
        kafkaClient.start();
        String clientId = UUID.randomUUID().toString();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, clientId, 0);
        SaslAuthenticateRequestData data = new SaslAuthenticateRequestData();
        SaslAuthenticateRequest req = new SaslAuthenticateRequest(data);
        SaslAuthenticateResponse response = kafkaClient.saslAuthenticate(requestHeader, req);
        log.info("saslAuthenticateResponse: {}", response);
        kafkaClient.stop();
    }
}
