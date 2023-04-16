package io.github.protocol.kafka.client.examples;

import io.github.protocol.kafka.client.KafkaClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.UUID;

@Slf4j
public class KafkaLeaveGroupExample {
    public static void main(String[] args) throws Exception {
        KafkaClient kafkaClient = new KafkaClient();
        kafkaClient.start();
        String clientId = UUID.randomUUID().toString();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, clientId, 0);
        Struct struct = new Struct(LeaveGroupRequestData.SCHEMA_0);
        LeaveGroupRequest req = new LeaveGroupRequest(struct, (short) 0);
        LeaveGroupResponseData response = kafkaClient.leaveGroup(requestHeader, req);
        log.info("LeaveGroupResponseData: {}", response);
        kafkaClient.stop();
    }
}
