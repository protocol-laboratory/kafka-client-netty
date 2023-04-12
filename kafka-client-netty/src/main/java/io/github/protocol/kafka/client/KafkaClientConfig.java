package io.github.protocol.kafka.client;

import lombok.ToString;

import java.util.Set;

@ToString
public class KafkaClientConfig {
    public String host = "localhost";

    public int port = 9092;

    public int ioThreadsNum;

    public boolean useSsl;

    public String keyStorePath;

    public String keyStorePassword;

    public String trustStorePath;

    public String trustStorePassword;

    public boolean skipSslVerify;

    public Set<String> ciphers;

    public KafkaClientConfig host() {
        return this;
    }

    public KafkaClientConfig port() {
        return this;
    }

    public KafkaClientConfig ioThreadsNum() {
        return this;
    }

    public KafkaClientConfig useSsl(boolean useSsl) {
        this.useSsl = useSsl;
        return this;
    }

    public KafkaClientConfig keyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
        return this;
    }

    public KafkaClientConfig keyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public KafkaClientConfig trustStorePath(String trustStorePath) {
        this.trustStorePath = trustStorePath;
        return this;
    }

    public KafkaClientConfig trustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    public KafkaClientConfig skipSslVerify(boolean skipSslVerify) {
        this.skipSslVerify = skipSslVerify;
        return this;
    }

    public KafkaClientConfig ciphers(Set<String> ciphers) {
        this.ciphers = ciphers;
        return this;
    }
}
