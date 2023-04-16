package io.github.protocol.kafka.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class KafkaClient extends SimpleChannelInboundHandler<ByteBuf> {
    private final KafkaClientConfig config;

    private final Map<Integer, CompletableFuture<ByteBuffer>> futureMap;

    private EventLoopGroup group;

    private ChannelHandlerContext ctx;

    private final Optional<SslContext> sslContextOp;

    private final LengthFieldPrepender lengthFieldPrepender;

    public KafkaClient() {
        this(new KafkaClientConfig());
    }

    public KafkaClient(KafkaClientConfig config) {
        this.config = config;
        this.futureMap = new ConcurrentHashMap<>();
        if (config.useSsl) {
            this.sslContextOp = Optional.of(SslContextUtil.buildFromJks(config.keyStorePath, config.keyStorePassword,
                    config.trustStorePath, config.trustStorePassword, config.skipSslVerify,
                    config.ciphers));
        } else {
            this.sslContextOp = Optional.empty();
        }
        this.lengthFieldPrepender = new LengthFieldPrepender(4);
    }

    public void start() throws Exception {
        if (group != null) {
            throw new IllegalStateException("kafka client already started");
        }
        log.info("begin start kafka client, config is {}", config);
        if (config.ioThreadsNum > 0) {
            group = new NioEventLoopGroup(config.ioThreadsNum);
        } else {
            group = new NioEventLoopGroup();
        }
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.remoteAddress(config.host, config.port);
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(lengthFieldPrepender);
                        ch.pipeline().addLast("frameDecoder",
                                new LengthFieldBasedFrameDecoder(KafkaConst.DEFAULT_MAX_BYTES_IN_MESSAGE, 0, 4, 0, 4));
                        p.addLast(KafkaClient.this);
                        if (config.useSsl) {
                            if (!sslContextOp.isPresent()) {
                                throw new IllegalStateException("ssl context not present");
                            }
                            p.addLast(sslContextOp.get().newHandler(ch.alloc()));
                        }
                    }
                });
        ChannelFuture channelFuture = bootstrap.connect().sync();
        if (channelFuture.isSuccess()) {
            log.info("kafka client started");
        } else {
            log.error("kafka client start failed", channelFuture.cause());
            throw new Exception("kafka client start failed", channelFuture.cause());
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf) throws Exception {
        int correlationId = byteBuf.getInt(4);
        CompletableFuture<ByteBuffer> future = this.futureMap.remove(correlationId);
        if (future == null) {
            log.warn("correlation id {} not found", correlationId);
            return;
        }
        future.complete(byteBuf.nioBuffer(4, byteBuf.capacity() - 4));
    }

    public ProduceResponse produce(RequestHeader header, ProduceRequest req) throws Exception {
        return produceAsync(header, req).get();
    }

    public CompletableFuture<ProduceResponse> produceAsync(RequestHeader header, ProduceRequest req) {
        CompletableFuture<ProduceResponse> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(ProduceResponse.parse(byteBuffer, header.apiVersion()));
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send produce request success");
            } else {
                log.error("send produce request failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> fetch(
            RequestHeader header, FetchRequest req) throws Exception {
        return fetchAsync(header, req).get();
    }

    public CompletableFuture<Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>>> fetchAsync(
            RequestHeader header, FetchRequest req) {
        CompletableFuture<Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>>> future =
                new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(FetchResponse.parse(byteBuffer, header.apiVersion()).responseData());
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send fetch request success");
            } else {
                log.error("send fetch request failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public Map<TopicPartition, ListOffsetResponse.PartitionData> listOffsets(
            RequestHeader header, ListOffsetRequest req) throws Exception {
        return listOffsetsAsync(header, req).get();
    }

    public CompletableFuture<Map<TopicPartition, ListOffsetResponse.PartitionData>> listOffsetsAsync(
            RequestHeader header, ListOffsetRequest req) {
        CompletableFuture<Map<TopicPartition, ListOffsetResponse.PartitionData>> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(ListOffsetResponse.parse(byteBuffer, header.apiVersion()).responseData());
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send list offsets request success");
            } else {
                log.error("send list offsets failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public Collection<MetadataResponse.TopicMetadata> metadata(
            RequestHeader header, MetadataRequest req) throws Exception {
        return metadataAsync(header, req).get();
    }

    public CompletableFuture<Collection<MetadataResponse.TopicMetadata>> metadataAsync(
            RequestHeader header, MetadataRequest req) {
        CompletableFuture<Collection<MetadataResponse.TopicMetadata>> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(MetadataResponse.parse(byteBuffer, header.apiVersion()).topicMetadata());
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send metadata request success");
            } else {
                log.error("send metadata failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public OffsetCommitResponseData offsetCommit(RequestHeader header, OffsetCommitRequest req) throws Exception {
        return offsetCommitAsync(header, req).get();
    }

    public CompletableFuture<OffsetCommitResponseData> offsetCommitAsync(
            RequestHeader header, OffsetCommitRequest req) {
        CompletableFuture<OffsetCommitResponseData> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(OffsetCommitResponse.parse(byteBuffer, header.apiVersion()).data());
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send offset commit request success");
            } else {
                log.error("send offset commit failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public OffsetFetchResponseData offsetFetch(RequestHeader header, OffsetFetchRequest req) throws Exception {
        return offsetFetchAsync(header, req).get();
    }

    public CompletableFuture<OffsetFetchResponseData> offsetFetchAsync(RequestHeader header, OffsetFetchRequest req) {
        CompletableFuture<OffsetFetchResponseData> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(OffsetFetchResponse.parse(byteBuffer, header.apiVersion()).data);
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send offset fetch request success");
            } else {
                log.error("send offset fetch failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public FindCoordinatorResponseData findCoordinator(RequestHeader header, FindCoordinatorRequest req)
            throws Exception {
        return findCoordinatorAsync(header, req).get();
    }

    public CompletableFuture<FindCoordinatorResponseData> findCoordinatorAsync(RequestHeader header,
                                                                               FindCoordinatorRequest req) {
        CompletableFuture<FindCoordinatorResponseData> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(FindCoordinatorResponse.parse(byteBuffer, header.apiVersion()).data());
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send find coordinator request success");
            } else {
                log.error("send find coordinator failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public JoinGroupResponseData joinGroup(RequestHeader header, JoinGroupRequest req) throws Exception {
        return joinGroupAsync(header, req).get();
    }

    public CompletableFuture<JoinGroupResponseData> joinGroupAsync(RequestHeader header,
                                                                   JoinGroupRequest req) {
        CompletableFuture<JoinGroupResponseData> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(JoinGroupResponse.parse(byteBuffer, header.apiVersion()).data());
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send join group request success");
            } else {
                log.error("send join group request failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public HeartbeatResponse heartbeat(RequestHeader header, HeartbeatRequest req) throws Exception {
        return heartbeatAsync(header, req).get();
    }

    public CompletableFuture<HeartbeatResponse> heartbeatAsync(RequestHeader header,
                                                               HeartbeatRequest req) {
        CompletableFuture<HeartbeatResponse> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(HeartbeatResponse.parse(byteBuffer, header.apiVersion()));
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send heartbeat request success");
            } else {
                log.error("send heartbeat request failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public LeaveGroupResponseData leaveGroup(RequestHeader header, LeaveGroupRequest req) throws Exception {
        return leaveGroupAsync(header, req).get();
    }

    public CompletableFuture<LeaveGroupResponseData> leaveGroupAsync(RequestHeader header,
                                                                     LeaveGroupRequest req) {
        CompletableFuture<LeaveGroupResponseData> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(LeaveGroupResponse.parse(byteBuffer, header.apiVersion()).data);
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send leave group request success");
            } else {
                log.error("send leave group request failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public SyncGroupResponseData syncGroup(RequestHeader header, SyncGroupRequest req) throws Exception {
        return syncGroupAsync(header, req).get();
    }

    public CompletableFuture<SyncGroupResponseData> syncGroupAsync(RequestHeader header,
                                                                   SyncGroupRequest req) {
        CompletableFuture<SyncGroupResponseData> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(SyncGroupResponse.parse(byteBuffer, header.apiVersion()).data);
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send sync group request success");
            } else {
                log.error("send sync group request failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public SaslHandshakeResponse saslHandshake(RequestHeader header, SaslHandshakeRequest req) throws Exception {
        return saslHandshakeAsync(header, req).get();
    }

    public CompletableFuture<SaslHandshakeResponse> saslHandshakeAsync(RequestHeader header,
                                                                       SaslHandshakeRequest req) {
        CompletableFuture<SaslHandshakeResponse> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(SaslHandshakeResponse.parse(byteBuffer, header.apiVersion()));
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send sasl handshake request success");
            } else {
                log.error("send sasl handshake request failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public ApiVersionsResponseData apiVersions(RequestHeader header, ApiVersionsRequest req) throws Exception {
        return this.apiVersionsAsync(header, req).get();
    }

    public CompletableFuture<ApiVersionsResponseData> apiVersionsAsync(RequestHeader header, ApiVersionsRequest req) {
        CompletableFuture<ApiVersionsResponseData> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                ApiVersionsResponse response = ApiVersionsResponse.parse(byteBuffer, header.apiVersion());
                future.complete(response.data);
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send api versions request success");
            } else {
                log.error("send api versions request failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public OffsetsForLeaderEpochResponse offsetForLeaderEpoch(RequestHeader header, OffsetsForLeaderEpochRequest req)
            throws Exception {
        return offsetForLeaderEpochAsync(header, req).get();
    }

    public CompletableFuture<OffsetsForLeaderEpochResponse> offsetForLeaderEpochAsync(
            RequestHeader header, OffsetsForLeaderEpochRequest req) {
        CompletableFuture<OffsetsForLeaderEpochResponse> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(OffsetsForLeaderEpochResponse.parse(byteBuffer, header.apiVersion()));
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send offset for leader epoch request success");
            } else {
                log.error("send offset for leader epoch request failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public SaslAuthenticateResponse saslAuthenticate(RequestHeader header, SaslAuthenticateRequest req)
            throws Exception {
        return saslAuthenticateAsync(header, req).get();
    }

    public CompletableFuture<SaslAuthenticateResponse> saslAuthenticateAsync(
            RequestHeader header, SaslAuthenticateRequest req) {
        CompletableFuture<SaslAuthenticateResponse> future = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> byteBufFuture = new CompletableFuture<>();
        byteBufFuture.whenComplete((byteBuffer, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(SaslAuthenticateResponse.parse(byteBuffer, header.apiVersion()));
            }
        });
        ByteBuffer byteBuffer = req.serialize(header);
        this.futureMap.put(header.correlationId(), byteBufFuture);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(byteBuffer)).addListener(f -> {
            if (f.isSuccess()) {
                log.info("send sasl authenticate request success");
            } else {
                log.error("send sasl authenticate request failed", f.cause());
                this.futureMap.remove(header.correlationId());
                byteBufFuture.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public void stop() {
        if (group != null) {
            group.shutdownGracefully();
        }
    }
}
