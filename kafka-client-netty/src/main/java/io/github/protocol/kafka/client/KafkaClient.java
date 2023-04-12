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
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.ByteBuffer;
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

    public void stop() {
        if (group != null) {
            group.shutdownGracefully();
        }
    }
}
