package org.apache.hadoop.ozone.container.stream;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

public class StreamingClient implements AutoCloseable {

    private static EventLoopGroup group;
    private final Bootstrap b;
    private ChannelFuture f;
    private int port;
    private String host;

    public StreamingClient(
        String host,
        int port,
        StreamingDestination streamingDestination
    ) throws InterruptedException {
        this.port = port;
        this.host = host;

        group = new NioEventLoopGroup(100);

        b = new Bootstrap();
        b.group(group)
            .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_RCVBUF, 1024 * 1024)
                .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast(new StringEncoder(CharsetUtil.UTF_8),
                        new DirstreamClientHandler(streamingDestination));
                }
            });

    }

    public Channel connect() throws InterruptedException {
        f = b.connect(host, port).sync();
        return f.channel();
    }

    public void close() throws InterruptedException {
        f.channel().closeFuture().sync();
        group.shutdownGracefully();
    }

}
