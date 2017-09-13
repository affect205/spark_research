package ru.alexside.update;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Created by abalyshev on 13.09.17.
 */
public class UPDServer {
    int port;

    public static void main(String[] args) {
        new UPDServer().start();
    }

    public void start() {
        port = 5252;
        EventLoopGroup producer = new NioEventLoopGroup();
        EventLoopGroup consumer = new NioEventLoopGroup();

        try {

            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(producer, consumer)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new UPDServerAdapterInitializer());
            System.out.println("Server started");
            bootstrap.bind(port).sync().channel().closeFuture().sync();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.shutdownGracefully();
            consumer.shutdownGracefully();
        }
    }
}
