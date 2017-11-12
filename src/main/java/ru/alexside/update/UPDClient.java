package ru.alexside.update;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created by abalyshev on 13.09.17.
 */
public class UPDClient {
    String server;
    int port;
    int containerPort;

    public UPDClient(String server, int port, int containerPort) {
        this.server = server;
        this.port = port;
        this.containerPort = containerPort;
    }

    public static void main(String[] args) {
        String server = "localhost";
        int port = 5252;
        //int port = 5252;
        int containerPort = 8094;
        //int containerPort = 8094;
        new UPDClient(server, port, containerPort).start();
    }

    public void start() {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap().group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new UPDClientAdapterInitializer());

            Channel channel = bootstrap.connect(server, port).sync().channel();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
                while (true) {
                    String input = br.readLine();
                    channel.writeAndFlush(input);
                    if ("quit".equals(input)) {
                        System.out.println("Exit app...");
                        System.exit(0);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }
    }
}
