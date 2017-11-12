package ru.alexside.update;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import javax.sql.rowset.serial.SerialException;

/**
 * Created by abalyshev on 13.09.17.
 */
public class UPDClientHandler extends SimpleChannelInboundHandler<String> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        System.out.printf("Friend: %s\n", msg);
        if (msg.equals("quit")) {
            throw new SerialException("Server is closed");
        }

    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
