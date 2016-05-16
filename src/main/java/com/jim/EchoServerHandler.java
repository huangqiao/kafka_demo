package com.jim;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

public class EchoServerHandler extends ChannelHandlerAdapter {
	
	// channel管道读取
	public void channelRead(ChannelHandlerContext ctx, Object obj){
		ctx.write(obj);
		ctx.flush();
	}

	// 异常捕捉
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { 
       cause.printStackTrace();
       ctx.close();
    }
}
