package com.lss233.minidb.networking.handler.postgres.startup

import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.postgres.SSLRequest
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import java.nio.charset.StandardCharsets

class SSLRequestRejectHandler(private val session: Session) : SimpleChannelInboundHandler<SSLRequest>() {
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: SSLRequest?) {
        ctx?.writeAndFlush(Unpooled.copiedBuffer("N", StandardCharsets.UTF_8))?.sync()
    }

}
