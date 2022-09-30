package com.lss233.minidb.networking.handler.startup

import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.AuthenticationOk
import com.lss233.minidb.networking.packets.ReadyForQuery
import com.lss233.minidb.networking.packets.StartupMessage
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler

class StartupMessageHandler(private val session: Session) : SimpleChannelInboundHandler<StartupMessage>() {
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: StartupMessage?) {
//        ctx.writeAnd
        session.state = Session.State.Query
        ctx?.writeAndFlush(AuthenticationOk())?.sync()
        ctx?.writeAndFlush(ReadyForQuery())?.sync()
    }

}
