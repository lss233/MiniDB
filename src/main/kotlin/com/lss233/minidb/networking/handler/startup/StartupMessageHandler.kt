package com.lss233.minidb.networking.handler.startup

import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.*
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import java.util.*

class StartupMessageHandler(private val session: Session) : SimpleChannelInboundHandler<StartupMessage>() {
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: StartupMessage?) {
//        ctx.writeAnd
        session.state = Session.State.Authenticating
//        ctx?.writeAndFlush(AuthenticationSASL(listOf("SCRAM-SHA256")))?.sync()
        ctx?.writeAndFlush(AuthenticationOk())?.sync()
        ctx?.writeAndFlush(ParameterStatus("client_encoding", "UTF8"))?.sync()
        ctx?.writeAndFlush(ParameterStatus("DataStyle", "ISO, YMD"))?.sync()
        ctx?.writeAndFlush(ParameterStatus("TimeZone", "Asia/Shanghai"))?.sync()
        ctx?.writeAndFlush(ParameterStatus("server_encoding", "UTF8"))?.sync()
        ctx?.writeAndFlush(ParameterStatus("server_version", "14.5"))?.sync()
        ctx?.writeAndFlush(ReadyForQuery())?.sync()
    }

}
