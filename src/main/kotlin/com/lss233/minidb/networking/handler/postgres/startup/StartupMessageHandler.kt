package com.lss233.minidb.networking.handler.postgres.startup

import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.postgres.AuthenticationOk
import com.lss233.minidb.networking.packets.postgres.ParameterStatus
import com.lss233.minidb.networking.packets.postgres.ReadyForQuery
import com.lss233.minidb.networking.packets.postgres.StartupMessage
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler

class StartupMessageHandler(private val session: Session) : SimpleChannelInboundHandler<StartupMessage>() {
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: StartupMessage?) {
//        session.state = Session.State.Authenticating
//        ctx?.writeAndFlush(AuthenticationSASL(listOf("SCRAM-SHA256")))?.sync()
        session.properties["client_encoding"] = "UTF8"
        session.properties["DataStyle"] = "ISO, YMD"
        session.properties["TimeZone"] = "Asia/Shanghai"
        session.properties["server_encoding"] = "UTF8"
        session.properties["server_version"] = "14.5"
        msg?.parameters?.let {
            for(entry in it) {
                session.properties[entry.key] = entry.value
                println("Parameter ${entry.key} -> ${entry.value}")
            }
        }
        ctx?.writeAndFlush(AuthenticationOk())?.sync()
        for (property in session.properties) {
            ctx?.writeAndFlush(ParameterStatus(property.key, property.value))?.sync()
        }
        session.state = Session.State.Query
        ctx?.writeAndFlush(ReadyForQuery())?.sync()
    }

}
