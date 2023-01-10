package com.lss233.minidb.networking.handler.mysql.handshake

import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.mysql.HandshakeResponse41
import com.lss233.minidb.networking.packets.mysql.OKPacket
import com.lss233.minidb.networking.protocol.mysql.CapabilitiesFlags
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler

class InitialHandshakeHandler(val session: MySQLSession): SimpleChannelInboundHandler<HandshakeResponse41>() {
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: HandshakeResponse41?) {
        println("user ${msg?.username} attempt to connect, password ${msg?.authResponse}, database ${msg?.database}")

        ctx?.writeAndFlush(OKPacket(
            0,
            0,
            0x0002,
            0,
            "Suck",
            "",
            false
        ))?.sync()
        session.clientFlags = session.clientFlags and msg?.clientFlag!!
        session.state = Session.State.Query
        session.packetSequenceId = -1
        session.database = msg.database
        println("Client connected with following capabilities:")
        for (capability in CapabilitiesFlags.getCapabilities(session.clientFlags)) {
            println("\t" + capability.name)
        }
    }

}
