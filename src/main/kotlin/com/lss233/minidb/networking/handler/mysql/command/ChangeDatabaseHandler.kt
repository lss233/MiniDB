package com.lss233.minidb.networking.handler.mysql.command

import com.lss233.minidb.networking.packets.mysql.ChangeDatabase
import com.lss233.minidb.networking.packets.mysql.OKPacket
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler

class ChangeDatabaseHandler(private val session: MySQLSession): SimpleChannelInboundHandler<ChangeDatabase>() {
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: ChangeDatabase?) {
        // TODO: permission check
        println("User database changed from ${session.database} to ${msg?.database}")
        session.database = msg?.database
        ctx?.writeAndFlush(OKPacket())?.sync()
    }
}