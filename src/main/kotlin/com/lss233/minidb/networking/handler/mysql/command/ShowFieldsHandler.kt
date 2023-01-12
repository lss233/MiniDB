package com.lss233.minidb.networking.handler.mysql.command

import com.lss233.minidb.networking.packets.mysql.OKPacket
import com.lss233.minidb.networking.packets.mysql.RequestShowFields
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler

/**
 * COM_FIELD_LIST
 * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_field_list.html#sect_protocol_com_field_list_response
 */
class ShowFieldsHandler(private val session: MySQLSession): SimpleChannelInboundHandler<RequestShowFields>() {
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: RequestShowFields?) {
        // TODO: permission check
        println("User request show fields on db: ${session.database} table ${msg?.table}")
        ctx?.writeAndFlush(OKPacket())?.sync()
    }
}