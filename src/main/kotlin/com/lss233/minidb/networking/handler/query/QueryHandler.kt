package com.lss233.minidb.networking.handler.query

import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.EmptyQueryResponse
import com.lss233.minidb.networking.packets.Query
import com.lss233.minidb.networking.packets.ReadyForQuery
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler

class QueryHandler(private val session: Session) : SimpleChannelInboundHandler<Query>() {
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: Query?) {
        println("  Q: " + msg?.queryString)
        ctx?.writeAndFlush(EmptyQueryResponse())?.sync()
        ctx?.writeAndFlush(ReadyForQuery())?.sync()
    }

}
