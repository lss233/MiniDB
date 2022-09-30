package com.lss233.minidb.networking.handler.query

import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.EmptyQueryResponse
import com.lss233.minidb.networking.packets.ErrorResponse
import com.lss233.minidb.networking.packets.Query
import com.lss233.minidb.networking.packets.ReadyForQuery
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import miniDB.parser.ast.stmt.ddl.DDLStatement
import miniDB.parser.recognizer.SQLParserDelegate
import java.sql.SQLSyntaxErrorException

class QueryHandler(private val session: Session) : SimpleChannelInboundHandler<Query>() {
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: Query?) {
        try {
            val ast = SQLParserDelegate.parse(msg?.queryString)
            println("  Q(${ast.javaClass.name}: ${msg?.queryString}")
            ctx?.writeAndFlush(EmptyQueryResponse())?.sync()

        } catch (e: SQLSyntaxErrorException) {
            println(" Q: ${msg?.queryString}")
            e.printStackTrace()
            val err = ErrorResponse()
            err.message = e.message!!
            ctx?.writeAndFlush(err)?.sync()
        }
        ctx?.writeAndFlush(ReadyForQuery())?.sync()


    }

}
