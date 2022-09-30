package com.lss233.minidb.networking.handler.query

import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.*
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import miniDB.parser.ast.stmt.dal.DALSetStatement
import miniDB.parser.ast.stmt.ddl.DDLStatement
import miniDB.parser.ast.stmt.dml.DMLStatement
import miniDB.parser.recognizer.SQLParserDelegate
import java.sql.SQLSyntaxErrorException

class QueryHandler(private val session: Session) : SimpleChannelInboundHandler<Query>() {
    private val REGEX_STMT_SET = Regex("set (.+) to (.+)");
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: Query?) {
        try {
            var queryString = msg?.queryString
            if(REGEX_STMT_SET.matches(queryString!!)) {
                queryString = queryString.replace(REGEX_STMT_SET, "SET $1=$2")
            }
            val ast = SQLParserDelegate.parse(queryString)
            println("  Q(${ast.javaClass.name}: $queryString")
            when(ast) {
                is DMLStatement -> {
                    ctx?.writeAndFlush(RowDescription())?.sync()
                    ctx?.writeAndFlush(CommandComplete("SELECT 0"))?.sync()
                }
                is DALSetStatement -> {
                    for (pair in ast.assignmentList) {
                        ctx?.writeAndFlush(CommandComplete("SET"))?.sync()
                        ctx?.writeAndFlush(ParameterStatus(
                            pair.key.evaluation(emptyMap()).toString(),
                            pair.value.evaluation(emptyMap()).toString()
                        ))?.sync()
                    }
                }
            }

        } catch (e: SQLSyntaxErrorException) {
            System.err.println(" Q(Error): ${msg?.queryString}")
            e.printStackTrace()
            val err = ErrorResponse()
            err.message = e.message!!
            ctx?.writeAndFlush(err)?.sync()
        }
        ctx?.writeAndFlush(ReadyForQuery())?.sync()


    }

}
