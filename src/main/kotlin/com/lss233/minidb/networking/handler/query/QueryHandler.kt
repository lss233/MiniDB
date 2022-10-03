package com.lss233.minidb.networking.handler.query

import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.*
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import miniDB.parser.ast.expression.primary.SysVarPrimary
import miniDB.parser.ast.stmt.dal.DALSetStatement
import miniDB.parser.ast.stmt.dml.DMLSelectStatement
import miniDB.parser.recognizer.SQLParserDelegate
import java.sql.SQLSyntaxErrorException

class QueryHandler(private val session: Session) : SimpleChannelInboundHandler<Query>() {
    private val REGEX_STMT_SET = Regex("set (.+) to (.+)");
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: Query?) {
        try {

            var queryString = msg?.queryString

            // 先把查询语句转化为 MySQL 风格
            if(REGEX_STMT_SET.matches(queryString!!)) {
                queryString = queryString.replace(REGEX_STMT_SET, "SET $1=$2")
            }

            // 交给词法解析器
            val ast = SQLParserDelegate.parse(queryString)
            println("  Q(${ast.javaClass.simpleName}: $queryString")

            // 分析解析后的 SQL 语句，作出不同的反应
            when(ast) {
                is DMLSelectStatement -> {
                    // 这是一条查询语句
                    ctx?.writeAndFlush(RowDescription())?.sync()
                    //  查到了 0 条结果也是一种查
                    ctx?.writeAndFlush(CommandComplete("SELECT 0"))?.sync()
                }
                is DALSetStatement -> {
                    // 这是一条设置语句
                    for (pair in ast.assignmentList) {
                        // 更新设置
                        session.properties[(pair.key as SysVarPrimary).varText] =
                            pair.value.evaluation(emptyMap()).toString()

                        // 告知客户端设置成功
                        ctx?.writeAndFlush(CommandComplete("SET"))?.sync()
                        ctx?.writeAndFlush(ParameterStatus(
                            (pair.key as SysVarPrimary).varText,
                            session.properties[(pair.key as SysVarPrimary).varText]!!
                        ))?.sync()
                    }
                }
            }

        } catch (e: SQLSyntaxErrorException) {
            System.err.println(" Q(Error): ${msg?.queryString}")
            e.printStackTrace()
            // 告诉客户端你发的东西有问题
            val err = ErrorResponse()
            err.message = e.message!!
            ctx?.writeAndFlush(err)?.sync()
        }
        // 等待下一条语句
        ctx?.writeAndFlush(ReadyForQuery())?.sync()
    }

}
