package com.lss233.minidb.networking.handler.query

import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.engine.visitor.SelectStatementVisitor
import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.*
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import miniDB.parser.ast.expression.primary.SysVarPrimary
import miniDB.parser.ast.stmt.dal.DALSetStatement
import miniDB.parser.ast.stmt.dml.DMLQueryStatement
import miniDB.parser.recognizer.SQLParserDelegate
import java.sql.SQLSyntaxErrorException

class QueryHandler(private val session: Session) : SimpleChannelInboundHandler<Query>() {
    private val REGEX_STMT_SET = Regex("set (.+) to (.+)");
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: Query?) {
        try {
            val queryStrings = msg?.queryString?.split(";")
            for(queryString in queryStrings!!) {
                // 先把查询语句转化为 MySQL 风格
                val queryStr = if(REGEX_STMT_SET.matches(queryString.lowercase())) {
                    queryString.lowercase().replace(REGEX_STMT_SET, "SET $1=$2")
                } else {
                    queryString
                }
                if(queryStr.isBlank()) {
                    continue
                }
                // 交给词法解析器
                val ast = SQLParserDelegate.parse(queryStr)
                println("  Q(${ast.javaClass.simpleName}: $queryStr")

                // 分析解析后的 SQL 语句，作出不同的反应
                when(ast) {
                    is DMLQueryStatement -> {
                        val relation: Relation? = if(queryStr == "select version()") {
                            Relation(mutableListOf(Column("version")), mutableListOf(arrayOf("1.0.0")))
                        } else {
                            val visitor = SelectStatementVisitor()
                            ast.accept(visitor)
                            visitor.relation
                        }

                        println(relation)

                        val rowDescription = RowDescription(relation?.columns?.map {
                            run {
                                val data = RowDescription.RowData()
                                data.name = it.name
                                data
                            } }?.toTypedArray() ?: emptyArray())
                        // 这是一条查询语句
                        ctx?.writeAndFlush(rowDescription)?.sync()
                        relation?.rows?.map { row -> run {
                            DataRow(row.map { col -> run {
                                DataRow.ColumnData(col.toString().encodeToByteArray())
                            } }.toTypedArray())
                        } }?.forEach { row ->
                            ctx?.writeAndFlush(row)?.sync()
                        }
                        //  查到了 0 条结果也是一种查
                        ctx?.writeAndFlush(CommandComplete("SELECT ${relation?.rows?.size ?: 0}"))?.sync()
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
            }
            ctx?.writeAndFlush(ReadyForQuery())?.sync()
        } catch (e: SQLSyntaxErrorException) {
            System.err.println(" Q(Error): ${msg?.queryString}")
            exceptionCaught(ctx, e)
        }
        // 等待下一条语句
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext?, cause: Throwable?) {
        System.err.println(" Q(Error): ${cause?.message}")
        cause?.printStackTrace()
        // 告诉客户端你发的东西有问题
        val err = ErrorResponse()
        err.message = cause?.message.toString()
        err.file = cause?.stackTrace?.get(0)?.fileName
        err.line = cause?.stackTrace?.get(0)?.lineNumber
        err.routine = cause?.stackTrace?.get(0)?.methodName
        if(cause is SQLSyntaxErrorException) {
            err.sqlStateCode = cause.sqlState ?: "50000"
            val positionRegex = "curIndex=\\d+".toRegex()
            err.position =  positionRegex.find(cause.message ?: "curIndex=0")?.value ?: "1"
        }
        ctx?.writeAndFlush(err)?.sync()
        ctx?.writeAndFlush(ReadyForQuery())?.sync()
    }
}
