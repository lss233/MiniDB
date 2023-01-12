package com.lss233.minidb.networking.handler.postgres.query

import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.SQLParser
import com.lss233.minidb.engine.memory.Engine
import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.engine.visitor.CreateTableStatementVisitor
import com.lss233.minidb.engine.visitor.InsertStatementVisitor
import com.lss233.minidb.engine.visitor.SelectStatementVisitor
import com.lss233.minidb.engine.visitor.UpdateStatementVisitor
import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.postgres.*
import hu.webarticum.treeprinter.printer.traditional.TraditionalTreePrinter
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import miniDB.parser.ast.expression.primary.SysVarPrimary
import miniDB.parser.ast.stmt.dal.DALSetStatement
import miniDB.parser.ast.stmt.ddl.DDLCreateTableStatement
import miniDB.parser.ast.stmt.ddl.DDLDropTableStatement
import miniDB.parser.ast.stmt.dml.DMLInsertStatement
import miniDB.parser.ast.stmt.dml.DMLQueryStatement
import miniDB.parser.ast.stmt.dml.DMLReplaceStatement
import miniDB.parser.ast.stmt.dml.DMLUpdateStatement
import java.sql.SQLSyntaxErrorException

class QueryHandler(private val session: Session) : SimpleChannelInboundHandler<Query>() {
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: Query?) {
        try {
            Engine.session.set(session)
            val queryStrings = msg?.queryString?.split(";")
            for(queryString in queryStrings!!) {
                if(queryString.isBlank()) {
                    continue
                }
                // 交给词法解析器
                println("  Q: $queryString")
                val ast = SQLParser.parse(queryString)
                println("  Q(${ast.javaClass.simpleName}): $queryString")

                // 分析解析后的 SQL 语句，作出不同的反应
                when(ast) {
                    is DMLInsertStatement -> {
                        val visitor = InsertStatementVisitor()
                        try {
                            ast.accept(visitor)
                        } finally {
                            TraditionalTreePrinter().print(visitor.rootNode)
                        }
                        ctx?.writeAndFlush(CommandComplete("INSERT 0 ${visitor.affects}"))?.sync()
                    }
                    is DMLReplaceStatement -> {
                        val visitor = InsertStatementVisitor()
                        try {
                            ast.accept(visitor)
                        } finally {
                            TraditionalTreePrinter().print(visitor.rootNode)
                        }
                        ctx?.writeAndFlush(CommandComplete("SELECT 1"))?.sync()
                    }
                    is DMLUpdateStatement -> {
                        val visitor = UpdateStatementVisitor()
                        try {
                            ast.accept(visitor)
                        } finally {
                            TraditionalTreePrinter().print(visitor.rootNode)
                        }
                        ctx?.writeAndFlush(CommandComplete("UPDATE ${visitor.affects}"))?.sync()
                    }
                    is DDLCreateTableStatement -> {
                        val visitor = CreateTableStatementVisitor()
                        try {
                            ast.accept(visitor)
                            Engine[session.properties["database"] ?: "minidb"].createTable(visitor.relation!!, visitor.tableIdentifier!!)
                        } finally {
                            TraditionalTreePrinter().print(visitor.rootNode)
                        }
                        ctx?.writeAndFlush(CommandComplete("SELECT 1"))?.sync()
                    }
                    is DDLDropTableStatement -> {
                        val statement = ast as DDLDropTableStatement
                        for(tableName in statement.tableNames) {
                            Engine[session.properties["database"] ?: "minidb"].dropTable(tableName)
                        }

                        ctx?.writeAndFlush(CommandComplete("DELETE 1"))?.sync()
                    }
                    is DMLQueryStatement -> {
                        val relation: Relation? = if(queryString.lowercase() == "select version()") {
                            Relation(mutableListOf(Column("version")), mutableListOf(arrayOf("1.0.0")))
                        } else {
                            val visitor = SelectStatementVisitor()
                            try {
                                ast.accept(visitor)
                            } finally {
                                TraditionalTreePrinter().print(visitor.rootNode)
                            }
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
                            ctx?.writeAndFlush(
                                ParameterStatus(
                                (pair.key as SysVarPrimary).varText,
                                session.properties[(pair.key as SysVarPrimary).varText]!!
                            )
                            )?.sync()
                            ctx?.writeAndFlush(CommandComplete("SET"))?.sync()
                        }
                    }
                }
            }
            // 等待下一条语句
            ctx?.writeAndFlush(ReadyForQuery())?.sync()
        } catch (e: SQLSyntaxErrorException) {
            System.err.println(" Q(Error): ${msg?.queryString}")
            exceptionCaught(ctx, e)
        } finally {
            Engine.session.remove()
        }
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
