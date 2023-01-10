package com.lss233.minidb.networking.handler.mysql.command

import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.SQLParser
import com.lss233.minidb.engine.StringUtils
import com.lss233.minidb.engine.memory.Engine
import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.engine.visitor.CreateTableStatementVisitor
import com.lss233.minidb.engine.visitor.InsertStatementVisitor
import com.lss233.minidb.engine.visitor.SelectStatementVisitor
import com.lss233.minidb.engine.visitor.UpdateStatementVisitor
import com.lss233.minidb.networking.packets.mysql.*
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import hu.webarticum.treeprinter.printer.traditional.TraditionalTreePrinter
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import miniDB.parser.ast.expression.primary.SysVarPrimary
import miniDB.parser.ast.stmt.dal.*
import miniDB.parser.ast.stmt.ddl.DDLCreateTableStatement
import miniDB.parser.ast.stmt.ddl.DDLDropTableStatement
import miniDB.parser.ast.stmt.dml.DMLInsertStatement
import miniDB.parser.ast.stmt.dml.DMLQueryStatement
import miniDB.parser.ast.stmt.dml.DMLReplaceStatement
import miniDB.parser.ast.stmt.dml.DMLUpdateStatement
import java.sql.SQLException
import java.sql.SQLSyntaxErrorException

class QueryHandler(val session: MySQLSession): SimpleChannelInboundHandler<RequestQuery>() {
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: RequestQuery?) {
        try {
            Engine.session.set(session)
            val queryStrings = msg?.query?.split(";")
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
                        ctx?.writeAndFlush(OKPacket())?.sync()
                    }
                    is DMLReplaceStatement -> {
                        val visitor = InsertStatementVisitor()
                        try {
                            ast.accept(visitor)
                        } finally {
                            TraditionalTreePrinter().print(visitor.rootNode)
                        }
//                        ctx?.writeAndFlush(CommandComplete("SELECT 1"))?.sync()
                    }
                    is DMLUpdateStatement -> {
                        val visitor = UpdateStatementVisitor()
                        try {
                            ast.accept(visitor)
                        } finally {
                            TraditionalTreePrinter().print(visitor.rootNode)
                        }
//                        ctx?.writeAndFlush(CommandComplete("UPDATE ${visitor.affects}"))?.sync()
                    }
                    is DDLCreateTableStatement -> {
                        val visitor = CreateTableStatementVisitor()
                        try {
                            ast.accept(visitor)
                            Engine[session.properties["database"] ?: "minidb"].createTable(visitor.relation!!, visitor.tableIdentifier!!)
                        } finally {
                            TraditionalTreePrinter().print(visitor.rootNode)
                        }
//                        ctx?.writeAndFlush(CommandComplete("SELECT 1"))?.sync()
                    }
                    is DDLDropTableStatement -> {
                        val statement = ast as DDLDropTableStatement
                        for(tableName in statement.tableNames) {
                            Engine[session.properties["database"] ?: "minidb"].dropTable(tableName)
                        }

//                        ctx?.writeAndFlush(CommandComplete("DELETE 1"))?.sync()
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
                        ctx?.writeAndFlush(relation)?.sync()
                    }
                    is ShowVariables -> {
                        // 查下环境变量
                        val relation = Relation(mutableListOf(
                            Column("Variable_name"),
                            Column("Value")
                        ), session
                            .properties
                            .filter { (key, _) -> StringUtils.like(key, ast.pattern.substring(1, ast.pattern.length - 1)) }
                            .map { (key, value) -> arrayOf<Any>(key, value) }
                            .toMutableList()
                        )
                        println(relation)
                        ctx?.writeAndFlush(relation)?.sync()
                    }
                    is ShowDatabases -> {
                        // 查数据库列表
                        val relation = Relation(mutableListOf(
                            Column("Database"),
                        ),
                            Engine.getDatabase().keys.map{ arrayOf<Any>(it)}.toMutableList()
                        )
                        println(relation)
                        ctx?.writeAndFlush(relation)?.sync()
                    }
                    is ShowTables -> {
                        // 查表
                        val relation = Relation(mutableListOf(
                            Column("Tables"),
                        ),
                            Engine[session.database!!].schemas["public"]?.views?.map{ arrayOf<Any>(it.key)}?.toMutableList() ?: mutableListOf()
                        )
                        println(relation)
                        ctx?.writeAndFlush(relation)?.sync()
                    }
                    is ShowTableStatus -> {
                        val relation = Relation(mutableListOf(
                            Column("Tables"),
                        ),
                            Engine[session.database!!].schemas["public"]?.views?.map{ arrayOf<Any>(it.key)}?.toMutableList() ?: mutableListOf()
                        )
                        println(relation)
                        ctx?.writeAndFlush(relation)?.sync()
                    }
                    is DALSetStatement -> {
                        // 这是一条设置语句
                        for (pair in ast.assignmentList) {
                            // 更新设置
                            session.properties[(pair.key as SysVarPrimary).varText] =
                                pair.value.evaluation(emptyMap()).toString()

                            // 告知客户端设置成功
//                            ctx?.writeAndFlush(
//                                ParameterStatus(
//                                    (pair.key as SysVarPrimary).varText,
//                                    session.properties[(pair.key as SysVarPrimary).varText]!!
//                                )
//                            )?.sync()
//                            ctx?.writeAndFlush(CommandComplete("SET"))?.sync()
                            ctx?.writeAndFlush(OKPacket(isEOF = false))?.sync()
                        }
                    }
                }
            }
            // 等待下一条语句
//            ctx?.writeAndFlush(ReadyForQuery())?.sync()
        } catch (e: SQLSyntaxErrorException) {
            System.err.println(" Q(Error): ${msg?.query}")
            exceptionCaught(ctx, e)
        } finally {
            Engine.session.remove()
        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext?, cause: Throwable?) {
        System.err.println(" Q(Error): ${cause?.message}")
        cause?.printStackTrace()
        // 告诉客户端你发的东西有问题
        val err = if (cause is SQLException)
            ERRPacket(
                errorCode = cause.errorCode,
                sqlState = cause.sqlState,
                sqlStateMarker = "1",
                errorMessage = cause.message.toString()
            )
        else
            ERRPacket(
                errorCode = 10000,
                sqlState = "12345",
                sqlStateMarker = "1",
                errorMessage = cause?.message.toString(),
            )
        ctx?.writeAndFlush(err)?.sync()
//        ctx?.writeAndFlush(ReadyForQuery())?.sync()
    }

}
