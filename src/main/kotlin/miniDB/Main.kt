package miniDB

import miniDB.parser.ast.stmt.ddl.DDLCreateTableStatement
import miniDB.parser.recognizer.SQLParserDelegate
import java.sql.SQLSyntaxErrorException

@Throws(SQLSyntaxErrorException::class)
fun main(args: Array<String>) {
    println("Hello World!")

    val sql =
        "CREATE TABLE `Test` ( Id_P int,LastName varchar(255),FirstName varchar(255),Address varchar(255),City varchar(255))"
    // 通过解析SQL后得到建表的实体类
    val ast: DDLCreateTableStatement = SQLParserDelegate.parse(sql) as DDLCreateTableStatement
    println("解析：创建表:" + ast.table.idText)

    // Try adding program arguments via Run/Debug configuration.
    // Learn more about running applications: https://www.jetbrains.com/help/idea/running-applications.html.
    println("Program arguments: ${args.joinToString()}")
}