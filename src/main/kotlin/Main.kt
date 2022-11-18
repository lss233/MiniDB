import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.SQLParser
import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.memory.Engine
import com.lss233.minidb.engine.memory.Schema
import com.lss233.minidb.engine.memory.Table
import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.engine.visitor.CreateTableStatementVisitor
import com.lss233.minidb.engine.visitor.SelectStatementVisitor
import com.lss233.minidb.networking.NettyServer
import hu.webarticum.treeprinter.printer.traditional.TraditionalTreePrinter
import miniDB.parser.ast.expression.Expression
import miniDB.parser.ast.expression.comparison.ComparisionEqualsExpression
import miniDB.parser.ast.expression.logical.LogicalAndExpression
import miniDB.parser.ast.expression.logical.LogicalOrExpression
import miniDB.parser.ast.fragment.tableref.OuterJoin
import miniDB.parser.ast.stmt.dml.DMLSelectStatement
import miniDB.parser.recognizer.SQLParserDelegate
import miniDB.parser.visitor.Visitor
import kotlin.system.measureNanoTime

fun main(args: Array<String>) {
    println("MiniDB!")

    Engine.createDatabase("minidb")

    val sqlStr = "CREATE TABLE \"public\".\"tab\" (\n" +
            "  \"col\" varchar(255)\n" +
            ")"
    println(sqlStr)
    val ast = SQLParser.parse(sqlStr)
    val visitorXX = CreateTableStatementVisitor()

    val elapsed = measureNanoTime  {
        try {
            ast.accept(visitorXX)
            Engine["minidb"].createTable(visitorXX.relation!!)
        } finally {
            TraditionalTreePrinter().print(visitorXX.rootNode)
        }
        println(visitorXX.relation)
    }
    println("Time elapsed $elapsed nano seconds")

    val server = NettyServer()

     server.start()

}
