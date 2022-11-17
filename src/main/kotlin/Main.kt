import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.SQLParser
import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.memory.Engine
import com.lss233.minidb.engine.memory.Schema
import com.lss233.minidb.engine.memory.Table
import com.lss233.minidb.engine.schema.Column
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

    println("SELECT n.nspname, c.relname, c.relkind FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = ANY ('{r,v,m}'::char[])  ORDER BY n.nspname, c.relname")
    val ast = SQLParser.parse("SELECT n.nspname, c.relname, c.relkind FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = ANY ('{r,v,m}'::char[])  ORDER BY n.nspname, c.relname")
    val visitorXX = SelectStatementVisitor()

    val elapsed = measureNanoTime  {
        try {
            ast.accept(visitorXX)
        } finally {
            TraditionalTreePrinter().print(visitorXX.rootNode)
        }
        println(visitorXX.relation)
    }
    println("Time elapsed $elapsed nano seconds")

    val server = NettyServer()

    // server.start()

}
