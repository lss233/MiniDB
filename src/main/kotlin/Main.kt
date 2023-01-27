import com.lss233.minidb.engine.SQLParser
import com.lss233.minidb.engine.memory.Engine
import com.lss233.minidb.engine.visitor.SelectStatementVisitor
import com.lss233.minidb.networking.NettyServer
import hu.webarticum.treeprinter.printer.traditional.TraditionalTreePrinter
import kotlin.system.measureNanoTime

fun main() {
    println("MiniDB!")

    Engine.createDatabase("minidb")

    Engine.execute("CREATE TABLE \"public\".\"tab\" (\n" +
            "  \"col\" varchar(255)\n" +
            ")")
    val sqlStr = "SELECT i.indrelid AS oid, ci.relname AS indexname, ct.relname AS tablename, am.amname, con.conexclop,i.indkey, i.indclass, i.indnatts, i.indoption ,i.indexrelid FROM pg_index i LEFT JOIN pg_class ct ON ct.oid = i.indrelid LEFT JOIN pg_class ci ON ci.oid = i.indexrelid LEFT JOIN pg_namespace tns ON tns.oid = ct.relnamespace LEFT JOIN pg_namespace ins ON ins.oid = ci.relnamespace LEFT JOIN pg_tablespace ts ON ci.reltablespace = ts.oid LEFT JOIN pg_am am ON ci.relam = am.oid LEFT JOIN pg_depend dep ON dep.classid = ci.tableoid AND dep.objid = ci.oid AND dep.refobjsubid = '0' LEFT JOIN pg_constraint con ON con.tableoid = dep.refclassid AND con.oid = dep.refobjid AND con.contype = 'x' WHERE tns.nspname = 'public' AND ct.relname = 'tab' AND conname IS NOT NULL"
    println(sqlStr)
    val ast = SQLParser.parse(sqlStr)
//    val visitorXX = CreateTableStatementVisitor()
    val visitorXX = SelectStatementVisitor()

    val elapsed = measureNanoTime  {
        try {
            ast.accept(visitorXX)
//            Engine["minidb"].createTable(visitorXX.relation!!, visitorXX.tableIdentifier!!)
        } finally {
            TraditionalTreePrinter().print(visitorXX.rootNode)
        }
        println(visitorXX.relation)
    }
    println("Time elapsed $elapsed nano seconds")

    val server = NettyServer()

     server.start()

}
