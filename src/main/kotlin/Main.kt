import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.memory.Engine
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

    Engine.createDatabase(pgCatalog)
//        .createTable(pgDatabaseTable)
        .createTable(pgNamespaceTable)
        .createTable(pgTablespace)
        .createTable(pgSettingsTable)

//    println("pg_database")
//    println(pg_sys.tables["pg_database"])
//    println("pg_tablespace")
//    println(pg_sys.tables["pg_tablespace"])
    println("SELECT d.oid, d.datname AS databasename, d.datacl, d.datistemplate, d.datallowconn, pg_get_userbyid(d.datdba) AS databaseowner, d.datcollate, d.datctype, shobj_description(d.oid, 'pg_database') AS description, d.datconnlimit, t.spcname, d.encoding, pg_encoding_to_char(d.encoding) AS encodingname FROM pg_database d LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid WHERE 1=1")
    val ast = SQLParserDelegate.parse("SELECT d.oid, d.datname AS databasename, d.datacl, d.datistemplate, d.datallowconn, pg_get_userbyid(d.datdba) AS databaseowner, d.datcollate, d.datctype, shobj_description(d.oid, 'pg_database') AS description, d.datconnlimit, t.spcname, d.encoding, pg_encoding_to_char(d.encoding) AS encodingname FROM pg_database d LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid WHERE 1=1") as DMLSelectStatement
    val visitorXX = SelectStatementVisitor()

    val elapsed = measureNanoTime  {
        ast.accept(visitorXX)
        TraditionalTreePrinter().print(visitorXX.rootNode)
        println(visitorXX.relation)
    }
    println("Time elapsed $elapsed nano seconds")

    val astt = SQLParserDelegate.parse("SELECT oid FROM pg_database UNION SELECT oid FROM pg_namespace")

//    println(subset)
    val server = NettyServer()

    server.start()

}
