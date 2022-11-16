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

    val pg_sys = Database()
    pg_sys.tables["pg_database"] = Table(arrayOf(
        Column("oid"), Column("datname"), Column("datdba"), Column("encoding"),
        Column("datlocprovider"), Column("datistemplate"), Column("datallowconn"),
        Column("datconnlimit"), Column("dattablespace"), Column("datcollate"),
        Column("datctype"), Column("datacl")
    ), arrayOf(
        NTuple.from("1", "pg_catalog", "1", "1", "c", "1", "true", "-1", "1", "utf8", "", ""),
        NTuple.from("2", "test", "1", "1", "c", "1", "true", "-1", "1", "utf8", "", ""),
        NTuple.from("3", "老子根本不是 PostgresSQL", "1", "1", "c", "1", "true", "-1", "1", "utf8", "", "")
        )
    )
    pg_sys.tables["pg_namespace"] = Table(arrayOf(
        Column("oid"), Column("nspname"), Column("nspower"), Column("nspacl")
    ), arrayOf(
        NTuple.from("1", "pg_catalog", "10", "{postgres=UC/postgres,=U/postgres}"),
        NTuple.from("2", "test", "10", "{postgres=UC/postgres,=U/postgres}"),
    )
    )
    pg_sys.tables["pg_tablespace"] = Table(arrayOf(
        Column("oid"), Column("spcname"), Column("spcowner"), Column("spcacl"), Column("spcoptions")
    ), arrayOf(
        NTuple.from("1", "test_default_tspace", "1", "", "")
        )
    )
    pg_sys.tables["pg_settings"] = Table(arrayOf(
        Column("set_config('bytea_output','hex',false)"), Column("name")
    ), arrayOf(
        NTuple.from("1", "bytea_output")
    )
    )
    Engine.databases["pg_catalog"] = pg_sys
    println("pg_database")
    println(pg_sys.tables["pg_database"])
    println("pg_tablespace")
    println(pg_sys.tables["pg_tablespace"])
    println("SELECT d.oid, d.datname AS databasename, d.datacl, d.datistemplate, d.datallowconn, pg_get_userbyid(d.datdba) AS databaseowner, d.datcollate, d.datctype, shobj_description(d.oid, 'pg_database') AS description, d.datconnlimit, t.spcname, d.encoding, pg_encoding_to_char(d.encoding) AS encodingname FROM pg_database d LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid WHERE 1=1")
    val ast = SQLParserDelegate.parse("SELECT d.oid, d.datname AS databasename, d.datacl, d.datistemplate, d.datallowconn, pg_get_userbyid(d.datdba) AS databaseowner, d.datcollate, d.datctype, shobj_description(d.oid, 'pg_database') AS description, d.datconnlimit, t.spcname, d.encoding, pg_encoding_to_char(d.encoding) AS encodingname FROM pg_database d LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid WHERE 1=1") as DMLSelectStatement
    val visitorXX = SelectStatementVisitor()

    val elapsed = measureNanoTime  {
        ast.accept(visitorXX)
        TraditionalTreePrinter().print(visitorXX.rootNode)
        println(visitorXX.relation)
    }
    println("Time elapsed $elapsed nano seconds")

//    println(subset)
    val server = NettyServer()

    server.start()

}
