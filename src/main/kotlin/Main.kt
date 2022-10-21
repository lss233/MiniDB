import com.lss233.minidb.engine.NTupleAbandon
import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.RelationMath
import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.networking.NettyServer
import miniDB.parser.ast.expression.Expression
import miniDB.parser.ast.expression.comparison.ComparisionEqualsExpression
import miniDB.parser.ast.expression.logical.LogicalAndExpression
import miniDB.parser.ast.expression.logical.LogicalOrExpression
import miniDB.parser.ast.fragment.tableref.OuterJoin
import miniDB.parser.ast.stmt.dml.DMLSelectStatement
import miniDB.parser.recognizer.SQLParserDelegate
import miniDB.parser.visitor.Visitor
import kotlin.system.measureNanoTime

fun where(expression: Expression) : Boolean {
    return when(expression) {
        is LogicalOrExpression -> {
            for(i in 0 until expression.arity) {
                if (where(expression.getOperand(i))) {
                    return true
                }
            }
            return false
        }
        is LogicalAndExpression -> {
            for(i in 0 until expression.arity) {
                if (!where(expression.getOperand(i))) {
                    return false
                }
            }
            return true
        }
        is ComparisionEqualsExpression -> {
            println("Now checking ${expression.leftOprand} == ${expression.rightOprand}")

            return true
        }

        else -> {
            println("Unsatisfying ${expression.toString() }")
            return true
        }
    }
}
fun main(args: Array<String>) {
    println("MiniDB!")
    val d1 = Pair(Column("ID"), setOf("1", "2", "3"))
    val d2 = Pair(Column("Name"), setOf("1", "b", "3", "d", "2", "f"))
    val d3 = Pair(Column("Last Name"), setOf("A", "B", "C", "D", "E", "F"))
//    val d4 = listOf("Z", "X", "L")
    val elapsed = measureNanoTime  {
        val relation = RelationMath.cartesianProduct(d1, d2);
        val subset = relation select { row: NTupleAbandon, _: Relation ->
            row[0] == "1"
        }

        println(relation)
        println(subset)
    }
    println("Time elapsed $elapsed nano seconds")


    val ast = SQLParserDelegate.parse("SELECT d.oid, d.datname AS databasename, d.datacl, d.datistemplate, d.datallowconn, pg_get_userbyid(d.datdba) AS databaseowner, d.datcollate, d.datctype, shobj_description(d.oid, 'pg_database') AS description, d.datconnlimit, t.spcname, d.encoding, pg_encoding_to_char(d.encoding) AS encodingname FROM pg_database d LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid WHERE 1=1") as DMLSelectStatement

    val k = ast.tables.tableReferenceList[0] as OuterJoin
    val visitor = object: Visitor() {

    }
    ast.accept(visitor)
    println(ast.tables.tableReferenceList[0])
    where(ast.where)

//    println(subset)
    val server = NettyServer()

//    server.start()

}
