import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.RelationMath
import com.lss233.minidb.networking.NettyServer
import java.util.function.Predicate
import java.util.stream.Collectors
import java.util.stream.Stream


fun main(args: Array<String>) {
    println("MiniDB!")
    val d1 = listOf("1", "2", "3")
    val d2 = listOf("1", "b", "3", "d", "2", "f")
    val d3 = listOf("A", "B", "C", "D", "E", "F")
    val d4 = listOf("Z", "X", "L")
    val retval = RelationMath.cartesianProduct(d1, d2, d3, d4);
    println(retval)
    val a = NTuple();
    print(RelationMath.union(d1.toSet(), d2.toSet()))
    val relation = Relation()
    val subset = relation select Predicate {
        run {
            it[0] == "A"
        } }
    println(subset)
//    val server = NettyServer()

//    server.start()

}
