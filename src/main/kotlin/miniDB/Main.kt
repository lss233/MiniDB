package miniDB

import com.lss233.minidb.engine.NTuple
import java.util.function.BiPredicate

fun main(args: Array<String>) {
    println("Hello World!")

    var tuple: NTuple? =null

    val testValid = BiPredicate {  t:String,u: String -> u.isNotEmpty() && t.isNotEmpty() }

    println(projection_test(testValid))

    // Try adding program arguments via Run/Debug configuration.
    // Learn more about running applications: https://www.jetbrains.com/help/idea/running-applications.html.
    println("Program arguments: ${args.joinToString()}")
}

fun projection_test(predicate: BiPredicate<String, String>):List<String> {
    println("Test BiPredicate")
    val tuples = ArrayList<String>()
    tuples.add("1")
    tuples.add("2")
    tuples.add("321")
    tuples.add("666")
    return tuples.filter { i -> predicate.test(i,"123") }.toList()
}
