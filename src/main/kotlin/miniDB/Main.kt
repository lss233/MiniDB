package miniDB

import java.util.function.Predicate

fun main(args: Array<String>) {
    println("Hello World!")

    val isUserNameValid =
        Predicate { u: String? -> u != null && u.length > 2 && u.length < 10 }

    println(projection(isUserNameValid))
    // Try adding program arguments via Run/Debug configuration.
    // Learn more about running applications: https://www.jetbrains.com/help/idea/running-applications.html.
    println("Program arguments: ${args.joinToString()}")
}

fun projection(predicate: Predicate<String?>):List<String> {
    println("Test Predicate")
    val tuples = ArrayList<String>()
    tuples.add("1")
    tuples.add("2")
    tuples.add("321")
    tuples.add("666")
    return tuples.filter { i -> predicate.test(i) }.toList()
}