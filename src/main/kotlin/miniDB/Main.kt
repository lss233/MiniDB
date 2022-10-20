package miniDB

import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.utils.OrderPair
import javax.management.Attribute

fun main(args: Array<String>) {
    test()
    // Try adding program arguments via Run/Debug configuration.
    // Learn more about running applications: https://www.jetbrains.com/help/idea/running-applications.html.
    println("Program arguments: ${args.joinToString()}")
}

fun test() {

    val items = ArrayList<OrderPair<*, *>>()

    for (i in 1..12) {
        val orderPair = OrderPair(i, i)
        items.add(orderPair)
    }
    val attribute = Attribute("name",items)

    val nTuple = NTuple()
    nTuple.add(attribute)

    println(nTuple[0])

}