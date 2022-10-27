package miniDB

import com.lss233.minidb.engine.Attribute
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.utils.OrderPair


fun main(args: Array<String>) {
    test()
    // Try adding program arguments via Run/Debug configuration.
    // Learn more about running applications: https://www.jetbrains.com/help/idea/running-applications.html.
    println("Program arguments: ${args.joinToString()}")
}

fun test() {
    val attribute = Attribute<OrderPair<*>>()

    for (i in 1..12) {
        val orderPair = OrderPair(i,i.toString())
        attribute.add(orderPair)
    }

    for (i in attribute) {
        println(i.getOrder())
        println(i.getValue())
    }

    val nTuple = NTuple()


    nTuple.setAttributes(listOf(attribute))

    /**
     * Test
     */
    println(nTuple.getAttributes()?.get(0)?.get(0)?.getValue())

}