import com.lss233.minidb.networking.NettyServer

fun main(args: Array<String>) {
    println("MiniDB!")

    val server = NettyServer()

    server.start()


}