import com.lss233.minidb.networking.NettyServerInitializer
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.Channel
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import java.net.InetSocketAddress
class NettyServer {

    private var channel: Channel?=null
    private lateinit var bossGroup: EventLoopGroup
    private lateinit var workerGroup: EventLoopGroup

    var port = 5432
        set(value)  {
            field = value
        }

    fun start() {
        object : Thread() {
            override fun run() {
                super.run()
                bossGroup = NioEventLoopGroup(1)
                workerGroup = NioEventLoopGroup()
                try {
                    val b = ServerBootstrap()
                    b.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel::class.java)
                        .localAddress(InetSocketAddress(port))
                        .childOption(ChannelOption.SO_KEEPALIVE, true)
                        .childOption(ChannelOption.SO_REUSEADDR, true)
                        .childOption(ChannelOption.TCP_NODELAY, true)
                        .childHandler(NettyServerInitializer())

                    // Bind and start to accept incoming connections.
                    val f = b.bind().sync()

                    f.channel().closeFuture().sync()
                } catch (e: Exception) {
                    e.printStackTrace()
                } finally {
                    disconnect()
                }
            }
        }.start()

    }

    fun disconnect() {
        workerGroup.shutdownGracefully()
        bossGroup.shutdownGracefully()
    }

}
fun main(args: Array<String>) {
    println("MiniDB!")

    val server = NettyServer()

    server.start()


}