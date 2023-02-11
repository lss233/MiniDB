package com.lss233.minidb.networking

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import java.net.InetSocketAddress

class NettyServer {

    private lateinit var bossGroup: EventLoopGroup
    private lateinit var workerGroup: EventLoopGroup

    // Avoid conflicts with the local mysql service port address
    var port = 3308

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
                        .childHandler(MySQLProtocolInitializer())

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